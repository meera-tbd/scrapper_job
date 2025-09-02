#!/usr/bin/env python
"""
Simple Michael Page Australia Job Scraper using HTML parsing

This script provides a simpler alternative approach that parses the HTML content
to extract job information without relying on complex browser automation.

Features:
- Direct HTML parsing approach
- Less likely to be blocked by anti-bot measures
- Faster execution
- Uses the same database structure as other scrapers

Usage:
    python michaelpage_simple_scraper.py [max_jobs]

Examples:
    python michaelpage_simple_scraper.py 20
"""

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, quote
import logging
from decimal import Decimal
import requests
from bs4 import BeautifulSoup
import concurrent.futures

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Back to INFO level since job types are working correctly
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('michaelpage_simple_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SimpleMichaelPageScraper:
    """
    Simple HTTP-based Michael Page Australia scraper.
    """
    
    def __init__(self, job_limit=None):
        """Initialize the simple scraper."""
        self.base_url = "https://www.michaelpage.com.au"
        self.job_limit = job_limit
        
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Set up session with realistic headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='michaelpage_simple_scraper',
                defaults={
                    'email': 'system@michaelpagesimplescraper.com',
                    'first_name': 'Michael Page Simple',
                    'last_name': 'Scraper',
                    'is_staff': True,
                    'is_active': True
                }
            )
            if created:
                logger.info("Created system user for job posting")
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {str(e)}")
            return None
    
    def human_delay(self, min_seconds=0.1, max_seconds=0.3):
        """Add minimal delay between requests (optimized for speed)."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def parse_date(self, date_string):
        """Parse relative date strings into datetime objects."""
        if not date_string:
            return None
            
        date_string = date_string.lower().strip()
        now = timezone.now()
        
        # Handle "today" and "yesterday"
        if 'today' in date_string:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        elif 'yesterday' in date_string:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Extract number and unit from strings like "2 days ago"
        match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_string)
        if match:
            number = int(match.group(1))
            unit = match.group(2)
            
            if unit == 'hour':
                delta = timedelta(hours=number)
            elif unit == 'day':
                delta = timedelta(days=number)
            elif unit == 'week':
                delta = timedelta(weeks=number)
            elif unit == 'month':
                delta = timedelta(days=number * 30)  # Approximate
            else:
                return None
                
            return (now - delta).replace(minute=0, second=0, microsecond=0)
        
        return None
    
    def fetch_full_description(self, job_url):
        """Fetch and return a clean, full job description from the job detail page.

        This keeps the rest of the scraper intact and only enriches the
        `summary` field with the full description content from the detail page.
        """
        try:
            if not job_url:
                return ""

            response = self.session.get(job_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Prefer well-known Michael Page blocks if present
            preferred_selectors = [
                'div.job-description',
                'div.job_advert__description',
                'div.job-advert__description',
                'div.job-advert',
                'article.job',
                'main',
                'div[role="main"]'
            ]

            def collect_text(container):
                if not container:
                    return ""
                parts = []
                # Capture structured sections first (common MP headings)
                headings = container.find_all(['h2', 'h3'])
                if headings:
                    # Allowed and ignored headings on Michael Page
                    allowed_headings = [
                        'about our client', 'job description', 'the successful applicant',
                        "what's on offer", 'benefits', 'requirements', 'responsibilities',
                        'key responsibilities', 'skills and experience', 'your profile', 'the role'
                    ]
                    ignored_headings = [
                        'job summary', 'save job', 'apply', 'diversity & inclusion',
                        'other users applied'
                    ]
                    # Normalizer used for deduplication
                    def norm_text(t):
                        t = re.sub(r'\s+', ' ', (t or '')).strip().lower()
                        t = re.sub(r'[^a-z0-9\-&\s]', '', t)
                        return t
                    section_map = {}
                    section_title_for_key = {}
                    section_order = []
                    global_seen = set()
                    for heading in headings:
                        heading_text = heading.get_text(strip=True)
                        if not heading_text:
                            continue
                        heading_norm = heading_text.lower()
                        if any(h in heading_norm for h in ignored_headings):
                            continue
                        if not any(h in heading_norm for h in allowed_headings):
                            # Skip unknown/side headings to avoid noise blocks
                            continue
                        # Use the allowed heading as a stable key to merge duplicates
                        key = None
                        for ah in allowed_headings:
                            if ah in heading_norm:
                                key = ah
                                break
                        if key is None:
                            key = heading_norm
                        if key not in section_map:
                            section_map[key] = []
                            section_title_for_key[key] = heading_text
                            section_order.append(key)
                        section_lines = []
                        for sib in heading.find_all_next():
                            # Stop at the next heading at the same or higher level
                            if sib.name in ['h2', 'h3']:
                                break
                            if sib.name in ['p', 'div']:
                                text = sib.get_text(" ", strip=True)
                                if text:
                                    section_lines.append(text)
                            elif sib.name in ['ul', 'ol']:
                                for li in sib.find_all('li'):
                                    li_text = li.get_text(" ", strip=True)
                                    if li_text:
                                        section_lines.append(f"- {li_text}")
                        # Deduplicate within section and globally; drop contact details
                        unique_lines = []
                        seen_local = set()
                        for ln in section_lines:
                            n = norm_text(ln)
                            if not n:
                                continue
                            if any(k in n for k in [
                                'consultant name', 'consultant phone', 'job reference',
                                'phone number', 'contact '
                            ]):
                                continue
                            if n in seen_local or n in global_seen:
                                continue
                            seen_local.add(n)
                            global_seen.add(n)
                            unique_lines.append(ln)
                        if unique_lines:
                            section_map[key].extend(unique_lines)
                    # If we built sections, format them once per heading in order
                    if any(section_map.values()):
                        for key in section_order:
                            body = section_map.get(key) or []
                            if not body:
                                continue
                            parts.append(section_title_for_key.get(key, key.title()))
                            parts.append('\n'.join(body))
                # Fallback: longest paragraph/list text from container
                if not parts:
                    texts = []
                    for tag in container.find_all(['p', 'li']):
                        t = tag.get_text(" ", strip=True)
                        if t:
                            texts.append(t)
                    if texts:
                        parts.append('\n'.join(texts))
                text_joined = '\n\n'.join([p for p in parts if p])
                # Global cleanups to remove unwanted boilerplate
                remove_phrases = [
                    'job summary', 'save job', 'apply',
                    'diversity & inclusion at michael page',
                    'other users applied', 'contact ', 'quote job ref', 'phone number'
                ]
                lines = []
                for line in text_joined.splitlines():
                    ln = line.strip()
                    low = ln.lower()
                    if not ln:
                        continue
                    if any(ph in low for ph in remove_phrases):
                        continue
                    # Skip consultant/contact metadata
                    if any(k in low for k in [
                        'consultant name', 'consultant phone', 'job reference',
                        'function', 'specialisation', "what is your industry?", 'location', 'job type'
                    ]):
                        continue
                    # Skip bare bullets that are just Save/Apply duplicates
                    if ln in ['- Save Job', '- Apply']:
                        continue
                    lines.append(ln)
                # Deduplicate globally while preserving order
                cleaned = []
                seen = set()
                for ln in lines:
                    n = re.sub(r'\s+', ' ', ln.strip().lower())
                    if n in seen:
                        continue
                    seen.add(n)
                    cleaned.append(ln)
                return '\n'.join(cleaned)

            # Try preferred selectors first
            for sel in preferred_selectors:
                container = soup.select_one(sel)
                text = collect_text(container)
                if text and len(text) > 200:  # ensure it's substantive
                    return text

            # Generic fallback: use the largest text block inside main/article
            candidates = soup.select('main, article, div[role="main"], div.content, div.region-content')
            best_text = ""
            for c in candidates:
                text = collect_text(c)
                if len(text) > len(best_text):
                    best_text = text
            # Cut off at known tail boilerplates if still present
            tail_cuts = [
                'diversity & inclusion at michael page',
                'other users applied'
            ]
            bt_low = best_text.lower()
            cut_index = None
            for cut in tail_cuts:
                idx = bt_low.find(cut)
                if idx != -1:
                    cut_index = idx if cut_index is None else min(cut_index, idx)
            if cut_index is not None:
                best_text = best_text[:cut_index]
            return best_text.strip()
        except Exception as e:
            logger.debug(f"Failed to fetch full description: {e}")
            return ""

    def parse_location(self, location_string):
        """Parse location string into normalized location data."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Australian state abbreviations and full names
        states = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria', 
            'QLD': 'Queensland',
            'WA': 'Western Australia',
            'SA': 'South Australia',
            'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory',
            'NT': 'Northern Territory'
        }
        
        # Split by comma or other delimiters
        parts = [part.strip() for part in re.split(r'[,\-]', location_string)]
        
        city = ""
        state = ""
        country = "Australia"
        
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1]
            # Check if state part contains a known state abbreviation
            for abbrev, full_name in states.items():
                if abbrev in state_part.upper():
                    state = full_name
                    break
            else:
                # Look for full state names
                for abbrev, full_name in states.items():
                    if full_name.lower() in state_part.lower():
                        state = full_name
                        break
                else:
                    state = state_part
        elif len(parts) == 1:
            # Try to extract state from the single part
            location_parts = location_string.split()
            if len(location_parts) >= 2:
                potential_state = location_parts[-1].upper()
                if potential_state in states:
                    state = states[potential_state]
                    city = ' '.join(location_parts[:-1])
                else:
                    city = location_string
            else:
                city = location_string
        
        # Create location name
        location_name = location_string
        if city and state:
            location_name = f"{city}, {state}"
        elif city:
            location_name = city
        
        return location_name, city, state, country
    
    def parse_salary(self, salary_text):
        """Parse salary information into structured data."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for salary extraction
        patterns = [
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*-\s*AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80-100k"
            r'(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80k"
        ]
        
        salary_min = None
        salary_max = None
        currency = "AUD"
        salary_type = "yearly"
        
        for pattern in patterns:
            match = re.search(pattern, salary_text.lower().replace(',', ''))
            if match:
                groups = match.groups()
                if len(groups) == 3:  # Range with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    salary_max = Decimal(groups[1].replace(',', ''))
                    salary_type = groups[2]
                    break
                elif len(groups) == 2 and 'k' in salary_text.lower():  # Range in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_max = Decimal(groups[1].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
                elif len(groups) == 2:  # Single amount with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    salary_type = groups[1]
                    break
                elif len(groups) == 1 and 'k' in salary_text.lower():  # Single amount in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
        
        return salary_min, salary_max, currency, salary_type, salary_text
    
    def extract_jobs_from_html(self, html_content):
        """Extract job data from HTML content using BeautifulSoup."""
        jobs = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Based on the HTML structure you provided, find job tiles specifically
            job_tiles = soup.find_all('div', class_='job-tile')
            
            logger.info(f"Found {len(job_tiles)} job tiles in the HTML")
            
            for tile in job_tiles:
                job_data = self.extract_job_from_tile(tile)
                if job_data:
                    jobs.append(job_data)
            
            # Fallback method if job tiles not found
            if not jobs:
                logger.warning("No job tiles found, trying alternative methods...")
                
                # Look for list items with views-row class (based on your HTML)
                job_rows = soup.find_all('li', class_='views-row')
                for row in job_rows:
                    job_data = self.extract_job_from_row(row)
                    if job_data:
                        jobs.append(job_data)
            
            # Remove duplicates based on job_url
            seen_urls = set()
            unique_jobs = []
            for job in jobs:
                if job['job_url'] not in seen_urls:
                    seen_urls.add(job['job_url'])
                    unique_jobs.append(job)
            
            logger.info(f"Extracted {len(unique_jobs)} unique jobs from HTML")
            return unique_jobs if self.job_limit is None else unique_jobs[:self.job_limit]
            
        except Exception as e:
            logger.error(f"Error extracting jobs from HTML: {str(e)}")
            return []
    
    def extract_job_from_tile(self, tile):
        """Extract job data from a job-tile div element based on the provided HTML structure."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Extract job title and URL from h3 > a
            title_element = tile.find('h3')
            if title_element:
                title_link = title_element.find('a')
                if title_link:
                    job_data['job_title'] = title_link.get_text(strip=True)
                    href = title_link.get('href')
                    if href:
                        job_data['job_url'] = urljoin(self.base_url, href)
            
            # Extract location from job-location div
            location_element = tile.find('div', class_='job-location')
            if location_element:
                job_data['location_text'] = location_element.get_text(strip=True).replace('', '').strip()
            
            # Extract salary from job-salary div
            salary_element = tile.find('div', class_='job-salary')
            if salary_element:
                job_data['salary_text'] = salary_element.get_text(strip=True).replace('', '').strip()
            
            # Extract job type from job-contract-type div
            contract_element = tile.find('div', class_='job-contract-type')
            if contract_element:
                # Remove icon and get clean text
                contract_text = contract_element.get_text(strip=True).replace('', '').strip()
                # Clean up any extra whitespace and normalize
                if contract_text:
                    # Remove common icon characters and clean up
                    contract_clean = contract_text.replace('🕒', '').replace('⏰', '').strip()
                    if contract_clean:
                        job_data['keywords'].append(contract_clean)
            
            # Extract work mode from job-nature div
            nature_element = tile.find('div', class_='job-nature')
            if nature_element:
                nature_text = nature_element.get_text(strip=True).replace('', '').strip()
                job_data['keywords'].append(nature_text)
            
            # Extract summary from job-summary div
            summary_element = tile.find('div', class_='job-summary')
            if summary_element:
                summary_text_elem = summary_element.find('div', class_='job_advert__job-summary-text')
                if summary_text_elem:
                    job_data['summary'] = summary_text_elem.get_text(strip=True)
            
            # Extract bullet points
            bullet_element = tile.find('div', class_='bullet_points')
            if bullet_element:
                bullet_list = bullet_element.find('ul')
                if bullet_list:
                    bullets = [li.get_text(strip=True) for li in bullet_list.find_all('li')]
                    job_data['keywords'].extend(bullets)
            
            # Only return if we have at least a title and URL
            if job_data['job_title'] and job_data['job_url']:
                logger.info(f"[EXTRACTED] {job_data['job_title']}")
                logger.info(f"   Location: {job_data['location_text']}")
                logger.info(f"   Salary: {job_data['salary_text']}")
                logger.info(f"   Keywords: {job_data['keywords']}")
                return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from tile: {str(e)}")
        
        return None

    def extract_job_from_row(self, row):
        """Extract job data from a views-row li element based on the provided HTML structure."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Skip job alert rows
            if row.find('div', class_='job-alert-wrap'):
                return None
            
            # Find the job-tile div within the row
            job_tile = row.find('div', class_='job-tile')
            if job_tile:
                return self.extract_job_from_tile(job_tile)
            
        except Exception as e:
            logger.error(f"Error extracting job from row: {str(e)}")
        
        return None

    def extract_job_from_container(self, container):
        """Extract job data from a job container element."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Find job title and URL
            title_link = container.find('a', href=True)
            if title_link:
                job_data['job_title'] = title_link.get_text(strip=True)
                job_data['job_url'] = urljoin(self.base_url, title_link['href'])
            
            # Find location if available
            location_element = container.find(class_=re.compile(r'location', re.I))
            if location_element:
                job_data['location_text'] = location_element.get_text(strip=True)
            
            # Find description/summary
            desc_element = container.find(class_=re.compile(r'description|summary', re.I))
            if desc_element:
                job_data['summary'] = desc_element.get_text(strip=True)
            
            # Find salary information
            salary_element = container.find(class_=re.compile(r'salary|pay|wage', re.I))
            if salary_element:
                job_data['salary_text'] = salary_element.get_text(strip=True)
            
            # Only return if we have at least a title and URL
            if job_data['job_title'] and job_data['job_url']:
                return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from container: {str(e)}")
        
        return None
    
    def save_job_to_database_sync(self, job_data):
        """Synchronous database save function."""
        try:
            connections.close_all()
            
            with transaction.atomic():
                # Check for duplicates
                job_url = job_data['job_url']
                job_title = job_data['job_title']
                company_name = job_data['company_name']
                
                if job_url and JobPosting.objects.filter(external_url=job_url).exists():
                    logger.info(f"[DUPLICATE SKIPPED] (URL): {job_title}")
                    self.duplicate_count += 1
                    return False
                
                if JobPosting.objects.filter(title=job_title, company__name=company_name).exists():
                    logger.info(f"[DUPLICATE SKIPPED] (Title+Company): {job_title}")
                    self.duplicate_count += 1
                    return False
                
                # Create location
                location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
                location_obj = None
                if location_name:
                    location_obj, created = Location.objects.get_or_create(
                        name=location_name,
                        defaults={'city': city, 'state': state, 'country': country}
                    )
                
                # Create company
                company_slug = slugify(company_name)
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Jobs from Michael Page Australia',
                        'website': 'https://www.michaelpage.com.au',
                        'company_size': 'large'
                    }
                )
                
                # Parse salary
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Parse date
                date_posted = self.parse_date(job_data.get('posted_ago', ''))
                
                # Determine job details from keywords
                job_type = "full_time"  # Default
                work_mode = ""
                experience_level = ""
                
                keywords = job_data.get('keywords', [])
                logger.info(f"[PROCESSING KEYWORDS] for '{job_data.get('job_title', '')}': {keywords}")
                
                for keyword in keywords:
                    keyword_lower = keyword.lower().strip()
                    
                    # Map website job types to database job types
                    if keyword_lower == 'permanent':
                        job_type = "permanent"  # Keep as permanent instead of converting to full_time
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif keyword_lower == 'temporary':
                        job_type = "temporary"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif keyword_lower == 'contract':
                        job_type = "contract"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'part-time' in keyword_lower or 'part time' in keyword_lower:
                        job_type = "part_time"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'casual' in keyword_lower:
                        job_type = "casual"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'internship' in keyword_lower:
                        job_type = "internship"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'freelance' in keyword_lower:
                        job_type = "freelance"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    # Work modes
                    elif 'hybrid' in keyword_lower or 'work from home' in keyword_lower or 'remote' in keyword_lower:
                        work_mode = keyword
                        logger.info(f"   [WORK_MODE] Set work_mode: {work_mode}")
                    # Experience levels
                    elif any(level in keyword_lower for level in ['senior', 'junior', 'graduate', 'executive', 'lead', 'manager']):
                        experience_level = keyword
                        logger.info(f"   [EXPERIENCE] Set experience_level: {experience_level}")
                
                # Automatic job categorization
                job_category = JobCategorizationService.categorize_job(
                    title=job_data.get('job_title', ''),
                    description=job_data.get('summary', '')
                )
                
                # Create unique slug
                base_slug = slugify(job_data.get('job_title', 'job'))
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Create the JobPosting
                job_posting = JobPosting.objects.create(
                    title=job_data.get('job_title', ''),
                    slug=unique_slug,
                    description=job_data.get('summary', 'No description available'),
                    company=company_obj,
                    posted_by=self.system_user,
                    location=location_obj,
                    job_category=job_category,
                    job_type=job_type,
                    experience_level=experience_level,
                    work_mode=work_mode,
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=currency,
                    salary_type=salary_type,
                    salary_raw_text=raw_text,
                    external_source='michaelpage.com.au',
                    external_url=job_data.get('job_url', ''),
                    status='active',
                    posted_ago=job_data.get('posted_ago', ''),
                    date_posted=date_posted,
                    additional_info=job_data
                )
                
                logger.info(f"[SAVED TO DATABASE]")
                logger.info(f"   Title: {job_posting.title}")
                logger.info(f"   Company: {job_posting.company.name}")
                logger.info(f"   Location: {job_posting.location.name if job_posting.location else 'Not specified'}")
                logger.info(f"   Job Type: {job_posting.job_type}")
                logger.info(f"   Work Mode: {job_posting.work_mode}")
                logger.info(f"   Salary: {job_posting.salary_display}")
                logger.info(f"   Category: {job_posting.job_category}")
                logger.info(f"   URL: {job_posting.external_url}")
                self.scraped_count += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job to database: {str(e)}")
            self.error_count += 1
            return False
    
    def extract_pagination_url(self, html_content):
        """Extract the next page URL from the 'Show more Jobs' pagination.
        
        Based on the HTML structure:
        <ul class="js-pager__items pager__items pager-show-more">
            <li class="pager__item">
                <a href="/jobs?page=1" title="Show more" rel="next">Show more Jobs</a>
            </li>
        </ul>
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Primary method: Look for the exact pagination structure from Michael Page
            pager_container = soup.find('ul', class_='js-pager__items pager__items pager-show-more')
            if pager_container:
                pager_item = pager_container.find('li', class_='pager__item')
                if pager_item:
                    show_more_link = pager_item.find('a', href=True)
                    if show_more_link:
                        # Check if it's the correct "Show more Jobs" link
                        link_text = show_more_link.get_text(strip=True)
                        if 'Show more' in link_text and 'Jobs' in link_text:
                            next_url = show_more_link['href']
                            # Convert relative URL to absolute URL
                            if next_url.startswith('/'):
                                next_url = urljoin(self.base_url, next_url)
                            logger.info(f"Found pagination URL (primary method): {next_url}")
                            return next_url
            
            # Secondary method: Look for any "Show more Jobs" link
            show_more_links = soup.find_all('a', href=True)
            for link in show_more_links:
                link_text = link.get_text(strip=True)
                if 'Show more' in link_text and 'Jobs' in link_text:
                    next_url = link['href']
                    if next_url.startswith('/'):
                        next_url = urljoin(self.base_url, next_url)
                    logger.info(f"Found pagination URL (secondary method): {next_url}")
                    return next_url
            
            # Fallback pagination patterns
            pagination_selectors = [
                'a[rel="next"]',
                'a[title*="Show more"]',
                '.pager-show-more a',
                '.js-pager__items a',
                'a[href*="page="]'
            ]
            
            for selector in pagination_selectors:
                next_link = soup.select_one(selector)
                if next_link and next_link.get('href'):
                    next_url = next_link['href']
                    if next_url.startswith('/'):
                        next_url = urljoin(self.base_url, next_url)
                    logger.info(f"Found pagination URL (fallback): {next_url}")
                    return next_url
            
            logger.info("No pagination URL found")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting pagination URL: {str(e)}")
            return None
    
    def debug_pagination_structure(self, html_content):
        """Debug method to understand the pagination structure."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for any pagination-related elements
            pagination_elements = []
            
            # Check for common pagination classes and IDs
            common_selectors = [
                'ul[class*="pager"]',
                'div[class*="pager"]',
                'nav[class*="pagination"]',
                'div[class*="pagination"]',
                'ul[class*="pagination"]',
                '[class*="show-more"]',
                '[class*="load-more"]',
                'a[rel="next"]',
                'a[href*="page="]'
            ]
            
            for selector in common_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    pagination_elements.append({
                        'selector': selector,
                        'element': str(elem)[:200] + '...' if len(str(elem)) > 200 else str(elem),
                        'text': elem.get_text(strip=True)[:100]
                    })
            
            if pagination_elements:
                logger.debug(f"Found {len(pagination_elements)} pagination-related elements:")
                for i, elem in enumerate(pagination_elements[:5]):  # Limit to first 5
                    logger.debug(f"  {i+1}. Selector: {elem['selector']}")
                    logger.debug(f"     Text: {elem['text']}")
                    logger.debug(f"     HTML: {elem['element']}")
            else:
                logger.debug("No pagination elements found in HTML")
                
        except Exception as e:
            logger.debug(f"Error in debug_pagination_structure: {str(e)}")
    
    def run(self):
        """Main method to run the scraping process with pagination support."""
        logger.info("Starting Simple Michael Page Australia job scraper...")
        logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        logger.info("Note: Now supports pagination with 'Show more Jobs' functionality")
        
        try:
            current_url = "https://www.michaelpage.com.au/jobs"
            page_number = 0
            total_jobs_processed = 0
            
            while current_url and (self.job_limit is None or self.scraped_count < self.job_limit):
                page_number += 1
                logger.info(f"Fetching page {page_number}: {current_url}")
                
                # Add delay between page requests
                if page_number > 1:
                    self.human_delay(1, 2)  # Longer delay between pages
                
                response = self.session.get(current_url, timeout=30)
                response.raise_for_status()
                
                logger.info(f"Successfully fetched page {page_number} (status: {response.status_code})")
                
                # Extract jobs from the HTML
                jobs = self.extract_jobs_from_html(response.text)
                
                if not jobs:
                    logger.warning(f"No jobs found on page {page_number}")
                    break
                
                logger.info(f"Found {len(jobs)} jobs on page {page_number}")
                total_jobs_processed += len(jobs)
                
                # Process jobs from current page
                jobs_saved_this_page = 0
                for i, job_data in enumerate(jobs):
                    if self.job_limit is not None and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}")
                        break
                    
                    logger.info(f"Processing job {i+1}/{len(jobs)} from page {page_number}: {job_data['job_title']}")
                    
                    # Quick duplicate check before processing (saves time)
                    job_url = job_data.get('job_url', '')
                    job_title = job_data.get('job_title', '')
                    if job_url:
                        try:
                            if JobPosting.objects.filter(external_url=job_url).exists():
                                logger.info(f"DUPLICATE SKIPPED (Quick Check): {job_title}")
                                self.duplicate_count += 1
                                continue
                        except Exception as e:
                            logger.debug(f"Quick duplicate check failed: {e}")
                            pass  # Continue with normal processing if quick check fails

                    # Enrich summary with full description from the detail page
                    try:
                        full_desc = self.fetch_full_description(job_url)
                        if full_desc:
                            job_data['summary'] = full_desc
                    except Exception as e:
                        logger.debug(f"Could not enrich description: {e}")
                    
                    if self.save_job_to_database_sync(job_data):
                        jobs_saved_this_page += 1
                    
                    # Add minimal delay between saves
                    self.human_delay(0.1, 0.3)
                
                logger.info(f"Page {page_number} completed: {jobs_saved_this_page} jobs saved")
                
                # Check if we've reached the limit
                if self.job_limit is not None and self.scraped_count >= self.job_limit:
                    logger.info(f"Reached job limit of {self.job_limit}, stopping pagination")
                    break
                
                # Extract next page URL for pagination
                # Debug: Log pagination structure for troubleshooting
                if page_number <= 2:  # Only debug first couple pages
                    self.debug_pagination_structure(response.text)
                next_url = self.extract_pagination_url(response.text)
                if next_url and next_url != current_url:
                    current_url = next_url
                    logger.info(f"Moving to next page: {current_url}")
                    
                    # Safety check to prevent infinite loops
                    if page_number > 50:  # Reasonable limit
                        logger.warning(f"Safety limit reached ({page_number} pages), stopping pagination")
                        break
                else:
                    logger.info("No more pages found or reached the end of pagination")
                    break
            
            # Final statistics
            logger.info("="*50)
            logger.info("MICHAEL PAGE SCRAPING COMPLETED!")
            logger.info(f"Pages scraped: {page_number}")
            logger.info(f"Total jobs found: {total_jobs_processed}")
            logger.info(f"Jobs saved to database: {self.scraped_count}")
            logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
            logger.info(f"Errors encountered: {self.error_count}")
            
            try:
                total_jobs_in_db = JobPosting.objects.count()
                logger.info(f"Total job postings in database: {total_jobs_in_db}")
            except:
                logger.info("Total job postings in database: (count unavailable)")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise


def main():
    """Main function to run the simple scraper."""
    print("🔍 Simple Michael Page Australia Job Scraper")
    print("="*50)
    
    # Parse command line arguments
    max_jobs = None  # Default (unlimited)
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            print("Invalid number of jobs. Using unlimited.")
    
    print(f"Target: {max_jobs} jobs from Michael Page Australia")
    print("Method: Direct HTML parsing with 'Show more Jobs' pagination support")
    print("Database: Professional structure with JobPosting, Company, Location")
    print("="*50)
    
    # Create scraper instance
    scraper = SimpleMichaelPageScraper(job_limit=max_jobs)
    
    try:
        # Run the scraping process
        scraper.run()
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


def run(job_limit=None):
    """Automation entrypoint for Michael Page simple scraper.

    Creates the scraper and runs it without CLI args; returns a summary dict.
    """
    try:
        scraper = SimpleMichaelPageScraper(job_limit=job_limit)
        scraper.run()
        return {
            'success': True,
            'jobs_scraped': scraper.scraped_count,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count,
            'message': f'Successfully scraped {scraper.scraped_count} Michael Page jobs'
        }
    except Exception as e:
        logger.error(f"Scraping failed in run(): {e}")
        return {
            'success': False,
            'error': str(e),
            'message': f'Scraping failed: {e}'
        }

if __name__ == "__main__":
    main()
