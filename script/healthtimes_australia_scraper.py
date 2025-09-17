#!/usr/bin/env python3
"""
Professional HealthTimes Australia Healthcare Jobs Scraper
==========================================================

Advanced Playwright-based scraper for HealthTimes Australia (https://healthtimes.com.au/job-search/) 
that integrates with the existing australia_job_scraper database structure.

HealthTimes specializes in healthcare jobs across Australia including:
- Nursing positions (RN, EN, midwives)
- Allied health roles (physiotherapy, occupational therapy, etc.)
- Medical positions (doctors, specialists, registrars)
- Healthcare administration and support roles
- Mental health and disability services

Features:
- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian healthcare job optimization
- Pagination support for complete data extraction

Usage:
    python healthtimes_australia_scraper.py [job_limit]
    
Examples:
    python healthtimes_australia_scraper.py 50    # Scrape 50 healthcare jobs
    python healthtimes_australia_scraper.py       # Scrape all available jobs
"""

import os
import sys
import django
import time
import random
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"  # Allow Django ORM in async context
# Add the project root to the Python path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
except NameError:
    # Handle case when __file__ is not defined (e.g., in interactive mode)
    project_root = os.getcwd()
sys.path.append(project_root)

django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

# Import your existing models and services
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()


@dataclass
class ScrapedJob:
    """Data class for scraped job information."""
    title: str
    company_name: str
    location_text: str
    job_type: str
    salary_text: str
    description: str
    posted_ago: str
    job_url: str
    requirements: str = ""
    benefits: str = ""
    experience_level: str = ""


class HealthTimesAustraliaJobScraper:
    """Professional HealthTimes Australia healthcare job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://healthtimes.com.au"
        self.search_url = "https://healthtimes.com.au/job-search/"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        
        # Pagination support
        self.current_page = 1
        self.total_pages = None
        self.max_pages = 50  # Safety limit to prevent infinite loops
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # HealthTimes specific categories mapping
        self.healthcare_specialties = {
            'nursing': ['registered nurse', 'enrolled nurse', 'nurse practitioner', 'midwife', 'clinical nurse', 'nurse manager'],
            'allied_health': ['physiotherapist', 'occupational therapist', 'speech pathologist', 'dietitian', 'psychologist', 'social worker'],
            'medical': ['doctor', 'registrar', 'consultant', 'gp', 'general practitioner', 'specialist', 'physician'],
            'administration': ['administration', 'receptionist', 'coordinator', 'manager', 'administrator'],
            'support': ['support worker', 'carer', 'assistant', 'aide', 'technician']
        }

    def setup_logging(self):
        """Configure comprehensive logging."""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('healthtimes_australia_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_or_create_bot_user(self):
        """Get or create the bot user for job postings."""
        try:
            user, created = User.objects.get_or_create(
                username='healthtimes_bot',
                defaults={
                    'email': 'healthtimes_bot@healthtimes.com.au',
                    'first_name': 'HealthTimes',
                    'last_name': 'Bot'
                }
            )
            if created:
                self.logger.info("Created new HealthTimes bot user")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return None

    # -----------------------------
    # Helpers: HTML + Skills
    # -----------------------------
    def sanitize_description_html(self, html: str) -> str:
        """Return clean, safe and compact HTML from a raw description block.

        - Strips scripts/styles/forms/nav/header/footer and unrelated sidebars
        - Preserves p, ul/ol/li, headings, strong/em, br
        - Removes most attributes except href on anchors
        """
        try:
            if not html:
                return ""
            soup = BeautifulSoup(html, "html.parser")

            # Remove unwanted nodes entirely
            for sel in [
                "script", "style", "form", "nav", "header", "footer",
                ".sidebar", ".related", ".share", ".apply", ".application",
            ]:
                for n in soup.select(sel):
                    n.decompose()

            # Allow-list of tags to keep
            allowed = {"p", "ul", "ol", "li", "strong", "em", "b", "i",
                       "br", "h1", "h2", "h3", "h4", "h5", "h6", "a"}

            for tag in list(soup.find_all(True)):
                if tag.name not in allowed:
                    tag.unwrap()
                    continue
                # Strip attributes except href for anchors
                attrs = dict(tag.attrs)
                for attr in attrs:
                    if tag.name == "a" and attr == "href":
                        continue
                    del tag.attrs[attr]

            # Collapse excessive whitespace
            texty = soup.get_text("\n")
            if texty and not soup.find(True):
                # If ended up as plain text, wrap to <p>
                lines = [ln.strip() for ln in texty.splitlines() if ln.strip()]
                return "\n".join(f"<p>{ln}</p>" for ln in lines)

            html_clean = str(soup)
            # Remove surrounding html/body if present
            html_clean = re.sub(r"^\s*<(?:html|body)[^>]*>|</(?:html|body)>\s*$", "", html_clean, flags=re.I)
            # Remove duplicate blank lines
            html_clean = re.sub(r"\n{3,}", "\n\n", html_clean)
            return html_clean.strip()
        except Exception as e:
            try:
                self.logger.warning(f"HTML sanitize failed: {e}")
            except Exception:
                pass
            return html

    def extract_skills_from_text(self, text: str, max_items: int = 18) -> tuple[str, str]:
        """Extract healthcare-oriented skills from plain text and split across
        `skills` and `preferred_skills` CSV fields (<=200 chars each).
        """
        if not text:
            return "", ""
        normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text.lower())
        keywords = [
            # Core healthcare & compliance
            "ahpra", "bls", "cpr", "first aid", "manual handling", "infection control",
            "medication administration", "wound care", "care planning", "clinical assessment",
            # Nursing
            "registered nurse", "enrolled nurse", "midwife", "icu", "ed", "theatre", "aged care",
            # Allied health
            "physiotherapy", "occupational therapy", "speech pathology", "psychology", "social work",
            # Systems & tools
            "emr", "epic", "cerner", "best practice", "ms office", "excel",
            # Soft skills
            "communication", "teamwork", "time management", "problem solving", "leadership",
            # Misc healthcare
            "mental health", "disability support", "risk assessment", "triage",
        ]
        found = []
        for kw in keywords:
            pattern = r"\b" + re.escape(kw.replace('.', '\\.')) + r"\b"
            if re.search(pattern, normalized):
                found.append(kw)
        # Deduplicate preserving order
        seen = set()
        dedup = []
        for kw in found:
            if kw not in seen:
                seen.add(kw)
                dedup.append(kw)
        if not dedup:
            return "", ""
        dedup = dedup[:max_items]
        # Pack into two fields within 200 chars each
        skills_list: List[str] = []
        preferred_list: List[str] = []
        limit = 200
        for item in dedup:
            trial = ", ".join(skills_list + [item]).strip(', ')
            if len(trial) <= limit:
                skills_list.append(item)
            else:
                trial2 = ", ".join(preferred_list + [item]).strip(', ')
                if len(trial2) <= limit:
                    preferred_list.append(item)
        return ", ".join(skills_list), ", ".join(preferred_list)

    def parse_closing_date_text(self, text: str) -> str:
        """Normalize a closing date string like 12-10-2025 or 12/10/2025.
        Returns the original if unrecognized; the DB field is free text.
        """
        if not text:
            return ""
        t = text.strip()
        # Try multiple formats
        for fmt in (r"(\d{2})[\-/](\d{2})[\-/](\d{4})", r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})"):
            m = re.search(fmt, t)
            if m:
                return m.group(0)
        return t

    def create_browser_context(self):
        """Create a browser context with Australian user agent."""
        if not self.browser:
            self.browser = sync_playwright().start().chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
        
        self.context = self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        
        self.page = self.context.new_page()
        
        # Set extra headers
        self.page.set_extra_http_headers({
            'Accept-Language': 'en-AU,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def human_like_delay(self, min_delay=1.0, max_delay=3.0):
        """Add human-like delays between actions."""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def get_total_pages(self):
        """Extract total pages from pagination HTML."""
        try:
            # Look for the "Last" button which contains the total page number
            last_button = self.page.query_selector('.pagination a[onclick*="jobsearch_pagination"]:has-text("Last")')
            if last_button:
                onclick_value = last_button.get_attribute('onclick')
                # Extract page number from onclick="jobsearch_pagination('42')"
                import re
                match = re.search(r"jobsearch_pagination\('(\d+)'\)", onclick_value)
                if match:
                    total_pages = int(match.group(1))
                    self.logger.info(f"Detected total pages: {total_pages}")
                    return total_pages
            
            # Alternative: look for numbered pagination links
            page_links = self.page.query_selector_all('.pagination a[onclick*="jobsearch_pagination"]')
            max_page = 0
            for link in page_links:
                onclick_value = link.get_attribute('onclick')
                if onclick_value:
                    match = re.search(r"jobsearch_pagination\('(\d+)'\)", onclick_value)
                    if match:
                        page_num = int(match.group(1))
                        max_page = max(max_page, page_num)
            
            if max_page > 0:
                self.logger.info(f"Detected total pages from numbered links: {max_page}")
                return max_page
            
            # Fallback: assume multiple pages exist
            self.logger.warning("Could not detect total pages, assuming 5 pages exist")
            return 5
            
        except Exception as e:
            self.logger.warning(f"Error detecting total pages: {e}")
            return 5  # Default fallback

    def extract_job_details(self, job_element) -> Optional[ScrapedJob]:
        """Extract job details from a job listing element."""
        try:
            # Try multiple selectors for job title and URL
            title_selectors = [
                'div.featured_news_text a',
                '.featured_news_text a',
                'a',
                'h3 a',
                'h2 a',
                '.job-title a',
                '[href*="job-details"]'
            ]
            
            title_link = None
            for selector in title_selectors:
                title_link = job_element.query_selector(selector)
                if title_link:
                    break
            
            if not title_link:
                # Try to extract text content without link
                text_selectors = [
                    'div.featured_news_text',
                    '.featured_news_text',
                    'h3',
                    'h2',
                    '.job-title'
                ]
                for selector in text_selectors:
                    text_element = job_element.query_selector(selector)
                    if text_element:
                        text_content = text_element.inner_text().strip()
                        if text_content and len(text_content) > 10:  # Basic validation
                            # Create a basic job entry without URL
                            return ScrapedJob(
                                title=text_content[:100],  # Limit title length
                                company_name="HealthTimes",
                                location_text="Australia",
                                job_type="Healthcare",
                                salary_text="",
                                description=text_content,
                                posted_ago="Recently",
                                job_url="",
                                requirements="",
                                benefits="",
                                experience_level=""
                            )
                
                self.logger.warning("Could not find job title in any expected location")
                return None
            
            title = title_link.inner_text().strip()
            job_url = title_link.get_attribute('href')
            if job_url and not job_url.startswith('http'):
                job_url = urljoin(self.base_url, job_url)
            
            # Extract company and date info from span
            info_span = job_element.query_selector('div.featured_news_text span')
            company_name = "HealthTimes"
            posted_ago = ""
            
            if info_span:
                info_text = info_span.inner_text().strip()
                # Split by " - " to get company and date
                parts = info_text.split(' - ')
                if len(parts) >= 2:
                    company_name = parts[0].strip()
                    posted_ago = parts[1].strip()
                elif len(parts) == 1:
                    # Could be just company or just date
                    if any(char.isdigit() for char in parts[0]):
                        posted_ago = parts[0].strip()
                    else:
                        company_name = parts[0].strip()
            
            # Extract description/salary info
            description_p = job_element.query_selector('div.featured_news_text p')
            description = ""
            salary_text = ""
            
            if description_p:
                desc_text = description_p.inner_text().strip()
                description = desc_text
                
                # Try to extract salary information
                if '$' in desc_text:
                    salary_match = re.search(r'\$[\d,]+(?:\s*per\s*\w+)?', desc_text)
                    if salary_match:
                        salary_text = salary_match.group(0)
            
            # Determine job type and location from title/description
            job_type = self.determine_job_type(title, description)
            location_text = self.extract_location(title, description)
            
            return ScrapedJob(
                title=title,
                company_name=company_name,
                location_text=location_text,
                job_type=job_type,
                salary_text=salary_text,
                description=description,
                posted_ago=posted_ago,
                job_url=job_url,
                requirements="",
                benefits="",
                experience_level=""
            )
            
        except Exception as e:
            self.logger.error(f"Error extracting job details: {e}")
            return None

    def determine_job_type(self, title: str, description: str) -> str:
        """Determine job type based on title and description."""
        title_lower = title.lower()
        desc_lower = description.lower()
        combined_text = f"{title_lower} {desc_lower}"
        
        # Check for specific healthcare specialties
        for category, keywords in self.healthcare_specialties.items():
            if any(keyword in combined_text for keyword in keywords):
                return category.replace('_', ' ').title()
        
        # Default job type determination
        if any(word in combined_text for word in ['part time', 'casual', 'contract']):
            return 'Part Time'
        elif any(word in combined_text for word in ['full time', 'permanent']):
            return 'Full Time'
        else:
            return 'Healthcare'

    def extract_location(self, title: str, description: str) -> str:
        """Extract location from job title or description."""
        combined_text = f"{title} {description}"
        
        # Australian states and territories
        locations = [
            'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT',
            'New South Wales', 'Victoria', 'Queensland', 'South Australia',
            'Western Australia', 'Tasmania', 'Northern Territory',
            'Australian Capital Territory', 'Sydney', 'Melbourne',
            'Brisbane', 'Perth', 'Adelaide', 'Hobart', 'Darwin', 'Canberra'
        ]
        
        for location in locations:
            if location.lower() in combined_text.lower():
                return location
        
        return 'Australia'

    def get_job_detailed_info(self, job_url: str) -> Dict[str, str]:
        """Get additional job details from the job detail page."""
        try:
            # Clean the URL to avoid pagination parameters on detail pages
            clean_url = job_url.split('?')[0] if '?' in job_url else job_url
            self.logger.info(f"Getting detailed info from: {clean_url}")
            self.page.goto(clean_url, wait_until='networkidle', timeout=30000)
            self.human_like_delay(1, 2)
            
            details = {
                'requirements': '',
                'benefits': '',
                'experience_level': '',
                'full_description': ''
            }
            
            # Try to extract full description from HealthTimes job detail page
            content_selectors = [
                '.job_advertisement_txt .detaildiv',  # HealthTimes specific
                '.job_advertisement_left',  # HealthTimes specific
                '.job-content',
                '.job-description', 
                '.content',
                '.main-content',
                'main',
                '.post-content'
            ]
            
            for selector in content_selectors:
                content_element = self.page.query_selector(selector)
                if content_element:
                    full_text = content_element.inner_text().strip()
                    details['full_description'] = full_text
                    
                    # Extract specific details from HealthTimes format
                    if 'Contact Name:' in full_text:
                        # Parse requirements/experience from Grade field
                        grade_match = re.search(r'Grade:\s*([^\n]+)', full_text)
                        if grade_match:
                            details['experience_level'] = grade_match.group(1).strip()
                        
                        # Parse benefits (Travel, Accommodation)
                        benefits = []
                        if 'Travel: Provided' in full_text:
                            benefits.append('Travel Provided')
                        if 'Accommodation: Provided' in full_text:
                            benefits.append('Accommodation Provided')
                        if benefits:
                            details['benefits'] = ', '.join(benefits)
                        
                        # Parse requirements from registration requirement
                        if 'General Registration with AHPRA' in full_text:
                            details['requirements'] = 'General Registration with AHPRA and current work rights in Australia required'
                    
                    break
            
            return details
            
        except Exception as e:
            self.logger.error(f"Error getting detailed job info from {job_url}: {e}")
            return {'requirements': '', 'benefits': '', 'experience_level': '', 'full_description': ''}

    def get_job_detailed_info_new_page(self, page, job_url: str) -> Dict[str, str]:
        """Get additional job details from the job detail page using a new page context."""
        try:
            # Clean the URL to avoid pagination parameters on detail pages
            clean_url = job_url.split('?')[0] if '?' in job_url else job_url
            self.logger.info(f"Getting detailed info from: {clean_url}")
            page.goto(clean_url, wait_until='load', timeout=20000)
            self.human_like_delay(2, 3)
            
            details = {
                'requirements': '',
                'benefits': '',
                'experience_level': '',
                'full_description': '',
                'description_html': '',
                'location': '',
                'job_type': '',
                'salary_text': '',
                'closing_date': ''
            }
            
            # Try to extract full description from HealthTimes job detail page
            content_selectors = [
                '.job_advertisement_txt .detaildiv',  # HealthTimes specific
                '.job_advertisement_left',  # HealthTimes specific
                '.job-content',
                '.job-description', 
                '.content',
                '.main-content',
                'main',
                '.post-content'
            ]
            
            for selector in content_selectors:
                content_element = page.query_selector(selector)
                if content_element:
                    full_text = content_element.inner_text().strip()
                    details['full_description'] = full_text
                    try:
                        raw_html = content_element.inner_html()
                        details['description_html'] = self.sanitize_description_html(raw_html)
                    except Exception:
                        details['description_html'] = ''
                    
                    # Extract specific details from HealthTimes format based on your provided HTML
                    if 'Contact Name:' in full_text:
                        # Parse requirements/experience from Grade field
                        grade_match = re.search(r'Grade:\s*([^\n]+)', full_text)
                        if grade_match:
                            details['experience_level'] = grade_match.group(1).strip()
                        
                        # Parse speciality
                        speciality_match = re.search(r'Speciality:\s*([^\n]+)', full_text)
                        if speciality_match:
                            if not details['experience_level']:
                                details['experience_level'] = speciality_match.group(1).strip()
                            else:
                                details['experience_level'] += f", {speciality_match.group(1).strip()}"
                        
                        # Parse benefits (Travel, Accommodation)
                        benefits = []
                        if 'Travel: Provided' in full_text or 'Travel:Provided' in full_text:
                            benefits.append('Travel Provided')
                        if 'Accommodation: Provided' in full_text or 'Accommodation:Provided' in full_text:
                            benefits.append('Accommodation Provided')
                        if benefits:
                            details['benefits'] = ', '.join(benefits)
                        
                        # Parse requirements from registration requirement
                        if 'General Registration with AHPRA' in full_text:
                            details['requirements'] = 'General Registration with AHPRA and current work rights in Australia required'
                    
                    break
            
            # Extract structured information from the job details table
            try:
                table_rows = page.query_selector_all('.apply_register_table tr')
                for row in table_rows:
                    cells = row.query_selector_all('td')
                    if len(cells) >= 2:
                        field_name = cells[0].inner_text().strip().lower()
                        field_value = cells[1].inner_text().strip()
                        
                        if 'location' in field_name:
                            details['location'] = field_value
                            self.logger.info(f"Found location from table: {field_value}")
                        
                        elif 'job type' in field_name:
                            # Map job type values to our standard format
                            job_type_mapping = {
                                'temporary/part-time': 'Part Time',
                                'temporary/full-time': 'Full Time',
                                'permanent/part-time': 'Part Time',
                                'permanent/full-time': 'Full Time',
                                'casual': 'Casual',
                                'contract': 'Contract'
                            }
                            mapped_job_type = job_type_mapping.get(field_value.lower(), field_value)
                            details['job_type'] = mapped_job_type
                            self.logger.info(f"Found job type from table: {mapped_job_type}")
                        
                        elif 'salary' in field_name or 'package' in field_name:
                            details['salary_text'] = field_value
                            self.logger.info(f"Found salary from table: {field_value}")
                        elif 'closing date' in field_name or 'closing' in field_name:
                            details['closing_date'] = self.parse_closing_date_text(field_value)
                            self.logger.info(f"Found closing date from table: {details['closing_date']}")
                        
                        elif 'classification' in field_name and not details['experience_level']:
                            details['experience_level'] = field_value
                            self.logger.info(f"Found classification from table: {field_value}")
                        
                        elif 'sub classification' in field_name:
                            if details['experience_level']:
                                details['experience_level'] += f", {field_value}"
                            else:
                                details['experience_level'] = field_value
                            self.logger.info(f"Found sub classification from table: {field_value}")
            
            except Exception as e:
                self.logger.warning(f"Could not extract table data: {e}")
            
            # If closing date not found in table, try scanning right-hand meta box if present
            if not details['closing_date']:
                try:
                    right_meta = page.query_selector('.job_advertisement_right')
                    if right_meta:
                        meta_text = right_meta.inner_text().strip()
                        m = re.search(r"closing date[:\s]*([\w\-/\s]+)", meta_text, flags=re.I)
                        if m:
                            details['closing_date'] = self.parse_closing_date_text(m.group(1))
                except Exception:
                    pass

            return details
            
        except Exception as e:
            self.logger.error(f"Error getting detailed job info from {job_url}: {e}")
            return {'requirements': '', 'benefits': '', 'experience_level': '', 'full_description': '', 'location': '', 'job_type': '', 'salary_text': ''}

    def scrape_jobs_from_page(self) -> List[ScrapedJob]:
        """Scrape jobs from the current page."""
        jobs = []
        try:
            # Try multiple selectors to find job listings
            selectors_to_try = [
                'li div.featured_news',
                'div.featured_news',
                '.job-listing',
                '.job-item',
                '[class*="job"]',
                'li'
            ]
            
            job_elements = []
            for selector in selectors_to_try:
                try:
                    self.page.wait_for_selector(selector, timeout=10000)
                    elements = self.page.query_selector_all(selector)
                    if elements:
                        job_elements = elements
                        self.logger.info(f"Found {len(job_elements)} job listings using selector: {selector}")
                        
                        # Debug: Print the HTML structure of the first few job elements
                        for i, element in enumerate(job_elements[:3]):
                            try:
                                html_content = element.inner_html()
                                self.logger.info(f"Job element {i+1} HTML structure:\n{html_content[:500]}...")
                            except Exception as e:
                                self.logger.warning(f"Could not get HTML for job element {i+1}: {e}")
                        break
                except:
                    continue
            
            if not job_elements:
                self.logger.warning("No job elements found with any selector")
                return jobs
            
            self.human_like_delay(1, 2)
            
            for i, job_element in enumerate(job_elements):
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    break
                    
                try:
                    # Create fresh element reference to avoid DOM context errors
                    fresh_elements = self.page.query_selector_all('li div.featured_news')
                    if i < len(fresh_elements):
                        job_element = fresh_elements[i]
                    
                    scraped_job = self.extract_job_details(job_element)
                    if scraped_job:
                        # Get additional details from job page with improved error handling
                        if scraped_job.job_url:
                            try:
                                # Create a new page context for job detail to avoid DOM errors
                                detail_page = self.context.new_page()
                                detailed_info = self.get_job_detailed_info_new_page(detail_page, scraped_job.job_url)
                                
                                # Update description if found
                                if detailed_info['full_description']:
                                    scraped_job.description = detailed_info['full_description']
                                
                                # Update missing fields from detail page
                                # Always use location from detail page if available (more specific)
                                if detailed_info.get('location'):
                                    scraped_job.location_text = detailed_info['location']
                                    self.logger.info(f"Updated location: '{detailed_info['location']}'")
                                
                                # Always use job type from detail page if available (more specific)
                                if detailed_info.get('job_type'):
                                    scraped_job.job_type = detailed_info['job_type']
                                    self.logger.info(f"Updated job type: '{detailed_info['job_type']}'")
                                
                                # Update salary if not available from listing page
                                if detailed_info.get('salary_text') and not scraped_job.salary_text:
                                    scraped_job.salary_text = detailed_info['salary_text']
                                    self.logger.info(f"Updated salary: '{detailed_info['salary_text']}'")
                                
                                # Always update these fields
                                scraped_job.requirements = detailed_info['requirements']
                                scraped_job.benefits = detailed_info['benefits']
                                scraped_job.experience_level = detailed_info['experience_level']

                                # Derive skills from the detailed plain text
                                skills_csv, preferred_csv = self.extract_skills_from_text(scraped_job.description)
                                scraped_job.skills_csv = skills_csv
                                scraped_job.preferred_csv = preferred_csv

                                # Capture closing date if available
                                scraped_job.closing_date = detailed_info.get('closing_date', '')
                                
                                detail_page.close()
                                self.logger.info(f"Successfully extracted detailed info for {scraped_job.title}")
                            except Exception as e:
                                self.logger.warning(f"Could not get detailed info for {scraped_job.job_url}: {e}")
                        else:
                            self.logger.info(f"No URL available for detailed extraction")
                        
                        jobs.append(scraped_job)
                        self.jobs_scraped += 1
                        self.logger.info(f"Scraped job {self.jobs_scraped}: {scraped_job.title}")
                        
                        # Human-like delay between jobs
                        self.human_like_delay(0.5, 1.5)
                        
                except Exception as e:
                    self.logger.error(f"Error processing job element: {e}")
                    self.errors_count += 1
                    continue
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from page: {e}")
            self.errors_count += 1
        
        return jobs

    def navigate_to_next_page(self) -> bool:
        """Navigate to the next page of results."""
        try:
            # Look for pagination controls
            next_selectors = [
                'a[aria-label="Next"]',
                '.pagination a:last-child',
                '.next-page',
                'a:has-text("Next")',
                'a:has-text(">")'
            ]
            
            for selector in next_selectors:
                next_button = self.page.query_selector(selector)
                if next_button and next_button.is_enabled():
                    next_button.click()
                    self.human_like_delay(2, 4)
                    return True
            
            # Try URL-based pagination
            current_url = self.page.url
            if '?page=' in current_url:
                page_match = re.search(r'page=(\d+)', current_url)
                if page_match:
                    current_page = int(page_match.group(1))
                    next_page_url = current_url.replace(f'page={current_page}', f'page={current_page + 1}')
                    self.page.goto(next_page_url, wait_until='networkidle')
                    self.human_like_delay(2, 4)
                    return True
            else:
                # First page, try adding page=2
                next_page_url = f"{current_url}{'&' if '?' in current_url else '?'}page=2"
                self.page.goto(next_page_url, wait_until='networkidle')
                self.human_like_delay(2, 4)
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False

    def run_scraper(self):
        """Main scraper execution."""
        self.logger.info("Starting HealthTimes Australia healthcare job scraping...")
        
        try:
            self.create_browser_context()
            
            # Navigate to the first page to detect total pages
            first_page_url = f"{self.search_url}?page=1"
            self.logger.info(f"Navigating to: {first_page_url}")
            
            # Try multiple strategies to load the page
            for attempt in range(3):
                try:
                    if attempt == 0:
                        # First attempt: standard navigation
                        self.page.goto(first_page_url, wait_until='networkidle', timeout=45000)
                    elif attempt == 1:
                        # Second attempt: wait for load instead of networkidle
                        self.page.goto(first_page_url, wait_until='load', timeout=30000)
                    else:
                        # Third attempt: just basic navigation
                        self.page.goto(first_page_url, timeout=20000)
                    
                    self.human_like_delay(3, 5)
                    self.logger.info(f"Successfully loaded page on attempt {attempt + 1}")
                    break
                    
                except Exception as e:
                    self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == 2:
                        raise e
                    self.human_like_delay(2, 4)
            
            # Detect total pages from pagination
            if not self.total_pages:
                self.total_pages = self.get_total_pages()
                self.total_pages = min(self.total_pages, self.max_pages)  # Apply safety limit
            
            all_jobs = []
            self.current_page = 1
            
            self.logger.info(f"Will scrape up to {self.total_pages} pages")
            
            while self.current_page <= self.total_pages:
                self.logger.info(f"Scraping page {self.current_page} of {self.total_pages}...")
                
                # Navigate to current page if not on page 1
                if self.current_page > 1:
                    page_url = f"{self.search_url}?page={self.current_page}"
                    self.logger.info(f"Navigating to page {self.current_page}: {page_url}")
                    
                    try:
                        self.page.goto(page_url, wait_until='load', timeout=30000)
                        self.human_like_delay(2, 4)
                    except Exception as e:
                        self.logger.error(f"Failed to navigate to page {self.current_page}: {e}")
                        break
                
                # Scrape jobs from current page
                page_jobs = self.scrape_jobs_from_page()
                
                if not page_jobs:
                    self.logger.info(f"No jobs found on page {self.current_page}, stopping pagination")
                    break
                
                all_jobs.extend(page_jobs)
                self.logger.info(f"Found {len(page_jobs)} jobs on page {self.current_page}")
                
                # Check if we've reached the job limit
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    self.logger.info(f"Reached job limit of {self.job_limit}")
                    break
                
                # Move to next page
                self.current_page += 1
                
                # Human-like delay between pages
                self.human_like_delay(3, 6)
            
            # Save all jobs to database
            if all_jobs:
                self.save_jobs_to_database(all_jobs)
            
        except Exception as e:
            self.logger.error(f"Fatal error in scraper: {e}")
            self.errors_count += 1
        finally:
            self.cleanup()
        
        # Print final statistics
        self.print_scraping_summary()

    def save_jobs_to_database(self, jobs: List[ScrapedJob]):
        """Save scraped jobs to the database."""
        self.logger.info(f"Saving {len(jobs)} jobs to database...")
        
        for job in jobs:
            try:
                with transaction.atomic():
                    # Get or create company
                    company, created = Company.objects.get_or_create(
                        name=job.company_name,
                        defaults={
                            'description': f'Healthcare company from HealthTimes',
                            'website': 'https://healthtimes.com.au'
                        }
                    )
                    
                    # Get or create location with proper mapping
                    location_city = job.location_text
                    location_state = 'Australia'
                    
                    # Map location abbreviations to full names and states
                    location_mapping = {
                        'NSW': {'city': 'New South Wales', 'state': 'NSW'},
                        'VIC': {'city': 'Victoria', 'state': 'VIC'},
                        'QLD': {'city': 'Queensland', 'state': 'QLD'},
                        'SA': {'city': 'South Australia', 'state': 'SA'},
                        'WA': {'city': 'Western Australia', 'state': 'WA'},
                        'TAS': {'city': 'Tasmania', 'state': 'TAS'},
                        'NT': {'city': 'Northern Territory', 'state': 'NT'},
                        'ACT': {'city': 'Australian Capital Territory', 'state': 'ACT'},
                        'Sydney': {'city': 'Sydney', 'state': 'NSW'},
                        'Melbourne': {'city': 'Melbourne', 'state': 'VIC'},
                        'Brisbane': {'city': 'Brisbane', 'state': 'QLD'},
                        'Perth': {'city': 'Perth', 'state': 'WA'},
                        'Adelaide': {'city': 'Adelaide', 'state': 'SA'},
                        'Hobart': {'city': 'Hobart', 'state': 'TAS'},
                        'Darwin': {'city': 'Darwin', 'state': 'NT'},
                        'Canberra': {'city': 'Canberra', 'state': 'ACT'}
                    }
                    
                    if job.location_text in location_mapping:
                        location_city = location_mapping[job.location_text]['city']
                        location_state = location_mapping[job.location_text]['state']
                        self.logger.info(f"Location mapping: '{job.location_text}' -> '{location_city}, {location_state}'")
                    
                    # Create location name for display
                    location_name = f"{location_city}, {location_state}" if location_state != 'Australia' else location_city
                    
                    location, created = Location.objects.get_or_create(
                        name=location_name,
                        defaults={
                            'city': location_city,
                            'state': location_state,
                            'country': 'Australia'
                        }
                    )
                    
                    if created:
                        self.logger.info(f"Created new location: {location_name} (city={location_city}, state={location_state})")
                    else:
                        self.logger.info(f"Using existing location: {location_name}")
                    
                    self.logger.info(f"Final job location will be: {job.location_text} -> Location ID: {location.id} ({location.name})")
                    
                    # Check for duplicate jobs
                    existing_job = JobPosting.objects.filter(
                        title=job.title,
                        company=company,
                        location=location
                    ).first()
                    
                    if existing_job:
                        self.duplicates_found += 1
                        self.logger.info(f"Duplicate job found: {job.title} at {job.company_name}")
                        continue
                    
                    # Parse salary with correct type detection
                    salary_min, salary_max, salary_type = self.parse_salary(job.salary_text)
                    
                    # Parse job type to match model choices with enhanced mapping
                    job_type_mapping = {
                        'Full Time': 'full_time',
                        'Part Time': 'part_time', 
                        'Casual': 'casual',
                        'Contract': 'contract',
                        'Temporary': 'temporary',
                        'Permanent': 'permanent',
                        'Healthcare': 'full_time',  # Default for healthcare
                        'Nursing': 'full_time',
                        'Allied Health': 'full_time',
                        'Medical': 'full_time',
                        'Administration': 'full_time',
                        'Support': 'part_time',
                        # Handle combinations from table extraction
                        'Temporary/Part-Time': 'part_time',
                        'Temporary/Full-Time': 'full_time',
                        'Permanent/Part-Time': 'part_time', 
                        'Permanent/Full-Time': 'full_time'
                    }
                    job_type_value = job_type_mapping.get(job.job_type, 'full_time')
                    
                    self.logger.info(f"Job type mapping: '{job.job_type}' -> '{job_type_value}'")
                    
                    # Ensure both skills fields are populated
                    skills_csv = getattr(job, 'skills_csv', '') or ''
                    preferred_csv = getattr(job, 'preferred_csv', '') or ''
                    if not skills_csv and not preferred_csv:
                        gen_s, gen_p = self.extract_skills_from_text((job.description or '') + ' ' + (job.title or ''))
                        skills_csv, preferred_csv = gen_s, gen_p
                    if skills_csv and not preferred_csv:
                        preferred_csv = skills_csv
                    if preferred_csv and not skills_csv:
                        skills_csv = preferred_csv
                    # Enforce DB max length (200 each)
                    skills_csv = (skills_csv or '')[:200]
                    preferred_csv = (preferred_csv or '')[:200]

                    # Create job posting with correct field names
                    job_posting = JobPosting.objects.create(
                        title=job.title,
                        company=company,
                        location=location,
                        posted_by=self.bot_user,
                        job_type=job_type_value,
                        job_category='healthcare',
                        # Prefer cleaned HTML if we produced any, else use plain text wrapped
                        description=(
                            self.sanitize_description_html(job.description) if job.description else ""
                        )[:5000] or "<p>Healthcare position</p>",
                        salary_min=salary_min,
                        salary_max=salary_max,
                        salary_currency='AUD',
                        salary_type=salary_type,
                        salary_raw_text=job.salary_text[:200] if job.salary_text else "",
                        experience_level=job.experience_level[:100] if job.experience_level else "",
                        date_posted=self.parse_posted_date(job.posted_ago),
                        external_url=job.job_url,
                        external_source='HealthTimes',
                        posted_ago=job.posted_ago[:50] if job.posted_ago else "",
                        status='active',
                        job_closing_date=getattr(job, 'closing_date', ''),
                        skills=skills_csv,
                        preferred_skills=preferred_csv,
                        additional_info={
                            'requirements': job.requirements,
                            'benefits': job.benefits,
                            'scraped_from': 'healthtimes.com.au'
                        }
                    )
                    
                    # Auto-categorize the job
                    try:
                        JobCategorizationService.categorize_job(job_posting)
                    except Exception as e:
                        self.logger.warning(f"Could not categorize job {job_posting.id}: {e}")
                    
                    self.jobs_saved += 1
                    self.logger.info(f"Saved job: {job.title} at {job.company_name}")
                    
            except Exception as e:
                self.logger.error(f"Error saving job {job.title}: {e}")
                self.errors_count += 1
                continue

    def parse_salary(self, salary_text: str) -> tuple:
        """Parse salary information and return min/max values and salary type."""
        if not salary_text:
            return None, None, 'yearly'
        
        try:
            # Determine salary type based on text
            salary_type = 'yearly'  # default
            if 'per day' in salary_text.lower() or '/day' in salary_text.lower():
                salary_type = 'daily'
            elif 'per hour' in salary_text.lower() or '/hour' in salary_text.lower():
                salary_type = 'hourly'
            elif 'per week' in salary_text.lower() or '/week' in salary_text.lower():
                salary_type = 'weekly'
            elif 'per month' in salary_text.lower() or '/month' in salary_text.lower():
                salary_type = 'monthly'
            
            # Remove currency symbols and common words for number extraction
            cleaned = re.sub(r'[^\d\-\s,]', '', salary_text.lower())
            
            # Look for salary ranges
            range_match = re.search(r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)', cleaned)
            if range_match:
                min_sal = Decimal(range_match.group(1).replace(',', ''))
                max_sal = Decimal(range_match.group(2).replace(',', ''))
                return min_sal, max_sal, salary_type
            
            # Look for single salary value
            single_match = re.search(r'(\d{1,3}(?:,\d{3})*)', cleaned)
            if single_match:
                salary = Decimal(single_match.group(1).replace(',', ''))
                return salary, salary, salary_type
            
        except Exception as e:
            self.logger.warning(f"Error parsing salary '{salary_text}': {e}")
        
        return None, None, 'yearly'

    def parse_posted_date(self, posted_ago: str) -> datetime:
        """Parse the posted date from relative time string."""
        try:
            if not posted_ago:
                return datetime.now()
            
            # Handle specific date formats
            if re.match(r'\d{2}-\d{2}-\d{4}', posted_ago):
                return datetime.strptime(posted_ago, '%d-%m-%Y')
            
            # Handle relative dates
            posted_ago_lower = posted_ago.lower()
            now = datetime.now()
            
            if 'today' in posted_ago_lower or 'just posted' in posted_ago_lower:
                return now
            elif 'yesterday' in posted_ago_lower:
                return now - timedelta(days=1)
            elif 'day' in posted_ago_lower:
                days_match = re.search(r'(\d+)', posted_ago_lower)
                if days_match:
                    days = int(days_match.group(1))
                    return now - timedelta(days=days)
            elif 'week' in posted_ago_lower:
                weeks_match = re.search(r'(\d+)', posted_ago_lower)
                if weeks_match:
                    weeks = int(weeks_match.group(1))
                    return now - timedelta(weeks=weeks)
            elif 'month' in posted_ago_lower:
                months_match = re.search(r'(\d+)', posted_ago_lower)
                if months_match:
                    months = int(months_match.group(1))
                    return now - timedelta(days=months * 30)
            
        except Exception as e:
            self.logger.warning(f"Error parsing posted date '{posted_ago}': {e}")
        
        return datetime.now()

    def cleanup(self):
        """Clean up browser resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def print_scraping_summary(self):
        """Print a summary of the scraping session."""
        print("\n" + "="*60)
        print("🏥 HEALTHTIMES AUSTRALIA SCRAPING SUMMARY")
        print("="*60)
        print(f"📊 Jobs Scraped: {self.jobs_scraped}")
        print(f"💾 Jobs Saved: {self.jobs_saved}")
        print(f"🔄 Duplicates Found: {self.duplicates_found}")
        print(f"❌ Errors Encountered: {self.errors_count}")
        if self.total_pages:
            print(f"📄 Pages Scraped: {self.current_page - 1} of {self.total_pages}")
        print(f"✅ Success Rate: {((self.jobs_saved)/(self.jobs_scraped) if self.jobs_scraped > 0 else 0)*100:.1f}%")
        print("="*60)
        
        if self.jobs_saved > 0:
            print("✅ Scraping completed successfully!")
            print("🔍 Check the database for new healthcare job postings.")
        else:
            print("⚠️  No new jobs were saved. Check logs for details.")


def main():
    """Main function to run the scraper."""
    job_limit = None
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"🎯 Job limit set to: {job_limit}")
        except ValueError:
            print("❌ Invalid job limit. Please provide a number.")
            return
    
    # Create and run scraper
    scraper = HealthTimesAustraliaJobScraper(job_limit=job_limit)
    scraper.run_scraper()


def run(job_limit=100):
    """Automation entrypoint for HealthTimes Australia scraper."""
    try:
        scraper = HealthTimesAustraliaJobScraper(job_limit=job_limit)
        scraper.run_scraper()
        return {
            'success': True,
            'message': 'HealthTimes scraping completed'
        }
    except SystemExit as e:
        return {
            'success': int(getattr(e, 'code', 1)) == 0,
            'exit_code': getattr(e, 'code', 1)
        }
    except Exception as e:
        try:
            logging.getLogger(__name__).error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    main()
