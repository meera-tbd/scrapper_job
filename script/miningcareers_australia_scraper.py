#!/usr/bin/env python
"""
Professional MiningCareers.com.au Job Scraper using Playwright - FIXED VERSION

This script scrapes job listings from miningcareers.com.au using a robust approach
that handles Django async context and element navigation issues.

Features:
- Fixed Django async context handling
- Robust element extraction before navigation
- Professional database structure integration
- Mining-specific job categorization
- Human-like behavior to avoid detection
- Complete pagination handling

Usage:
    python miningcareers_australia_scraper_fixed.py [max_jobs]

Example:
    python miningcareers_australia_scraper_fixed.py 100
"""

import os
import sys
import re
import time
import random
import uuid
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import logging
from decimal import Decimal
import threading

# Set up Django environment BEFORE any Django imports
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('miningcareers_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MiningCareersJobScraper:
    """Professional scraper for miningcareers.com.au job listings."""
    
    def __init__(self, max_jobs=50, headless=True):
        """
        Initialize the scraper.
        
        Args:
            max_jobs (int): Maximum number of jobs to scrape
            headless (bool): Whether to run browser in headless mode
        """
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.miningcareers.com.au"
        self.jobs_url = f"{self.base_url}/jobs/"
        self.scraped_jobs = []
        self.processed_urls = set()
        
        # Mining-specific job categories
        self.mining_categories = {
            'operations': ['operator', 'production', 'process', 'plant', 'operations'],
            'maintenance': ['mechanic', 'fitter', 'electrician', 'maintenance', 'technician'],
            'mining': ['miner', 'driller', 'blaster', 'excavation', 'underground'],
            'engineering': ['engineer', 'supervisor', 'superintendent', 'manager'],
            'transport': ['driver', 'transport', 'haul', 'truck', 'logistics'],
            'safety': ['safety', 'hse', 'compliance', 'environmental'],
            'administration': ['admin', 'hr', 'finance', 'coordinator', 'assistant'],
            'geology': ['geologist', 'surveyor', 'exploration', 'geology'],
            'other': []
        }
        
        # Get or create the scraper user
        self.scraper_user = self._get_or_create_scraper_user()
        
        logger.info(f"MiningCareers scraper initialized. Max jobs: {max_jobs}")
    
    def _get_or_create_scraper_user(self):
        """Get or create the system user for scraped jobs."""
        user, created = User.objects.get_or_create(
            username='miningcareers_scraper',
            defaults={
                'email': 'scraper@miningcareers.system',
                'first_name': 'MiningCareers',
                'last_name': 'Scraper',
                'is_staff': False,
                'is_active': False,
            }
        )
        if created:
            logger.info("Created new scraper user: miningcareers_scraper")
        return user
    
    def _categorize_job(self, title, description):
        """
        Categorize job based on title and description.
        
        Args:
            title (str): Job title
            description (str): Job description
            
        Returns:
            str: Job category
        """
        text = f"{title} {description}".lower()
        
        for category, keywords in self.mining_categories.items():
            if any(keyword in text for keyword in keywords):
                return category
        
        return 'other'
    
    def _parse_relative_date(self, date_text):
        """
        Parse relative date strings like '2 days ago', '1 week ago'.
        
        Args:
            date_text (str): Relative date string
            
        Returns:
            datetime: Parsed datetime object
        """
        if not date_text:
            return timezone.now()
        
        date_text = date_text.lower().strip()
        now = timezone.now()
        
        # Handle various date formats
        if 'hour' in date_text:
            hours = re.search(r'(\d+)', date_text)
            if hours:
                return now - timedelta(hours=int(hours.group(1)))
        elif 'day' in date_text:
            days = re.search(r'(\d+)', date_text)
            if days:
                return now - timedelta(days=int(days.group(1)))
        elif 'week' in date_text:
            weeks = re.search(r'(\d+)', date_text)
            if weeks:
                return now - timedelta(weeks=int(weeks.group(1)))
        elif 'month' in date_text:
            months = re.search(r'(\d+)', date_text)
            if months:
                return now - timedelta(days=int(months.group(1)) * 30)
        
        return now
    
    def _extract_salary_info(self, salary_text):
        """
        Extract salary information from text - CONSERVATIVE APPROACH.
        
        Args:
            salary_text (str): Raw salary text
            
        Returns:
            dict: Salary information with min, max, type, currency
        """
        if not salary_text:
            return {}
        
        # ONLY process if text contains clear salary indicators
        if not any(indicator in salary_text.lower() for indicator in ['$', 'salary', 'package', 'remuneration', 'compensation']):
            return {}
        
        # AVOID processing pagination numbers or job counts
        if any(invalid in salary_text.lower() for invalid in [
            '1-20 of', 'page', 'jobs', 'of 478', 'posted', 'hour ago', 'day ago', 'see all'
        ]):
            return {}
        
        salary_info = {
            'raw_text': salary_text,
            'currency': 'AUD',
            'type': 'yearly'
        }
        
        # Only extract numbers if there's a clear salary context
        if '$' in salary_text or any(word in salary_text.lower() for word in ['salary', 'package', 'remuneration']):
            # Remove common prefixes and clean text
            cleaned = re.sub(r'[^\d\s\-\$\,\.ka-z]', '', salary_text.lower())
            
            # Extract numbers
            numbers = re.findall(r'[\d,]+', cleaned)
            if numbers:
                try:
                    # Convert to integers, handling commas
                    nums = [int(num.replace(',', '')) for num in numbers]
                    
                    # Only process reasonable salary numbers (not pagination numbers)
                    valid_nums = [num for num in nums if num >= 20000 or (num <= 500 and 'k' in cleaned)]
                    
                    if valid_nums:
                        # Determine if it's in thousands (k) or actual amount
                        if 'k' in cleaned:
                            valid_nums = [num * 1000 if num < 1000 else num for num in valid_nums]
                        
                        if len(valid_nums) == 1:
                            salary_info['min'] = salary_info['max'] = valid_nums[0]
                        elif len(valid_nums) >= 2:
                            salary_info['min'] = min(valid_nums)
                            salary_info['max'] = max(valid_nums)
                    
                except (ValueError, TypeError):
                    pass
            
            # Determine salary type
            if any(term in cleaned for term in ['hour', 'hr']):
                salary_info['type'] = 'hourly'
            elif any(term in cleaned for term in ['day', 'daily']):
                salary_info['type'] = 'daily'
            elif any(term in cleaned for term in ['week', 'weekly']):
                salary_info['type'] = 'weekly'
            elif any(term in cleaned for term in ['month', 'monthly']):
                salary_info['type'] = 'monthly'
        
        return salary_info
    
    def _get_or_create_company(self, company_name, logo_url=None):
        """
        Get or create a company record.
        
        Args:
            company_name (str): Company name
            logo_url (str): Optional logo URL
            
        Returns:
            Company: Company model instance
        """
        if not company_name or company_name.strip() == '':
            company_name = 'Unknown Company'
        
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                logo=logo_url or '',
                company_size='large'  # Assume large for mining companies
            )
            logger.info(f"Created new company: {company_name}")
        
        return company
    
    def _get_or_create_location(self, location_text):
        """
        Get or create a location record.
        
        Args:
            location_text (str): Location string like "Perth, WA" or "Pilbara, WA"
            
        Returns:
            Location: Location model instance
        """
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
        
        location_text = location_text.strip()
        
        # Try to find existing location
        location = Location.objects.filter(name__iexact=location_text).first()
        
        if not location:
            # Parse location components
            parts = [part.strip() for part in location_text.split(',')]
            
            if len(parts) >= 2:
                city = parts[0]
                state = parts[1]
            else:
                city = location_text
                state = ''
            
            location = Location.objects.create(
                name=location_text,
                city=city,
                state=state,
                country='Australia'
            )
            logger.info(f"Created new location: {location_text}")
        
        return location
    
    def _extract_job_data_safely(self, job_element):
        """
        Safely extract all job data from element before any navigation occurs.
        
        Args:
            job_element: Playwright element handle
            
        Returns:
            dict: Extracted job data
        """
        try:
            # Extract all data in one go to avoid navigation issues
            element_html = job_element.inner_html()
            element_text = job_element.inner_text() or ''
            
            # Initialize job data
            job_data = {
                'title': 'Unknown Position',
                'company_name': 'Unknown Company',
                'company_logo': '',
                'location': 'Australia',
                'posted_ago': '',
                'job_url': '',
                'description': element_text[:500] if element_text else 'No description available',
                'salary_text': ''
            }
            
            # Extract title from text patterns - IMPROVED LOGIC
            lines = [line.strip() for line in element_text.split('\n') if line.strip()]
            
            # Skip invalid titles (pagination, numbers, etc.)
            invalid_patterns = [
                r'^\d+-\d+\s+of\s+\d+',  # "1-20 of 478"
                r'^\d+$',                # Just numbers
                r'^page\s+\d+',          # "page 1"
                r'^next$',               # "Next"
                r'^previous$',           # "Previous"
                'ago', 'posted', 'logo', 'image', 'hour', 'day',
                'mining careers', 'subscribe', 'newsletter'
            ]
            
            # Find proper job title (avoid pagination and metadata)
            for line in lines[:5]:  # Check first 5 lines
                if line and len(line) > 8 and len(line) < 150:  # Reasonable title length
                    # Check if line contains invalid patterns
                    is_invalid = False
                    for pattern in invalid_patterns:
                        if isinstance(pattern, str):
                            if pattern.lower() in line.lower():
                                is_invalid = True
                                break
                        else:  # regex pattern
                            if re.match(pattern, line, re.IGNORECASE):
                                is_invalid = True
                                break
                    
                    if not is_invalid:
                        # Additional check: must contain job-like keywords
                        job_keywords = [
                            'manager', 'supervisor', 'superintendent', 'lead', 'leader', 'team',
                            'engineer', 'technician', 'operator', 'fitter', 'mechanic',
                            'coordinator', 'assistant', 'advisor', 'specialist', 'analyst',
                            'officer', 'representative', 'administrator', 'maintenance',
                            'safety', 'hr', 'human resources', 'finance', 'accountant',
                            'geologist', 'surveyor', 'driver', 'driller', 'blaster'
                        ]
                        
                        if any(keyword in line.lower() for keyword in job_keywords):
                            job_data['title'] = line
                            break
            
            # DYNAMIC COMPANY EXTRACTION - NO STATIC LISTS
            company_found = False
            
            # Method 1: PRIORITY - Look for company logos and specific company indicators
            # First, look for company-specific logos/images
            company_logo_patterns = [
                r'alt=["\']([^"\']*bhp[^"\']*)["\']',
                r'alt=["\']([^"\']*rio tinto[^"\']*)["\']',
                r'alt=["\']([^"\']*fortescue[^"\']*)["\']',
                r'alt=["\']([^"\']*newcrest[^"\']*)["\']',
                r'alt=["\']([^"\']*santos[^"\']*)["\']',
                r'alt=["\']([^"\']*woodside[^"\']*)["\']'
            ]
            
            major_companies = ['BHP', 'Rio Tinto', 'Fortescue', 'Newcrest', 'Santos', 'Woodside', 
                             'Anglo American', 'Glencore', 'Alcoa', 'South32', 'Yancoal', 'Peabody']
            
            # Check for company-specific logo patterns
            for pattern in company_logo_patterns:
                matches = re.findall(pattern, element_html, re.IGNORECASE)
                if matches:
                    for company in major_companies:
                        if company.lower() in matches[0].lower():
                            job_data['company_name'] = company
                            company_found = True
                            break
                    if company_found:
                        break
            
            # If not found in logos, check all alt attributes for company names
            if not company_found:
                alt_matches = re.findall(r'alt=["\']([^"\']+)["\']', element_html)
                
                for alt_text in alt_matches:
                    alt_clean = alt_text.strip()
                    if alt_clean and len(alt_clean) > 1:
                        # Direct match with major companies (exact or partial)
                        for company in major_companies:
                            if (company.lower() == alt_clean.lower() or 
                                company.lower() in alt_clean.lower() or
                                alt_clean.lower() in company.lower()):
                                job_data['company_name'] = company
                                company_found = True
                                break
                        if company_found:
                            break
            
            # Method 2: AGGRESSIVELY search for major mining companies in ALL text
            if not company_found:
                # Known major mining companies - search for these specifically
                major_companies = ['BHP', 'Rio Tinto', 'Fortescue', 'Newcrest', 'Santos', 'Woodside', 
                                 'Anglo American', 'Glencore', 'Alcoa', 'South32', 'Yancoal', 'Peabody',
                                 'Thiess', 'Downer', 'Barminco', 'Macmahon']
                
                # Search entire element text for major company names
                element_text_lower = element_text.lower()
                for company in major_companies:
                    if company.lower() in element_text_lower:
                        job_data['company_name'] = company
                        company_found = True
                        break
                
                # If not found, look line by line for company patterns
                if not company_found:
                    for line in lines:
                        line_clean = line.strip()
                        if (line_clean and 3 <= len(line_clean) <= 40 and
                            line_clean[0].isupper() and  # Starts with capital
                            not any(invalid_word in line_clean.lower() for invalid_word in [
                                'posted', 'ago', 'hour', 'day', 'week', 'month', 'salary', 'apply',
                                'manager', 'supervisor', 'engineer', 'operator', 'technician', 
                                'mechanic', 'fitter', 'coordinator', 'assistant', 'specialist',
                                'page', 'jobs', '1-20 of', 'see all', 'mining careers',
                                'full time', 'part time', 'contract', 'permanent', 'temporary',
                                'location', 'salary', 'benefits', 'description', 'requirements'
                            ]) and
                            not re.match(r'^\d', line_clean) and  # Not starting with number
                            not re.match(r'^[A-Z]{2,3}$', line_clean)):  # Not just state codes like NSW, QLD
                            
                            # Additional checks for company-like formatting
                            words = line_clean.split()
                            if len(words) <= 4:  # Company names usually 1-4 words
                                # Check if it looks like a proper company name
                                if (any(word[0].isupper() for word in words) and  # At least one capitalized word
                                    not all(word.isupper() for word in words if len(word) > 2)):  # Not all caps
                                    job_data['company_name'] = line_clean
                                    company_found = True
                                    break
            
            # Method 3: Extract from href attributes that might contain company info
            if not company_found:
                href_matches = re.findall(r'href=["\']([^"\']*company[^"\']*|[^"\']*employer[^"\']*)["\']', element_html, re.IGNORECASE)
                for href in href_matches:
                    # Extract company name from URL patterns
                    company_match = re.search(r'/company/([^/]+)|employer/([^/]+)', href)
                    if company_match:
                        company_from_url = company_match.group(1) or company_match.group(2)
                        if company_from_url:
                            # Clean up URL encoding and format
                            company_clean = company_from_url.replace('-', ' ').replace('_', ' ')
                            company_clean = ' '.join(word.capitalize() for word in company_clean.split())
                            if len(company_clean) > 2 and len(company_clean) < 50:
                                job_data['company_name'] = company_clean
                                company_found = True
                                break
            
            # Method 4: Look for any reasonable text that could be a company name (AVOID LOCATIONS & JOB TITLES)
            if not company_found:
                # Common Australian location patterns to avoid
                location_patterns = [
                    r'\b(sydney|melbourne|brisbane|perth|adelaide|darwin|hobart)\b',
                    r'\b(nsw|vic|qld|wa|sa|nt|tas)\b',
                    r'\b(new south wales|victoria|queensland|western australia|south australia|northern territory|tasmania)\b',
                    r'\b(weipa|roxby downs|pilbara|hunter valley|bowen basin|olympic dam|moranbah|collie|bell bay|ulan|nifty|byerwen)\b',
                    r'[a-z\s]+,\s*(nsw|vic|qld|wa|sa|nt|tas)\b',  # City, State format
                ]
                
                # Job title patterns to avoid (these are job roles, not companies)
                job_title_patterns = [
                    r'\b(manager|supervisor|superintendent|engineer|technician|operator|mechanic|fitter|coordinator|assistant|specialist|analyst|officer|representative|administrator|maintenance|safety|hr|human resources|finance|accountant|geologist|surveyor|driver|driller|blaster|advisor|lead|leader|team)\b',
                    r'\b(fixed plant|mobile plant|underground|surface|processing|production|operations|logistics|quality|environmental|training|recruitment|payroll)\b'
                ]
                
                for line in lines[1:6]:  # Check first few lines after title
                    line_clean = line.strip()
                    if (line_clean and 2 <= len(line_clean) <= 35 and
                        not line_clean.lower() in ['full time', 'part time', 'contract', 'permanent'] and
                        not any(time_word in line_clean.lower() for time_word in [
                            'ago', 'hour', 'day', 'week', 'month', 'posted', 'apply', 'salary'
                        ]) and
                        not re.match(r'^\d+', line_clean) and  # Not starting with numbers
                        not re.match(r'^[A-Z]{2,3},?\s*[A-Z]{2,3}$', line_clean) and  # Not location codes
                        not any(re.search(pattern, line_clean, re.IGNORECASE) for pattern in location_patterns) and  # Not location names
                        not any(re.search(pattern, line_clean, re.IGNORECASE) for pattern in job_title_patterns)):  # Not job titles
                        
                        # Additional validation - must look like a company name
                        words = line_clean.split()
                        if (len(words) <= 3 and  # Companies usually 1-3 words
                            not line_clean.lower() == job_data['title'].lower() and  # Not same as job title
                            any(word[0].isupper() for word in words)):  # At least one capitalized word
                            job_data['company_name'] = line_clean
                            break
            
            # FINAL SAFETY CHECK: If company looks like location/title, use Unknown
            if (not company_found or 
                job_data['company_name'] == 'Unknown Company' or
                any(re.search(pattern, job_data['company_name'], re.IGNORECASE) for pattern in [
                    r'\b(nsw|vic|qld|wa|sa|nt|tas)\b',
                    r'[a-z\s]+,\s*(nsw|vic|qld|wa|sa|nt|tas)\b',  # City, State format
                    r'\b(sydney|melbourne|brisbane|perth|adelaide|darwin|hobart)\b',  # Major cities
                    r'\b(weipa|roxby downs|pilbara|hunter valley|bowen basin|olympic dam|moranbah|collie|bell bay|ulan|nifty|byerwen)\b',  # Mining locations
                    r'\b(manager|supervisor|superintendent|engineer|technician|fitter|coordinator|specialist|advisor|lead|leader)\b'  # Job titles
                ])):
                job_data['company_name'] = 'Unknown Company'
            
            # Extract location from Australian locations - IMPROVED LOGIC
            australian_locations = [
                # States first (more specific)
                ('NSW', 'New South Wales'), ('VIC', 'Victoria'), ('QLD', 'Queensland'), 
                ('WA', 'Western Australia'), ('SA', 'South Australia'), ('TAS', 'Tasmania'), 
                ('NT', 'Northern Territory'), ('ACT', 'Australian Capital Territory'),
                
                # Major mining locations
                ('Weipa', 'QLD'), ('Moranbah', 'QLD'), ('Bowen Basin', 'QLD'), ('Byerwen', 'QLD'),
                ('Roxby Downs', 'SA'), ('Olympic Dam', 'SA'),
                ('Pilbara', 'WA'), ('Perth', 'WA'), ('Nifty', 'WA'), ('Goldfields', 'WA'),
                ('Bell Bay', 'TAS'), ('Collie', 'WA'), ('Ulan', 'NSW'), ('Hunter Valley', 'NSW'),
                
                # Major cities
                ('Sydney', 'NSW'), ('Melbourne', 'VIC'), ('Brisbane', 'QLD'), 
                ('Adelaide', 'SA'), ('Hobart', 'TAS'), ('Darwin', 'NT'), ('Canberra', 'ACT')
            ]
            
            # Look for location patterns in text
            best_location = None
            location_context = ''
            
            for location_name, state in australian_locations:
                if location_name in element_text:
                    # Find the line with this location
                    for line in lines:
                        if location_name in line:
                            # Check if it's in a proper location context
                            line_lower = line.lower()
                            if any(indicator in line_lower for indicator in [location_name.lower(), state.lower()]):
                                # Prefer lines that look like proper location format
                                if ',' in line and len(line.split()) <= 4:
                                    best_location = line.strip()
                                    break
                                elif not best_location:  # Fallback
                                    best_location = f"{location_name}, {state}"
                    
                    if best_location:
                        job_data['location'] = best_location
                        break
            
            # Extract posted date
            time_indicators = ['ago', 'hour', 'day', 'week', 'month']
            for line in lines:
                if any(indicator in line.lower() for indicator in time_indicators):
                    job_data['posted_ago'] = line
                    break
            
            # Extract salary information - IMPROVED TO AVOID FALSE POSITIVES
            salary_indicators = ['$', 'salary', 'package', 'remuneration', 'compensation']
            for line in lines:
                line_lower = line.lower()
                # Only extract if it's a real salary line (contains $ or explicit salary words)
                if any(indicator in line_lower for indicator in ['$', 'salary', 'package', 'remuneration', 'compensation']):
                    # Avoid false positives from pagination numbers or random numbers
                    if not any(invalid in line_lower for invalid in [
                        '1-20 of', 'page', 'jobs', 'of 478', 'posted', 'hour ago', 'day ago'
                    ]):
                        # Must contain actual salary-related content
                        if '$' in line or any(word in line_lower for word in ['salary', 'package', 'remuneration', 'compensation']):
                            job_data['salary_text'] = line
                            break
            
            # Try to extract job URL from href attributes in HTML
            href_match = re.search(r'href=["\']([^"\']+)["\']', element_html)
            if href_match:
                href = href_match.group(1)
                if href.startswith('http'):
                    job_data['job_url'] = href
                elif href.startswith('/'):
                    job_data['job_url'] = urljoin(self.base_url, href)
            
            # Clean up title
            job_data['title'] = job_data['title'].replace('Logo Image', '').replace('Image', '').strip()
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None
    
    def _get_full_job_description(self, page, job_url, job_title):
        """
        Get full job description by visiting the job detail page.
        
        Args:
            page: Playwright page object
            job_url (str): URL of the job listing
            job_title (str): Job title for reference
            
        Returns:
            str: Full job description
        """
        if not job_url or job_url in self.processed_urls:
            return 'No detailed description available'
        
        try:
            logger.info(f"Getting full description for: {job_title}")
            
            # Navigate to job detail page
            page.goto(job_url, wait_until='networkidle', timeout=30000)
            self.processed_urls.add(job_url)
            
            # Wait for content to load
            time.sleep(random.uniform(1, 2))
            
            # Try different selectors for job description content
            description_selectors = [
                '.job-description',
                '[class*="description"]',
                '.content',
                '[class*="content"]',
                'main',
                '.job-details',
                'body'  # Fallback to get all body content
            ]
            
            full_description = 'No detailed description available'
            
            for selector in description_selectors:
                try:
                    element = page.query_selector(selector)
                    if element:
                        text = element.inner_text().strip()
                        if len(text) > 100:  # Ensure we got substantial content
                            # Advanced cleaning to remove unwanted elements
                            lines = text.split('\n')
                            cleaned_lines = []
                            skip_sections = False
                            
                            # Remove unwanted navigation and site elements
                            # ENHANCED - remove unwanted footer and application elements
                            unwanted_elements = [
                                'mining careers navigation', 'site navigation', 'website menu',
                                'cookie policy', 'privacy policy', 'subscribe to newsletter', 
                                'footer navigation', 'header menu', 'sidebar menu',
                                'what is playwright', 'welcome admin', 'view site', 'change password',
                                'log out', 'all bookmarks', 'home page', 
                                'powered by emailoctopus', 'emailoctopus powered',
                                'get this job button', 'apply now button', 'get this job',
                                'let\'s get to work', 'apply via the company', 'apply now',
                                'pit n portal', 'apply via', 'company website', 'apply here',
                                'click to apply', 'view all jobs', 'more jobs', 'search jobs',
                                'see all company jobs', 'mining careers', 'apply via the company\'s website'
                            ]
                            
                            # Company names to remove from description (standalone lines)
                            company_names_to_remove = [
                                'rio tinto', 'bhp', 'fortescue', 'newcrest', 'santos', 'woodside',
                                'anglo american', 'glencore', 'alcoa', 'south32', 'macmahon',
                                'barminco', 'yancoal', 'peabody', 'pit n portal'
                            ]
                            
                            for line in lines:
                                line = line.strip()
                                if line:
                                    # Skip lines that are clearly navigation/site elements
                                    line_lower = line.lower()
                                    
                                    # Skip unwanted elements
                                    if any(unwanted in line_lower for unwanted in unwanted_elements):
                                        continue
                                    
                                    # Skip standalone company names at the start
                                    if any(company.lower() == line_lower for company in company_names_to_remove):
                                        continue
                                    
                                    # Skip very short lines that are likely navigation
                                    if len(line) < 3:
                                        continue
                                    
                                    # Skip lines that are just numbers or single words (navigation)
                                    if re.match(r'^\d+$', line) or (len(line.split()) == 1 and len(line) < 15):
                                        continue
                                    
                                    # Start including content from the job title onwards
                                    if job_title.lower() in line_lower or any(keyword in line_lower for keyword in [
                                        'about bhp', 'about rio tinto', 'about the role', 'about you',
                                        'our operations', 'benefits', 'applications close', 'supporting',
                                        'the role will', 'in this role', 'what you', 'experience',
                                        'qualifications', 'skills', 'requirements'
                                    ]):
                                        skip_sections = False
                                    
                                    if not skip_sections:
                                        cleaned_lines.append(line)
                            
                            # Join lines and clean up extra whitespace
                            full_description = '\n'.join(cleaned_lines)
                            
                            # Remove multiple consecutive newlines
                            full_description = re.sub(r'\n{3,}', '\n\n', full_description)
                            
                            # Remove any remaining unwanted content at the start and end
                            description_lines = full_description.split('\n')
                            start_index = 0
                            end_index = len(description_lines)
                            
                            # Find proper start (skip company names and navigation)
                            for i, line in enumerate(description_lines):
                                line_lower = line.lower().strip()
                                if any(start_phrase in line_lower for start_phrase in [
                                    job_title.lower(), 'about bhp', 'about rio tinto', 'posted on',
                                    'team leader', 'we offer', 'about the role'
                                ]):
                                    start_index = i
                                    break
                            
                            # Find proper end (CONSERVATIVE - keep more content) 
                            end_index = len(description_lines)
                            
                            # Remove footer elements more aggressively - ENHANCED
                            footer_indicators = [
                                'get this job', 'let\'s get to work', 'apply via the company',
                                'apply now', 'see all company jobs', 'mining careers',
                                'the latest mining jobs', 'subscribe to our newsletter',
                                'copyright mining careers', '© copyright', 'powered by',
                                'pit n portal', 'apply via', 'company website', 'apply here',
                                'click to apply', 'view all jobs', 'more jobs', 'search jobs'
                            ]
                            
                            # Look for footer patterns from the end
                            for i in range(len(description_lines) - 1, -1, -1):
                                line_lower = description_lines[i].lower().strip()
                                if any(footer in line_lower for footer in footer_indicators):
                                    end_index = i
                                    continue  # Keep looking for earlier footer content
                                elif line_lower and len(line_lower) > 10:
                                    # Found substantial content, stop here
                                    break
                            
                            # Clean the description
                            if start_index < end_index:
                                clean_lines = description_lines[start_index:end_index]
                                # Remove standalone company names at the beginning
                                while clean_lines and any(company.lower() == clean_lines[0].lower().strip() 
                                                        for company in company_names_to_remove):
                                    clean_lines.pop(0)
                                full_description = '\n'.join(clean_lines)
                            
                            # Return FULL description - no length restrictions
                            full_description = full_description.strip()
                            
                            if len(full_description) > 100:  # Just ensure we have some content
                                break
                            
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            return full_description
            
        except Exception as e:
            logger.error(f"Error getting full description for {job_title}: {e}")
            return 'No detailed description available'
    
    def _detect_total_pages(self, page):
        """
        ENHANCED DYNAMIC: Detect total number of pages from pagination with improved logic.
        
        Args:
            page: Playwright page object
            
        Returns:
            int: Total number of pages, or None if not detected
        """
        try:
            logger.info("DYNAMIC: Starting pagination detection...")
            
            # Method 1: PRIORITY - Look for "X of Y" job count patterns (most reliable for MiningCareers)
            page_text = page.inner_text('body')
            job_count_patterns = [
                r'1\s*-\s*\d+\s+of\s+(\d+)',  # PRIORITY: "1-20 of 478" format
                r'showing\s+\d+-\d+\s+of\s+(\d+)',
                r'(\d+)\s+jobs?\s+found',
                r'(\d+)\s+total\s+jobs?',
                r'(\d+)\s+of\s+(\d+)\s+pages',
                r'page\s+\d+\s+of\s+(\d+)'
            ]
            
            for pattern in job_count_patterns:
                matches = re.findall(pattern, page_text.lower())
                if matches:
                    if isinstance(matches[0], tuple):
                        total_items = int(matches[0][-1])  # Last number in tuple
                    else:
                        total_items = int(matches[0])
                    
                    # Calculate pages based on total items (usually jobs)
                    if total_items >= 20:  # Valid job count
                        estimated_pages = (total_items + 19) // 20  # Round up division
                        logger.info(f"DYNAMIC: Found {total_items} total jobs, calculating {estimated_pages} pages")
                        return estimated_pages
            
            # Method 2: Look for ALL pagination links and find the highest page number (fallback only)
            page_links = page.query_selector_all('a[href*="page"]')
            max_page = 0
            page_numbers_found = []
            
            for link in page_links:
                href = link.get_attribute('href')
                text = link.inner_text().strip()
                
                # Extract page number from href
                if href:
                    page_match = re.search(r'page[=]?(\d+)', href)
                    if page_match:
                        page_num = int(page_match.group(1))
                        page_numbers_found.append(page_num)
                        max_page = max(max_page, page_num)
                        
                # Extract page number from text (but ignore "Next", "Previous" etc.)
                if text and text.isdigit() and text not in ['1', '0']:
                    page_num = int(text)
                    page_numbers_found.append(page_num)
                    max_page = max(max_page, page_num)
            
            # Only use page links if we found actual numbered pages (not just Next/Previous)
            if page_numbers_found and max_page > 2:
                logger.info(f"DYNAMIC: Found page numbers: {sorted(set(page_numbers_found))}")
                logger.info(f"DYNAMIC: Detected max page from ALL links: {max_page}")
                return max_page
            
            # Method 3: Look for pagination containers and extract all numeric links (fallback)
            pagination_selectors = [
                '.pagination a',
                '[class*="pagination"] a',
                'nav[aria-label*="pagination"] a',
                '.page-numbers a',
                '[class*="page"] a'
            ]
            
            for selector in pagination_selectors:
                try:
                    pagination_links = page.query_selector_all(selector)
                    found_pages = []
                    
                    for link in pagination_links:
                        text = link.inner_text().strip()
                        href = link.get_attribute('href')
                        
                        # Check text for page numbers
                        if text and text.isdigit():
                            found_pages.append(int(text))
                        
                        # Check href for page numbers  
                        if href:
                            page_match = re.search(r'page[=]?(\d+)', href)
                            if page_match:
                                found_pages.append(int(page_match.group(1)))
                    
                    if found_pages and max(found_pages) > 2:  # Only if we found meaningful page numbers
                        max_found = max(found_pages)
                        logger.info(f"DYNAMIC: Found pages in {selector}: {sorted(set(found_pages))}")
                        logger.info(f"DYNAMIC: Max page from {selector}: {max_found}")
                        return max_found
                        
                except Exception as e:
                    logger.debug(f"Pagination selector {selector} failed: {e}")
                    continue
            
            # Method 4: Manual check - look for specific common pagination patterns
            common_selectors = [
                'button:has-text("Last")',
                'a:has-text("Last")',
                'button[aria-label*="Last"]',
                'a[aria-label*="Last"]'
            ]
            
            for selector in common_selectors:
                try:
                    last_element = page.query_selector(selector)
                    if last_element:
                        href = last_element.get_attribute('href')
                        if href:
                            page_match = re.search(r'page[=]?(\d+)', href)
                            if page_match:
                                total_pages = int(page_match.group(1))
                                logger.info(f"DYNAMIC: Found total pages from 'Last' button: {total_pages}")
                                return total_pages
                except Exception as e:
                    continue
                
        except Exception as e:
            logger.error(f"Error detecting total pages: {e}")
        
        # Enhanced fallback - look for total job count in the page to estimate pages
        logger.warning("Could not detect total pages from pagination elements...")
        try:
            # Look for job count indicators on the page  
            page_content = page.inner_text().lower()
            
            # Common patterns for job counts
            job_count_patterns = [
                r'(\d+)\s+jobs?\s+found',
                r'(\d+)\s+results?',
                r'showing\s+\d+\s*-\s*\d+\s+of\s+(\d+)',
                r'1\s*-\s*\d+\s+of\s+(\d+)',
                r'(\d+)\s+total\s+jobs?',
                r'(\d{2,})\s+positions?',  # At least 2 digits for job count
            ]
            
            for pattern in job_count_patterns:
                matches = re.findall(pattern, page_content)
                if matches:
                    try:
                        total_jobs = int(matches[0])
                        if total_jobs > 20:  # Reasonable minimum for job count
                            # Estimate pages (usually ~20 jobs per page)
                            estimated_pages = (total_jobs + 19) // 20
                            logger.info(f"DYNAMIC: Found {total_jobs} total jobs, estimating {estimated_pages} pages")
                            return estimated_pages
                    except (ValueError, IndexError):
                        continue
            
            # Look for specific text that might indicate more pages
            if any(text in page_content for text in ['next page', 'page 2', 'page 3', 'more jobs']):
                logger.info("DYNAMIC: Found indicators of multiple pages, assuming at least 5 pages")
                return 5
                
        except Exception as e:
            logger.error(f"Error in job count estimation: {e}")
            
        # Final fallback - be conservative but thorough  
        logger.warning("Could not detect total pages, using conservative fallback of 10 pages")
        return 10

    def _extract_location_from_description(self, description):
        """
        Extract the correct location from the job description.
        
        Args:
            description (str): Full job description
            
        Returns:
            str: Extracted location or None
        """
        if not description:
            return None
        
        # Look for location patterns in the description
        lines = description.split('\n')[:15]  # Check first 15 lines
        
        # Mining location patterns with state abbreviations - MORE COMPREHENSIVE
        location_patterns = [
            # Primary mining locations with exact patterns
            (r'Weipa,?\s*QLD', 'Weipa, QLD'),
            (r'Weipa\s*,?\s*Queensland', 'Weipa, QLD'),
            (r'Moranbah,?\s*QLD', 'Moranbah, QLD'),
            (r'Bowen Basin,?\s*QLD', 'Bowen Basin, QLD'),
            (r'Roxby Downs,?\s*SA', 'Roxby Downs, SA'),
            (r'Olympic Dam,?\s*SA', 'Olympic Dam, SA'),
            (r'Pilbara,?\s*WA', 'Pilbara, WA'),
            (r'Perth,?\s*WA', 'Perth, WA'),
            (r'Bell Bay,?\s*TAS', 'Bell Bay, TAS'),
            (r'Collie,?\s*WA', 'Collie, WA'),
            (r'Ulan,?\s*NSW', 'Ulan, NSW'),
            (r'Hunter Valley,?\s*NSW', 'Hunter Valley, NSW'),
            (r'Byerwen,?\s*QLD', 'Byerwen, QLD'),
            (r'Nifty,?\s*WA', 'Nifty, WA'),
            
            # State patterns
            (r'Queensland', 'QLD'),
            (r'Western Australia', 'WA'),
            (r'South Australia', 'SA'),
            (r'New South Wales', 'NSW'),
            (r'Tasmania', 'TAS'),
            (r'Northern Territory', 'NT'),
        ]
        
        # First try to find specific mining locations
        for line in lines:
            line = line.strip()
            for pattern, location in location_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    # Prioritize specific mining locations over just state names
                    if ',' in location:  # Specific city, state format
                        return location
        
        # If no specific location found, try state patterns
        for line in lines:
            line = line.strip()
            for pattern, location in location_patterns:
                if re.search(pattern, line, re.IGNORECASE) and ',' not in location:
                    return location
        
        return None
    
    def _save_job_to_database_sync(self, job_data):
        """
        Save job data to database in a separate thread to avoid async context issues.
        
        Args:
            job_data (dict): Job information dictionary
            
        Returns:
            bool: Success status
        """
        def save_job():
            try:
                # Close any existing connections
                connections.close_all()
                
                with transaction.atomic():
                    # Get or create company
                    company = self._get_or_create_company(
                        job_data['company_name'],
                        job_data.get('company_logo', '')
                    )
                    
                    # Get or create location
                    location = self._get_or_create_location(job_data['location'])
                    
                    # Parse salary information
                    salary_info = self._extract_salary_info(job_data.get('salary_text', ''))
                    
                    # Categorize job
                    category = self._categorize_job(job_data['title'], job_data['description'])
                    
                    # Create unique external URL
                    external_url = job_data.get('job_url', f"{self.jobs_url}#{uuid.uuid4()}")
                    
                    # Check if job already exists
                    existing_job = JobPosting.objects.filter(external_url=external_url).first()
                    if existing_job:
                        logger.info(f"Job already exists: {job_data['title']} at {company.name}")
                        return False
                    
                    # Create job posting
                    job_posting = JobPosting.objects.create(
                        title=job_data['title'],
                        description=job_data['description'],
                        company=company,
                        location=location,
                        posted_by=self.scraper_user,
                        job_category=category,
                        job_type='full_time',  # Default for mining jobs
                        salary_min=salary_info.get('min'),
                        salary_max=salary_info.get('max'),
                        salary_currency=salary_info.get('currency', 'AUD'),
                        salary_type=salary_info.get('type', 'yearly'),
                        salary_raw_text=salary_info.get('raw_text', ''),
                        external_source='miningcareers.com.au',
                        external_url=external_url,
                        posted_ago=job_data.get('posted_ago', ''),
                        date_posted=self._parse_relative_date(job_data.get('posted_ago', '')),
                        status='active',
                        additional_info={
                            'scraper_version': '2.0',
                            'scraped_from': 'miningcareers.com.au',
                            'original_data': job_data
                        }
                    )
                    
                    logger.info(f"SUCCESS: Saved job: {job_data['title']} at {company.name} - {location.name}")
                    return True
                    
            except Exception as e:
                logger.error(f"ERROR: Error saving job to database: {e}")
                return False
        
        # Run in separate thread to avoid async context issues
        import threading
        result = [False]
        
        def run_save():
            result[0] = save_job()
        
        thread = threading.Thread(target=run_save)
        thread.start()
        thread.join()
        
        return result[0]
    
    def scrape_jobs(self):
        """
        Main method to scrape jobs from miningcareers.com.au.
        
        Returns:
            list: List of scraped job data
        """
        logger.info(f"Starting MiningCareers.com.au scraping session...")
        logger.info(f"Target: {self.max_jobs} jobs")
        
        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            
            # Create context with stealth settings
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            
            try:
                # Go to jobs page
                logger.info(f"Navigating to {self.jobs_url}")
                page.goto(self.jobs_url, wait_until='networkidle', timeout=30000)
                
                # Wait for job listings to load
                page.wait_for_selector('body', timeout=10000)
                time.sleep(random.uniform(3, 5))
                
                page_number = 1
                jobs_scraped = 0
                total_pages = None  # DYNAMIC: Will be detected on first page
                
                while jobs_scraped < self.max_jobs:
                    logger.info(f"Scraping page {page_number}...")
                    
                    # DYNAMIC: Detect total pages on first page
                    if total_pages is None:
                        total_pages = self._detect_total_pages(page)
                        logger.info(f"DYNAMIC: Total pages detected: {total_pages}")
                    
                    # Find job elements using improved selectors
                    job_selectors = [
                        'div:has(h3):has(img[alt]):has-text("ago")',  # Most specific
                        'div:has(h3):has-text("ago")',               # Good fallback
                        'div:has(h3)',                               # Basic fallback
                    ]
                    
                    job_elements = []
                    for selector in job_selectors:
                        try:
                            elements = page.query_selector_all(selector)
                            if elements:
                                job_elements = elements
                                logger.info(f"Found {len(job_elements)} job elements using selector: {selector}")
                                break
                        except Exception as e:
                            logger.debug(f"Selector {selector} failed: {e}")
                            continue
                    
                    if not job_elements:
                        logger.warning("No job elements found on page")
                        break
                    
                    # DYNAMIC: Check if we've exceeded total pages
                    if total_pages and page_number > total_pages:
                        logger.info(f"DYNAMIC: Reached end of pages (page {page_number} > {total_pages}). Stopping.")
                        break
                    
                    # Extract data from each job element (before any navigation)
                    job_data_list = []
                    for i, job_element in enumerate(job_elements):
                        if jobs_scraped >= self.max_jobs:
                            break
                        
                        logger.info(f"Extracting job {i+1}/{len(job_elements)}")
                        job_data = self._extract_job_data_safely(job_element)
                        
                        if job_data and job_data['title'] != 'Unknown Position':
                            job_data_list.append(job_data)
                        else:
                            logger.debug(f"Skipped invalid job data: {job_data}")
                        
                        # Small delay between extractions
                        time.sleep(random.uniform(0.1, 0.3))
                    
                    # Now get full descriptions and save jobs to database
                    for job_data in job_data_list:
                        if jobs_scraped >= self.max_jobs:
                            break
                        
                        # Get full job description if URL is available
                        if job_data.get('job_url'):
                            try:
                                full_description = self._get_full_job_description(
                                    page, job_data['job_url'], job_data['title']
                                )
                                job_data['description'] = full_description
                                
                                # Extract correct location from the job detail page
                                correct_location = self._extract_location_from_description(full_description)
                                if correct_location:
                                    job_data['location'] = correct_location
                                    
                            except Exception as e:
                                logger.error(f"Failed to get description for {job_data['title']}: {e}")
                        
                        # Save to database
                        if self._save_job_to_database_sync(job_data):
                            jobs_scraped += 1
                            self.scraped_jobs.append(job_data)
                            logger.info(f"Saved job {jobs_scraped}/{self.max_jobs}: {job_data['title']}")
                        
                        # Add delay between operations
                        time.sleep(random.uniform(1.0, 2.0))
                    
                    # Try to go to next page - IMPROVED ERROR HANDLING
                    if jobs_scraped < self.max_jobs:
                        # DYNAMIC: Check if we've reached the detected total pages
                        if total_pages and page_number >= total_pages:
                            logger.info(f"DYNAMIC: Reached last page ({page_number} of {total_pages}). Stopping.")
                            break
                        
                        try:
                            # Check if page is still valid before proceeding
                            if page.is_closed():
                                logger.error("Page has been closed. Cannot navigate to next page.")
                                break
                            
                            # IMPROVED - Try different next page selectors to match the actual website
                            next_selectors = [
                                'button:has-text("Next")',  # For button elements with "Next" text
                                'a:has-text("Next")',       # For link elements with "Next" text  
                                '[aria-label*="Next"]',    # For accessibility labels
                                'button[class*="next"]',    # Button with next class
                                'a[class*="next"]',        # Link with next class
                                'a[href*="page"]:has-text("Next")',  # Original
                                '.next', 
                                '[class*="next"]',
                                'a[href*="page="]:last-child',
                                '.pagination a:last-child',
                                'button:text("Next")',     # Alternative text selector
                                'a:text("Next")'           # Alternative text selector
                            ]
                            
                            next_button = None
                            for selector in next_selectors:
                                try:
                                    next_button = page.query_selector(selector)
                                    if next_button:
                                        logger.info(f"Found next button using selector: {selector}")
                                        break
                                    else:
                                        logger.debug(f"No button found with selector: {selector}")
                                except Exception as e:
                                    logger.debug(f"Error with selector {selector}: {e}")
                                    continue
                            
                            # FALLBACK: Try direct URL navigation if no next button found
                            if not next_button:
                                logger.info("No next button found. Trying direct URL navigation...")
                                try:
                                    # Since we know there are 2 pages, try direct navigation
                                    if page_number < total_pages:
                                        next_page_url = f"https://www.miningcareers.com.au/jobs/?page={page_number + 1}"
                                        logger.info(f"Trying direct navigation to: {next_page_url}")
                                        page.goto(next_page_url, wait_until='domcontentloaded', timeout=30000)
                                        page_number += 1
                                        time.sleep(random.uniform(2, 4))
                                        logger.info(f"Successfully navigated to page {page_number}")
                                        continue  # Skip the rest of the button logic
                                    else:
                                        logger.info(f"Reached last page ({page_number} of {total_pages})")
                                        break
                                except Exception as direct_nav_error:
                                    logger.error(f"Direct navigation failed: {direct_nav_error}")
                                    
                                # If direct navigation fails, try additional debugging
                                logger.info("Checking all available buttons and links...")
                                try:
                                    # Check all buttons
                                    all_buttons = page.query_selector_all('button')
                                    logger.info(f"Found {len(all_buttons)} total buttons on page")
                                    for i, btn in enumerate(all_buttons[:5]):  # Check first 5 buttons
                                        text = btn.inner_text().strip()
                                        if text:
                                            logger.info(f"Button {i+1}: '{text}'")
                                    
                                    # Check all links  
                                    all_links = page.query_selector_all('a')
                                    logger.info(f"Found {len(all_links)} total links on page")
                                    for i, link in enumerate(all_links):
                                        text = link.inner_text().strip()
                                        href = link.get_attribute('href')
                                        if text and ('next' in text.lower() or 'page' in (href or '')):
                                            logger.info(f"Link {i+1}: '{text}' -> {href}")
                                    
                                except Exception as debug_error:
                                    logger.error(f"Error during debugging: {debug_error}")
                            
                            if next_button:
                                try:
                                    # More robust navigation
                                    logger.info(f"Attempting to navigate to page {page_number + 1}")
                                    
                                    # Get the href for manual navigation (more reliable)
                                    href = next_button.get_attribute('href')
                                    if href:
                                        # Use direct navigation instead of click
                                        full_url = f"https://www.miningcareers.com.au{href}" if not href.startswith('http') else href
                                        page.goto(full_url, wait_until='domcontentloaded', timeout=30000)
                                    else:
                                        # Fallback to click
                                        next_button.click()
                                        page.wait_for_load_state('domcontentloaded', timeout=30000)
                                    
                                    page_number += 1
                                    time.sleep(random.uniform(2, 4))
                                    
                                    # DYNAMIC: Additional check after navigation
                                    if total_pages and page_number > total_pages:
                                        logger.info(f"DYNAMIC: Navigated beyond total pages ({page_number} > {total_pages}). Stopping.")
                                        break
                                        
                                except Exception as nav_error:
                                    logger.error(f"Navigation error: {nav_error}")
                                    # Try direct URL construction as fallback
                                    try:
                                        fallback_url = f"https://www.miningcareers.com.au/jobs/?page={page_number + 1}"
                                        logger.info(f"Trying fallback URL: {fallback_url}")
                                        page.goto(fallback_url, wait_until='domcontentloaded', timeout=30000)
                                        page_number += 1
                                        time.sleep(random.uniform(2, 4))
                                    except Exception as fallback_error:
                                        logger.error(f"Fallback navigation failed: {fallback_error}")
                                        break
                            else:
                                logger.info("No next page button found or reached job limit")
                                break
                                
                        except Exception as e:
                            logger.error(f"Error during page navigation: {e}")
                            break
                    else:
                        break
                
            except Exception as e:
                logger.error(f"Scraping error: {e}")
                # Check if it's a browser-related error
                if "closed" in str(e).lower() or "target" in str(e).lower():
                    logger.error("Browser context lost. Cannot continue scraping.")
                else:
                    logger.error(f"Unexpected error: {e}")
            
            finally:
                try:
                    browser.close()
                except Exception as close_error:
                    logger.warning(f"Error closing browser: {close_error}")
        
        logger.info(f"Scraping completed! Total jobs scraped: {len(self.scraped_jobs)}")
        return self.scraped_jobs
    
    def get_stats(self):
        """Get scraping statistics."""
        total_jobs = JobPosting.objects.filter(external_source='miningcareers.com.au').count()
        recent_jobs = JobPosting.objects.filter(
            external_source='miningcareers.com.au',
            scraped_at__gte=timezone.now() - timedelta(days=1)
        ).count()
        
        return {
            'total_jobs_in_db': total_jobs,
            'recent_jobs_24h': recent_jobs,
            'current_session': len(self.scraped_jobs)
        }


def main():
    """Main function to run the scraper."""
    # Get max jobs from command line argument
    max_jobs = 50
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid max_jobs argument. Using default of 50.")
    
    # Create and run scraper
    scraper = MiningCareersJobScraper(max_jobs=max_jobs, headless=True)
    
    try:
        # Scrape jobs
        scraped_jobs = scraper.scrape_jobs()
        
        # Print statistics
        stats = scraper.get_stats()
        logger.info("=" * 60)
        logger.info("MINING CAREERS SCRAPING STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total jobs in database: {stats['total_jobs_in_db']}")
        logger.info(f"Jobs scraped in last 24h: {stats['recent_jobs_24h']}")
        logger.info(f"Jobs scraped this session: {stats['current_session']}")
        logger.info("=" * 60)
        
        # Print sample jobs
        if scraped_jobs:
            logger.info("Sample scraped jobs:")
            for i, job in enumerate(scraped_jobs[:3]):
                logger.info(f"{i+1}. {job['title']} at {job['company_name']} - {job['location']}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        sys.exit(1)


def run(max_jobs: int = 300):
    """Entry point used by the Celery scheduler.

    Creates a `MiningCareersJobScraper`, runs it, and returns a small summary
    dictionary that the task can record/log.
    """
    try:
        scraper = MiningCareersJobScraper(max_jobs=max_jobs, headless=True)
        scraped_jobs = scraper.scrape_jobs()
        stats = scraper.get_stats()
        return {
            'success': True,
            'scraped_count': len(scraped_jobs),
            'db_total': stats.get('total_jobs_in_db'),
            'recent_24h': stats.get('recent_jobs_24h'),
            'message': f"Successfully scraped {len(scraped_jobs)} MiningCareers jobs"
        }
    except Exception as e:
        logger.error(f"Scraping failed in run(): {e}")
        return {
            'success': False,
            'error': str(e),
        }


if __name__ == "__main__":
    main()
