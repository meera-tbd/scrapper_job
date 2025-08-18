#!/usr/bin/env python3
"""
Professional Prosple Australia Job Scraper using Playwright
===========================================================

Advanced Playwright-based scraper for Prosple Australia (https://au.prosple.com/search-jobs) 
that integrates with your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Graduate and entry-level job optimization

Features:
- 🎯 Smart job data extraction from Prosple Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional graduate job categorization

Usage:
    python prosple_australia_scraper.py [job_limit]
    
Examples:
    python prosple_australia_scraper.py 20    # Scrape 20 jobs
    python prosple_australia_scraper.py       # Scrape all available jobs
"""

import os
import sys
import django
import time
import random
import logging
import re
import json
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prosple_australia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProspleAustraliaScraper:
    """
    Professional scraper for Prosple Australia job listings
    """
    
    def __init__(self, max_jobs=None, headless=True, max_pages=None):
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.headless = headless
        self.base_url = "https://au.prosple.com"
        self.search_url = "https://au.prosple.com/search-jobs"
        self.search_url_with_location = "https://au.prosple.com/search-jobs?keywords=&locations=9692&defaults_applied=1"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'new_jobs': 0,
            'duplicate_jobs': 0,
            'errors': 0,
            'companies_created': 0,
            'locations_created': 0,
            'pages_scraped': 0,
            'total_pages_found': 0
        }
        
        # Get or create default user for job postings
        self.default_user, _ = User.objects.get_or_create(
            username='prosple_australia_scraper',
            defaults={'email': 'scraper@prosple.com.au'}
        )
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        logger.info("Prosple Australia Scraper initialized")
        if max_jobs:
            logger.info(f"Job limit: {max_jobs}")
        else:
            logger.info("No job limit set - will scrape all available jobs")

    def human_delay(self, min_delay=1, max_delay=3):
        """Add human-like delays between requests"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def extract_salary_info(self, text):
        """Extract salary information from text"""
        if not text:
            return None, None, 'yearly', ''
            
        text = text.strip()
        original_text = text
        
        # Common salary patterns for graduate/entry-level positions
        patterns = [
            r'AUD\s*([\d,]+)\s*-\s*([\d,]+)',      # AUD 68,000 - 80,000
            r'(\$[\d,]+)\s*-\s*(\$[\d,]+)',        # $50,000 - $60,000
            r'(\$[\d,]+)\s*to\s*(\$[\d,]+)',       # $50,000 to $60,000
            r'AUD\s*([\d,]+)\s*\+',                # AUD 50,000+
            r'(\$[\d,]+)\s*\+',                    # $50,000+
            r'AUD\s*([\d,]+)',                     # AUD 50,000
            r'(\$[\d,]+)',                         # $50,000
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # 50-60k
            r'(\d{1,3}(?:,\d{3})*)\s*k',          # 50k
        ]
        
        salary_min = None
        salary_max = None
        salary_type = 'yearly'
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 2:
                        if 'k' in text.lower():
                            salary_min = Decimal(match.group(1).replace(',', '')) * 1000
                            salary_max = Decimal(match.group(2).replace(',', '')) * 1000
                        else:
                            # Handle both AUD and $ prefixes
                            salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                            salary_max = Decimal(match.group(2).replace('$', '').replace(',', ''))
                    else:
                        if 'k' in text.lower():
                            salary_min = Decimal(match.group(1).replace(',', '')) * 1000
                        else:
                            # Handle both AUD and $ prefixes
                            salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                        if '+' in text:
                            salary_max = None
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Determine salary type
        if 'hour' in text.lower():
            salary_type = 'hourly'
        elif 'day' in text.lower():
            salary_type = 'daily'
        elif 'week' in text.lower():
            salary_type = 'weekly'
        elif 'month' in text.lower():
            salary_type = 'monthly'
        
        return salary_min, salary_max, salary_type, original_text

    def parse_closing_date(self, date_text):
        """Parse closing date from various formats"""
        if not date_text:
            return None
            
        try:
            # Clean up date text
            date_text = date_text.strip().replace('Applications close:', '').replace('Closes:', '').strip()
            
            # Try different date formats
            formats = [
                '%d %b %Y',       # 27 Sep 2025
                '%d %B %Y',       # 27 September 2025
                '%d/%m/%Y',       # 27/09/2025
                '%d-%m-%Y',       # 27-09-2025
                '%Y-%m-%d',       # 2025-09-27
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_text, fmt).date()
                except ValueError:
                    continue
                    
        except Exception as e:
            logger.warning(f"Could not parse date: {date_text} - {e}")
            
        return None

    def get_or_create_company(self, company_name, company_url=None):
        """Get or create company with proper handling"""
        if not company_name or company_name.strip() == '':
            company_name = 'Unknown Company'
            
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                website=company_url if company_url else '',
                description=f'Organization posting graduate and professional jobs on Prosple Australia'
            )
            self.stats['companies_created'] += 1
            logger.info(f"Created new company: {company_name}")
            
        return company

    def get_or_create_location(self, location_text):
        """Get or create location with proper handling"""
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
            
        location_text = location_text.strip()
        
        # Clean up location text
        location_text = re.sub(r'\s+', ' ', location_text)
        
        # Truncate to fit database field (varchar 100)
        location_text = location_text[:100]
        
        # Try to find existing location (case-insensitive)
        location = Location.objects.filter(
            name__iexact=location_text
        ).first()
        
        if not location:
            # Parse location components
            city = location_text
            state = 'Unknown'
            country = 'Australia'
            
            # Extract state if present
            aus_states = {
                'VIC': 'Victoria', 'NSW': 'New South Wales', 'QLD': 'Queensland',
                'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
                'ACT': 'Australian Capital Territory', 'NT': 'Northern Territory'
            }
            
            for abbr, full_name in aus_states.items():
                if abbr in location_text or full_name in location_text:
                    state = full_name
                    break
            
            location = Location.objects.create(
                name=location_text,
                city=city,
                state=state,
                country=country
            )
            self.stats['locations_created'] += 1
            logger.info(f"Created new location: {location_text}")
            
        return location

    def extract_jobs_from_nextjs_data(self, page):
        """Extract job data from Next.js __NEXT_DATA__ script tag"""
        try:
            logger.info("Attempting to extract jobs from Next.js data...")
            
            # Get the __NEXT_DATA__ script content
            next_data_script = page.query_selector('script#__NEXT_DATA__')
            if not next_data_script:
                logger.warning("No __NEXT_DATA__ script found")
                return []
            
            # Parse the JSON data
            json_content = next_data_script.inner_text()
            data = json.loads(json_content)
            
            # Navigate through the data structure to find jobs
            jobs = []
            
            # Common paths where job data might be stored in Next.js apps
            possible_paths = [
                ['props', 'pageProps', 'jobs'],
                ['props', 'pageProps', 'data', 'jobs'],
                ['props', 'pageProps', 'initialData', 'jobs'],
                ['props', 'data', 'jobs'],
                ['props', 'jobs'],
                ['props', 'pageProps', 'results'],
                ['props', 'pageProps', 'data', 'results'],
                ['props', 'pageProps', 'opportunities'],
                ['props', 'pageProps', 'data', 'opportunities']
            ]
            
            for path in possible_paths:
                try:
                    current = data
                    for key in path:
                        if isinstance(current, dict) and key in current:
                            current = current[key]
                        else:
                            break
                    else:
                        # Successfully navigated the full path
                        if isinstance(current, list) and current:
                            logger.info(f"Found job data at path: {' -> '.join(path)}")
                            jobs = current
                            break
                except Exception as e:
                    continue
            
            # If no jobs found in standard paths, search recursively
            if not jobs:
                logger.info("Searching for job data recursively...")
                jobs = self.find_jobs_recursive(data)
            
            if jobs:
                logger.info(f"Found {len(jobs)} jobs in Next.js data")
                logger.info(f"Sample job data: {jobs[0] if jobs else 'None'}")
                return self.parse_nextjs_jobs(jobs)
            else:
                logger.warning("No job data found in Next.js data structure")
                return []
                
        except Exception as e:
            logger.error(f"Error extracting jobs from Next.js data: {e}")
            return []

    def find_jobs_recursive(self, data, max_depth=5, current_depth=0):
        """Recursively search for job data in the JSON structure"""
        if current_depth > max_depth:
            return []
        
        if isinstance(data, list):
            # Check if this looks like a job list
            if len(data) > 0 and isinstance(data[0], dict):
                first_item = data[0]
                # Check if it has job-like properties
                job_indicators = ['title', 'company', 'location', 'url', 'id', 'slug']
                if any(key in first_item for key in job_indicators):
                    return data
        
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (list, dict)):
                    result = self.find_jobs_recursive(value, max_depth, current_depth + 1)
                    if result:
                        return result
        
        return []

    def parse_nextjs_jobs(self, jobs_data):
        """Parse job data from Next.js format into standard format"""
        parsed_jobs = []
        
        logger.info(f"Parsing {len(jobs_data)} jobs from Next.js data")
        
        for i, job in enumerate(jobs_data):
            try:
                logger.info(f"Processing job {i+1}: {job}")
                
                if not isinstance(job, dict):
                    logger.warning(f"Job {i+1} is not a dict: {type(job)}")
                    continue
                
                # Extract basic job information with various possible field names
                job_data = {
                    'title': job.get('title', job.get('name', job.get('position', ''))),
                    'company': '',
                    'location': '',
                    'url': job.get('url', job.get('link', job.get('href', ''))),
                    'salary': job.get('salary', job.get('pay', job.get('compensation', ''))),
                    'job_type': job.get('employment_type', job.get('type', job.get('workType', 'full_time'))),
                    'posted_ago': job.get('posted_date', job.get('created_at', job.get('datePosted', '')))
                }
                
                # Handle company - could be string or object
                company = job.get('company', job.get('employer', job.get('organisation', '')))
                if isinstance(company, dict):
                    job_data['company'] = company.get('name', company.get('title', ''))
                else:
                    job_data['company'] = str(company) if company else ''
                
                # Handle location - could be string or object
                location = job.get('location', job.get('city', job.get('state', '')))
                if isinstance(location, dict):
                    city = location.get('city', location.get('name', ''))
                    state = location.get('state', location.get('region', ''))
                    job_data['location'] = f"{city}, {state}".strip(', ') if city or state else 'Australia'
                elif isinstance(location, list) and location:
                    job_data['location'] = ', '.join(str(loc) for loc in location)
                else:
                    job_data['location'] = str(location) if location else 'Australia'
                
                # Handle different URL formats
                if job_data['url'] and not job_data['url'].startswith('http'):
                    if job_data['url'].startswith('/'):
                        job_data['url'] = urljoin(self.base_url, job_data['url'])
                    else:
                        job_data['url'] = f"{self.base_url}/{job_data['url']}"
                
                # Validate essential data
                if job_data['title'] and len(job_data['title']) > 2:
                    parsed_jobs.append(job_data)
                    logger.info(f"Successfully parsed job: {job_data['title']} at {job_data['company']}")
                else:
                    logger.warning(f"Job {i+1} has no valid title: {job_data}")
                
            except Exception as e:
                logger.error(f"Error parsing job {i+1} from JSON: {e}")
                logger.error(f"Job data was: {job}")
                continue
        
        logger.info(f"Successfully parsed {len(parsed_jobs)} jobs from Next.js data")
        return parsed_jobs

    def extract_job_data(self, job_element, page):
        """Extract ONLY title and URL from job listing element"""
        try:
            job_data = {}
            
            # Extract job title and URL using generic selectors (no hardcoded URLs)
            title_element = job_element.query_selector('h2 a, h1 a, h3 a, a[href*="/job"], a[href*="/career"], a[href*="/position"]')
            if not title_element:
                # Additional fallback selectors  
                title_element = job_element.query_selector('a[target="_blank"], section a, div a, a:first-of-type')
            
            if title_element:
                job_data['title'] = title_element.inner_text().strip()
                job_data['url'] = title_element.get_attribute('href')
                if job_data['url'] and not job_data['url'].startswith('http'):
                    job_data['url'] = urljoin(self.base_url, job_data['url'])
            else:
                logger.warning("No title element found")
                return None
            
            # Validate essential data
            if not job_data['title'] or len(job_data['title']) < 2:
                logger.warning("Job title too short or empty")
                return None
            
            # Truncate title to avoid database errors
            job_data['title'] = job_data['title'][:200]
            
            logger.info(f"Extracted job title: {job_data['title']}")
            logger.info(f"Job URL: {job_data['url']}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None

    def get_job_details(self, job_url, page):
        """Get detailed job information from the job detail page"""
        try:
            page.goto(job_url)
            self.human_delay(2, 4)
            
            # Wait for content to load
            page.wait_for_selector('body, .job-detail, .content', timeout=10000)
            
            job_details = {
                'description': 'Job listing from Prosple Australia.',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'full_time',
                'closing_date': None,
                'industry': '',
                'job_level': 'graduate'
            }
            
            # Extract company name using generic selectors (no hardcoded content)
            company_selectors = [
                'div[class*="masthead"] h2',          # h2 inside masthead-like div
                'div[class*="header"] h2',            # h2 inside header-like div
                'h2:first-of-type',                   # First h2 on page (usually company)
                'header h2',                          # h2 inside header element
                'h1:first-of-type',                   # First h1 (company name)
                '.company-name',                      # Standard class selectors
                '.employer-name', 
                '.job-company',
                '[data-testid*="company"]'            # Data attribute containing "company"
            ]
            
            for selector in company_selectors:
                company_element = page.query_selector(selector)
                if company_element:
                    company_text = company_element.inner_text().strip()
                    if company_text and len(company_text) > 1:
                        job_details['company'] = company_text
                        break
            
            # Extract location using generic selectors (no hardcoded cities)
            location_selectors = [
                'div:has(svg[class*="map"]) p',        # Paragraph near map icon
                'div:has(svg) p:last-child',           # Last paragraph in div with any SVG
                'p:nth-of-type(2)',                    # Second paragraph (often location)
                'p:last-of-type',                      # Last paragraph (might be location)
                '[class*="location"] p',               # Paragraph in location-like class
                '.job-location',                       # Standard class selectors
                '.location',
                '[data-testid="location"]',
                '[data-testid*="location"]'            # Any data attribute with "location"
            ]
            
            for selector in location_selectors:
                location_element = page.query_selector(selector)
                if location_element:
                    location_text = location_element.inner_text().strip()
                    if location_text and len(location_text) > 1:
                        job_details['location'] = location_text
                        break
            
            # Extract salary information using semantic selectors (no static class dependencies)
            salary_selectors = [
                # Primary: salary icon (dollar sign SVG with specific path pattern) -> last span in that li
                'li[datatype="detail"]:has(svg path[d*="M128 24a104 104 0 1 0 104 104A104.11 104.11 0 0 0 128 24m0 192a88 88 0 1 1 88-88a88.1 88.1 0 0 1-88 88m40-68a28 28 0 0 1-28 28h-4v8a8 8 0 0 1-16 0v-8h-16a8 8 0 0 1 0-16h36a12 12 0 0 0 0-24h-24a28 28 0 0 1 0-56h4v-8a8 8 0 0 1 16 0v8h16a8 8 0 0 1 0 16h-36a12 12 0 0 0 0 24h24a28 28 0 0 1 28 28"]) span:last-child',
                # Alternative: li containing hidden "Salary" text -> last span
                'li[datatype="detail"]:has(span[style*="position: absolute"]:contains("Salary")) span:last-child',
                # Backup: Look for specific dollar sign pattern in path
                'li:has(svg path[d*="m40-68a28 28 0 0 1-28 28"]) span:last-child',
                # Generic SVG with currency/dollar patterns -> parent li -> last span
                'li:has(svg[class*="dollar"]) span:last-child',           # SVG with dollar class
                'li:has(svg[class*="currency"]) span:last-child',         # SVG with currency class
                # Generic class-based fallbacks
                '[class*="salary"] span',                                # Span in salary-like class
                '.salary, .job-salary',                                  # Standard class selectors
                '[data-testid="salary"], [data-testid*="salary"]'        # Data attributes with "salary"
            ]
            
            for selector in salary_selectors:
                salary_element = page.query_selector(selector)
                if salary_element:
                    salary_text = salary_element.inner_text().strip()
                    if salary_text:
                        job_details['salary_raw_text'] = salary_text
                        salary_min, salary_max, salary_type, _ = self.extract_salary_info(salary_text)
                        job_details['salary_min'] = salary_min
                        job_details['salary_max'] = salary_max
                        job_details['salary_type'] = salary_type
                        break
            
            # Extract job type using generic selectors (no hardcoded types)
            type_selectors = [
                # Primary: briefcase/work icon with specific path pattern -> last span in that li
                'li[datatype="detail"]:has(svg path[d*="M216 56h-40v-8a24 24 0 0 0-24-24h-48a24 24 0 0 0-24 24v8H40a16 16 0 0 0-16 16v128a16 16 0 0 0 16 16h176a16 16 0 0 0 16-16V72a16 16 0 0 0-16-16"]) span:last-child',
                # Alternative: Look for hidden "Opportunity type" text
                'li[datatype="detail"]:has(span[style*="position: absolute"]:contains("Opportunity type")) span:last-child',
                # Backup: briefcase/work related selectors
                'li:has(svg[class*="briefcase"]) span:last-child', # Span in li with briefcase icon
                'li:has(svg[class*="work"]) span:last-child',       # Span in li with work icon
                # Generic class-based fallbacks
                '[class*="type"] span',                            # Span in type-like class
                '[class*="employment"] span',                      # Span in employment-like class
                '.job-type',                                       # Standard class selectors
                '.employment-type',
                '[data-testid="job-type"]',
                '[data-testid*="type"]'                            # Any data attribute with "type"
            ]
            
            for selector in type_selectors:
                type_element = page.query_selector(selector)
                if type_element:
                    type_text = type_element.inner_text().strip()
                    
                    # Skip if this contains salary information (AUD, $, numbers)
                    if any(indicator in type_text.upper() for indicator in ['AUD', '$', '000', 'YEAR', 'HOUR', 'WEEK', 'MONTH']):
                        continue
                        
                    type_text_lower = type_text.lower()
                    
                    # Dynamic job type mapping based on common keywords (no hardcoded types)
                    job_type_mapping = {
                        'intern': 'internship',
                        'clerkship': 'internship', 
                        'placement': 'internship',
                        'part': 'part_time',
                        'contract': 'contract',
                        'casual': 'casual',
                        'temporary': 'temporary',
                        'temp': 'temporary',
                        'graduate': 'graduate',
                        'grad': 'graduate',
                        'full': 'full_time',
                        'freelance': 'contract',
                        'consultant': 'contract',
                        'permanent': 'full_time'
                    }
                    
                    # Check for job type keywords in extracted text
                    job_details['job_type'] = 'full_time'  # Default
                    for keyword, job_type in job_type_mapping.items():
                        if keyword in type_text_lower:
                            job_details['job_type'] = job_type
                            break
                    
                    # If we found actual job type text, use it and break
                    if type_text and len(type_text) > 2 and job_details['job_type'] != 'full_time':
                        break
                    
                    # If no mapping found but text looks like job type, store original text (truncated)
                    if (job_details['job_type'] == 'full_time' and type_text and len(type_text) > 2 and 
                        not any(k in type_text_lower for k in job_type_mapping.keys())):
                        job_details['job_type'] = type_text[:50]
                        break
            
            # Extract closing date
            date_selectors = [
                '.closing-date',
                '.application-deadline',
                '[data-testid="closing-date"]',
                '.deadline'
            ]
            
            for selector in date_selectors:
                date_element = page.query_selector(selector)
                if date_element:
                    date_text = date_element.inner_text().strip()
                    job_details['closing_date'] = self.parse_closing_date(date_text)
                    if job_details['closing_date']:
                        break
            
            # Extract industry
            industry_selectors = [
                '.industry',
                '.job-industry',
                '[data-testid="industry"]',
                '.sector'
            ]
            
            for selector in industry_selectors:
                industry_element = page.query_selector(selector)
                if industry_element:
                    job_details['industry'] = industry_element.inner_text().strip()
                    break
            
            # Extract job description - based on provided HTML structure
            description = ''
            description_selectors = [
                '[data-testid="raw-html"]',  # Main selector based on provided HTML
                '.sc-c682c328-0',  # Alternative class-based selector
                '.job-description',  # Fallback selectors
                '.job-detail-description',
                '.description',
                '.job-content',
                '.job-summary',
                '.role-description',
                '[data-testid="job-description"]',
                'main .content',
                '.job-detail .content'
            ]
            
            for selector in description_selectors:
                try:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        desc_text = desc_element.inner_text().strip()
                        if len(desc_text) > 100:
                            description = desc_text
                            logger.info(f"Found description using selector: {selector}")
                            break
                except Exception as e:
                    continue
            
            # If no description found, try to get main content
            if not description:
                try:
                    main_content = page.query_selector('main, .main, #main, .container, .content-area')
                    if main_content:
                        full_text = main_content.inner_text().strip()
                        # Filter out navigation and header content
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                        content_lines = []
                        
                        for line in lines:
                            if len(line) > 20 and not any(keyword in line.lower() for keyword in 
                                ['navigation', 'menu', 'header', 'footer', 'search', 'filter']):
                                content_lines.append(line)
                        
                        if content_lines:
                            description = '\n'.join(content_lines[:50])  # Limit to first 50 meaningful lines
                            logger.info("Extracted description from main content area")
                except Exception as e:
                    logger.warning(f"Error extracting from main content: {e}")
            
            if description and len(description) > 50:
                job_details['description'] = description
            else:
                # Enhanced fallback description
                fallback_parts = [f"Position: {job_details.get('title', 'Graduate Position')}"]
                fallback_parts.append(f"Company: {job_details['company']}")
                fallback_parts.append(f"Location: {job_details['location']}")
                
                if job_details['industry']:
                    fallback_parts.append(f"Industry: {job_details['industry']}")
                if job_details['salary_raw_text']:
                    fallback_parts.append(f"Salary: {job_details['salary_raw_text']}")
                
                fallback_parts.append("This is a graduate and professional opportunity posted on Prosple Australia.")
                fallback_parts.append(f"For full job details, visit: {job_url}")
                
                job_details['description'] = '\n'.join(fallback_parts)
                logger.warning(f"Using enhanced fallback description for {job_url}")
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Could not get job details for {job_url}: {e}")
            return {
                'description': 'Graduate opportunity from Prosple Australia',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'graduate',
                'closing_date': None,
                'industry': '',
                'job_level': 'graduate'
            }

    def categorize_job(self, title, description, company_name):
        """Categorize job using the categorization service"""
        try:
            category = self.categorization_service.categorize_job(title, description)
            
            # Map to specific graduate/entry-level categories if applicable
            title_lower = title.lower()
            desc_lower = description.lower()
            
            # Graduate-specific categorizations
            if any(term in title_lower for term in ['graduate', 'entry level', 'junior', 'trainee']):
                if any(term in title_lower for term in ['analyst', 'data', 'research']):
                    return 'analyst'
                elif any(term in title_lower for term in ['engineer', 'software', 'developer']):
                    return 'engineering'
                elif any(term in title_lower for term in ['consultant', 'advisory']):
                    return 'consulting'
                elif any(term in title_lower for term in ['marketing', 'communications']):
                    return 'marketing'
                elif any(term in title_lower for term in ['finance', 'accounting']):
                    return 'finance'
                elif any(term in title_lower for term in ['hr', 'human resources']):
                    return 'human_resources'
            
            return category
            
        except Exception as e:
            logger.warning(f"Error categorizing job: {e}")
            return 'other'

    def save_job_from_data(self, job_data, page):
        """Save job to database from JSON data with proper error handling"""
        try:
            with transaction.atomic():
                # Check for duplicates
                if not job_data.get('url'):
                    logger.warning(f"No URL for job: {job_data.get('title', 'Unknown')}")
                    return False
                    
                existing_job = JobPosting.objects.filter(external_url=job_data['url']).first()
                if existing_job:
                    logger.info(f"Duplicate job found: {job_data['title']}")
                    self.stats['duplicate_jobs'] += 1
                    return False
                
                # Get detailed job information from the individual job page if URL is available
                if job_data['url']:
                    job_details = self.get_job_details(job_data['url'], page)
                else:
                    # Use data from JSON with fallbacks
                    job_details = {
                        'description': f"Graduate opportunity: {job_data.get('title', 'Position')} at {job_data.get('company', 'Unknown Company')}. For full details, visit Prosple Australia.",
                        'company': job_data.get('company', 'Unknown Company'),
                        'location': job_data.get('location', 'Australia'),
                        'salary_min': None,
                        'salary_max': None,
                        'salary_type': 'yearly',
                        'salary_raw_text': job_data.get('salary', ''),
                        'job_type': job_data.get('job_type', 'full_time'),
                        'closing_date': None,
                        'industry': '',
                        'job_level': 'graduate'
                    }
                    
                    # Extract salary info if available
                    if job_data.get('salary'):
                        salary_min, salary_max, salary_type, _ = self.extract_salary_info(job_data['salary'])
                        job_details['salary_min'] = salary_min
                        job_details['salary_max'] = salary_max
                        job_details['salary_type'] = salary_type
                
                # Get or create company
                company = self.get_or_create_company(job_details['company'])
                
                # Get or create location
                location = self.get_or_create_location(job_details['location'])
                
                # Categorize job
                category = self.categorize_job(job_data['title'], job_details['description'], job_details['company'])
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],
                    description=job_details['description'],
                    company=company,
                    location=location,
                    posted_by=self.default_user,
                    job_category=category,
                    job_type=job_details['job_type'],
                    salary_min=job_details['salary_min'],
                    salary_max=job_details['salary_max'],
                    salary_type=job_details['salary_type'],
                    salary_raw_text=job_details['salary_raw_text'][:200] if job_details['salary_raw_text'] else '',
                    external_source='prosple.com.au',
                    external_url=job_data['url'][:500] if job_data['url'] else '',
                    posted_ago=job_data.get('posted_ago', '')[:50],
                    status='active',
                    additional_info={
                        'closing_date': job_details['closing_date'].isoformat() if job_details['closing_date'] else None,
                        'industry': job_details['industry'],
                        'job_level': job_details['job_level'],
                        'scrape_timestamp': datetime.now().isoformat(),
                        'source_type': 'nextjs_json'
                    }
                )
                
                logger.info(f"Saved job: {job_data['title']} at {company.name}")
                self.stats['new_jobs'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    def save_job(self, job_data, page):
        """Save job to database with proper error handling"""
        try:
            with transaction.atomic():
                # Check for duplicates
                existing_job = JobPosting.objects.filter(external_url=job_data['url']).first()
                if existing_job:
                    logger.info(f"Duplicate job found: {job_data['title']}")
                    self.stats['duplicate_jobs'] += 1
                    return False
                
                # Get detailed job information from the individual job page
                job_details = self.get_job_details(job_data['url'], page)
                
                # Get or create company using details from job page
                company = self.get_or_create_company(job_details['company'])
                
                # Get or create location using details from job page
                location = self.get_or_create_location(job_details['location'])
                
                # Categorize job
                category = self.categorize_job(job_data['title'], job_details['description'], job_details['company'])
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],
                    description=job_details['description'],
                    company=company,
                    location=location,
                    posted_by=self.default_user,
                    job_category=category,
                    job_type=job_details['job_type'],
                    salary_min=job_details['salary_min'],
                    salary_max=job_details['salary_max'],
                    salary_type=job_details['salary_type'],
                    salary_raw_text=job_details['salary_raw_text'][:200] if job_details['salary_raw_text'] else '',
                    external_source='prosple.com.au',
                    external_url=job_data['url'][:500],
                    posted_ago=job_data.get('posted_ago', '')[:50],
                    status='active',
                    additional_info={
                        'closing_date': job_details['closing_date'].isoformat() if job_details['closing_date'] else None,
                        'industry': job_details['industry'],
                        'job_level': job_details['job_level'],
                        'scrape_timestamp': datetime.now().isoformat()
                    }
                )
                
                logger.info(f"Saved job: {job_data['title']} at {company.name}")
                self.stats['new_jobs'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    def scrape_jobs(self):
        """Main scraping method"""
        logger.info("Starting Prosple Australia job scraping...")
        
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(user_agent=self.user_agent)
            page = context.new_page()
            
            # Track network requests to find API calls
            api_requests = []
            def handle_request(request):
                if any(keyword in request.url.lower() for keyword in ['job', 'search', 'api', 'opportunity']):
                    api_requests.append(request.url)
                    logger.info(f"Captured relevant request: {request.url}")
            
            page.on("request", handle_request)
            
            try:
                # Navigate to the jobs page
                logger.info(f"Navigating to: {self.search_url}")
                page.goto(self.search_url)
                self.human_delay(3, 5)
                
                # Wait for page to load and dynamic content to render
                page.wait_for_selector('body', timeout=15000)
                
                # Wait for job content to load (give more time for React/Next.js to render)
                logger.info("Waiting for job content to load...")
                time.sleep(8)  # Give more time for JavaScript to execute and render content
                
                # Try to trigger search/filtering to load job data
                try:
                    # Look for search button or submit button
                    search_button = page.query_selector('button[type="submit"], .search-button, [class*="search"], button:has-text("Search")')
                    if search_button:
                        logger.info("Found search button, clicking to trigger job loading...")
                        search_button.click()
                        time.sleep(5)  # Wait for results to load
                        
                    # Also try pressing Enter in any search field
                    search_input = page.query_selector('input[type="search"], input[placeholder*="search"], input[name*="search"]')
                    if search_input:
                        logger.info("Found search input, pressing Enter to trigger search...")
                        search_input.press('Enter')
                        time.sleep(5)
                        
                except Exception as e:
                    logger.info(f"Could not interact with search elements: {e}")
                
                # Try to wait for common job-related elements
                try:
                    page.wait_for_selector('a[href*="/opportunities/"], .job, .card, [class*="job"]', timeout=15000)
                    logger.info("Job content detected")
                except Exception as e:
                    logger.warning(f"No job elements loaded within timeout: {e}")
                    logger.info("Proceeding with current page state")
                
                # Log any API requests that were captured
                if api_requests:
                    logger.info(f"Captured {len(api_requests)} relevant API requests:")
                    for url in api_requests:
                        logger.info(f"  - {url}")
                else:
                    logger.info("No job-related API requests detected")
                
                # Try alternative URL if needed
                if "403" in page.content() or "forbidden" in page.content().lower():
                    logger.info("Trying alternative URL with location filter...")
                    page.goto(self.search_url_with_location)
                    self.human_delay(3, 5)
                
                # Debug: Log page content to understand structure
                logger.info(f"Page URL: {page.url}")
                logger.info(f"Page title: {page.title()}")
                
                # Save page content for debugging
                with open('prosple_debug.html', 'w', encoding='utf-8') as f:
                    f.write(page.content())
                logger.info("Saved page content to prosple_debug.html for analysis")
                
                # Find job listings using generic selectors (no site-specific URLs)
                job_selectors = [
                    'li:has(section[role="button"])',         # Li with button section (stable structure)
                    'li:has(a[href*="/job"])',                # Li containing any job-related links
                    'li:has(a[href*="/career"])',             # Li containing career links
                    'li:has(a[href*="/position"])',           # Li containing position links
                    'li:has(h2)',                             # Li containing h2 titles
                    'li:has(h3)',                             # Li containing h3 titles
                    'section[role="button"]',                 # Direct section elements
                    'article',                                # Article elements (common for job listings)
                    '.job-listing',                           # Standard class selectors
                    '.job-card',
                    '.job-item',
                    '.position',
                    '.career-item',
                    '[data-testid*="job"]'                    # Any data attribute with "job"
                ]
                
                job_elements = []
                for selector in job_selectors:
                    try:
                        elements = page.query_selector_all(selector)
                        if elements:
                            job_elements = elements
                            logger.info(f"Found {len(elements)} jobs using selector: {selector}")
                            break
                        else:
                            logger.debug(f"No elements found with selector: {selector}")
                    except Exception as e:
                        logger.debug(f"Error with selector {selector}: {e}")
                        continue
                
                # If still no elements, try to extract from Next.js JSON data
                if not job_elements:
                    logger.warning("No job elements found with standard selectors, trying Next.js data extraction...")
                    nextjs_jobs = self.extract_jobs_from_nextjs_data(page)
                    
                    if nextjs_jobs:
                        logger.info(f"Successfully extracted {len(nextjs_jobs)} jobs from Next.js data")
                        # Process jobs from JSON data
                        jobs_to_process = nextjs_jobs[:self.max_jobs] if self.max_jobs else nextjs_jobs
                        
                        for i, job_data in enumerate(jobs_to_process, 1):
                            try:
                                logger.info(f"Processing job {i}/{len(jobs_to_process)}: {job_data['title']}")
                                self.stats['total_processed'] += 1
                                success = self.save_job_from_data(job_data, page)
                                
                                if success:
                                    logger.info(f"Successfully saved: {job_data['title']}")
                                
                                # Add delay between jobs
                                self.human_delay(1, 2)
                                
                            except Exception as e:
                                logger.error(f"Error processing job {i}: {e}")
                                self.stats['errors'] += 1
                                continue
                        
                        return  # Exit here since we processed jobs from JSON
                    
                    # Last resort: try to find any links that might be job listings
                    logger.warning("No Next.js data found, trying link analysis...")
                    all_links = page.query_selector_all('a[href]')
                    job_links = []
                    for link in all_links:
                        href = link.get_attribute('href')
                        if href and any(keyword in href.lower() for keyword in ['/job', '/position', '/opportunity', '/career']):
                            job_links.append(link)
                    
                    if job_links:
                        job_elements = job_links
                        logger.info(f"Found {len(job_links)} potential job links through link analysis")
                    else:
                        logger.error("No job elements found on the page")
                        logger.info("Content preview:")
                        logger.info(page.content()[:2000])
                        return
                
                # First, collect all job URLs to avoid DOM context issues
                logger.info("Step 1: Collecting all job URLs from the listing page...")
                job_urls = []
                jobs_to_process = job_elements[:self.max_jobs] if self.max_jobs else job_elements
                
                for i, job_element in enumerate(jobs_to_process, 1):
                    try:
                        logger.info(f"Extracting URL for job {i}/{len(jobs_to_process)}")
                        job_data = self.extract_job_data(job_element, page)
                        
                        if job_data and job_data.get('url'):
                            job_urls.append({
                                'title': job_data['title'], 
                                'url': job_data['url']
                            })
                            logger.info(f"Collected URL for: {job_data['title']}")
                        else:
                            logger.warning(f"Could not extract URL for job {i}")
                            
                    except Exception as e:
                        logger.error(f"Error extracting URL for job {i}: {e}")
                        continue
                
                logger.info(f"Step 2: Successfully collected {len(job_urls)} job URLs")
                
                # Now process each job URL individually to avoid DOM context issues
                successfully_extracted = 0
                for i, job_info in enumerate(job_urls, 1):
                    try:
                        logger.info(f"Processing job {i}/{len(job_urls)}: {job_info['title']}")
                        
                        # Create job_data structure for saving
                        job_data = {
                            'title': job_info['title'],
                            'url': job_info['url']
                        }
                        
                        self.stats['total_processed'] += 1
                        success = self.save_job(job_data, page)
                        
                        if success:
                            logger.info(f"Successfully saved: {job_data['title']}")
                            successfully_extracted += 1
                        
                        # Add delay between jobs
                        self.human_delay(1, 2)
                            
                    except Exception as e:
                        logger.error(f"Error processing job {i}: {e}")
                        self.stats['errors'] += 1
                        continue
                
                # If we found job elements but couldn't extract data from any of them,
                # try the Next.js JSON extraction as a fallback
                if job_elements and successfully_extracted == 0:
                    logger.warning("Found job elements but couldn't extract data from any. Trying Next.js data extraction as fallback...")
                    nextjs_jobs = self.extract_jobs_from_nextjs_data(page)
                    
                    if nextjs_jobs:
                        logger.info(f"Successfully extracted {len(nextjs_jobs)} jobs from Next.js data")
                        # Process jobs from JSON data
                        jobs_to_process = nextjs_jobs[:self.max_jobs] if self.max_jobs else nextjs_jobs
                        
                        for i, job_data in enumerate(jobs_to_process, 1):
                            try:
                                logger.info(f"Processing job {i}/{len(jobs_to_process)}: {job_data['title']}")
                                self.stats['total_processed'] += 1
                                success = self.save_job_from_data(job_data, page)
                                
                                if success:
                                    logger.info(f"Successfully saved: {job_data['title']}")
                                
                                # Add delay between jobs
                                self.human_delay(1, 2)
                                
                            except Exception as e:
                                logger.error(f"Error processing job {i}: {e}")
                                self.stats['errors'] += 1
                                continue
                
            except Exception as e:
                logger.error(f"Error during scraping: {e}")
                
            finally:
                browser.close()
        
        self.print_summary()

    def print_summary(self):
        """Print scraping summary"""
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total jobs processed: {self.stats['total_processed']}")
        logger.info(f"New jobs saved: {self.stats['new_jobs']}")
        logger.info(f"Duplicate jobs skipped: {self.stats['duplicate_jobs']}")
        logger.info(f"Companies created: {self.stats['companies_created']}")
        logger.info(f"Locations created: {self.stats['locations_created']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main function"""
    max_jobs = None
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
            logger.info(f"Job limit set to: {max_jobs}")
        except ValueError:
            logger.error("Invalid job limit. Please provide a number.")
            sys.exit(1)
    
    # Create and run scraper (headless=False to see the browser)
    scraper = ProspleAustraliaScraper(max_jobs=max_jobs, headless=False)
    scraper.scrape_jobs()


if __name__ == "__main__":
    main()
