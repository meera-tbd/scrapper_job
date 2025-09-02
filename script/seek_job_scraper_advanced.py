#!/usr/bin/env python
"""
Professional Seek.com.au Job Scraper using Playwright

This script uses the professional database structure similar to the Michael Page scraper
with JobPosting, Company, and Location models, plus automatic job categorization.

Features:
- Professional database structure with proper relationships
- Automatic job categorization using AI-like keyword matching
- Human-like behavior to avoid detection
- Complete data extraction and normalization
- Playwright for modern web scraping
- Configurable job limits

Usage:
    python scrape_seek_professional.py [max_jobs]

Example:
    python scrape_seek_professional.py 50
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
from playwright.sync_api import sync_playwright

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_professional.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProfessionalSeekScraper:
    """
    Professional Seek.com.au scraper using the advanced database structure.
    """
    
    def __init__(self, headless=False, job_category="all", job_limit=30):
        """Initialize the professional scraper."""
        self.headless = headless
        self.base_url = "https://www.seek.com.au"
        self.job_limit = job_limit
        
        # Set start URL based on job category
        if job_category == "all":
            self.start_url = "https://www.seek.com.au/jobs/in-All-Australia"
        elif job_category == "python":
            self.start_url = "https://www.seek.com.au/python-developer-jobs/in-All-Australia"
        else:
            self.start_url = f"https://www.seek.com.au/{job_category}-jobs/in-All-Australia"
            
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='seek_scraper_system',
                defaults={
                    'email': 'system@seekscraper.com',
                    'first_name': 'Seek',
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
    
    def human_delay(self, min_seconds=1, max_seconds=3):
        """Add human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Waiting {delay:.2f} seconds...")
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
    
    def parse_location(self, location_string):
        """Parse location string into normalized location data."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Australian state abbreviations
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
        
        # Split by comma first
        parts = [part.strip() for part in location_string.split(',')]
        
        city = ""
        state = ""
        country = "Australia"
        
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1]
            # Check if state part contains a known state abbreviation
            for abbrev, full_name in states.items():
                if abbrev in state_part:
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
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
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
    
    def extract_job_data(self, job_element, page):
        """Extract all available data from a job card element."""
        try:
            job_data = {}
            
            # Extract job title
            try:
                title_element = job_element.query_selector('[data-automation="jobTitle"]')
                job_data['job_title'] = title_element.inner_text().strip() if title_element else ""
            except:
                job_data['job_title'] = ""
            
            # Extract company name
            try:
                company_element = job_element.query_selector('[data-automation="jobCompany"]')
                job_data['company_name'] = company_element.inner_text().strip() if company_element else ""
            except:
                job_data['company_name'] = ""
            
            # Extract location
            try:
                location_element = job_element.query_selector('[data-automation="jobLocation"]')
                location_text = location_element.inner_text().strip() if location_element else ""
                job_data['location_text'] = location_text
            except:
                job_data['location_text'] = ""
            
            # Extract job URL
            try:
                link_element = job_element.query_selector('a[data-automation="jobTitle"]')
                if link_element:
                    href = link_element.get_attribute('href')
                    job_data['job_url'] = urljoin(self.base_url, href) if href else ""
                else:
                    job_data['job_url'] = ""
            except:
                job_data['job_url'] = ""
            
            # Extract posting date
            try:
                date_element = job_element.query_selector('[data-automation="jobListingDate"]')
                job_data['posted_ago'] = date_element.inner_text().strip() if date_element else ""
            except:
                job_data['posted_ago'] = ""
            
            # Extract job summary/description
            try:
                summary_element = job_element.query_selector('[data-automation="jobShortDescription"]')
                job_data['summary'] = summary_element.inner_text().strip() if summary_element else ""
            except:
                job_data['summary'] = ""
            
            # Extract salary information
            try:
                salary_element = job_element.query_selector('[data-automation="jobSalary"]')
                job_data['salary_text'] = salary_element.inner_text().strip() if salary_element else ""
            except:
                job_data['salary_text'] = ""
            
            # Extract job type and work mode from badges/tags
            try:
                badge_elements = job_element.query_selector_all('[data-automation="jobWorkType"], [data-automation="jobBadge"]')
                badges = []
                for badge in badge_elements:
                    badge_text = badge.inner_text().strip()
                    if badge_text:
                        badges.append(badge_text)
                job_data['badges'] = badges
            except:
                job_data['badges'] = []
            
            # Extract keywords
            try:
                all_text = job_element.inner_text()
                keywords = []
                common_terms = ['remote', 'hybrid', 'full-time', 'part-time', 'contract', 'permanent', 
                               'senior', 'junior', 'mid-level', 'graduate', 'internship']
                for term in common_terms:
                    if term.lower() in all_text.lower():
                        keywords.append(term)
                job_data['keywords'] = keywords
            except:
                job_data['keywords'] = []
            
            # Attempt to fetch the FULL job description from the job detail page
            # without navigating away or opening new tabs. We use a same-origin
            # fetch from within the page context and parse the HTML with DOMParser.
            try:
                job_url_for_description = job_data.get('job_url', '')
                if job_url_for_description:
                    description_text = page.evaluate(
                        """
                        async (url) => {
                            try {
                                const response = await fetch(url, { credentials: 'include' });
                                const html = await response.text();
                                const parser = new DOMParser();
                                const doc = parser.parseFromString(html, 'text/html');
                                const selectors = [
                                    '[data-automation="jobDescription"]',
                                    '[data-automation="jobAdDetails"]',
                                    '[data-automation="jobAd"]',
                                    '[data-automation="searchDetailJob"]',
                                    'div[data-automation="jobDetails"]',
                                    'section[data-automation="job-detail"]'
                                ];
                                for (const sel of selectors) {
                                    const el = doc.querySelector(sel);
                                    if (el && el.innerText && el.innerText.trim().length > 0) {
                                        return el.innerText.trim();
                                    }
                                }
                                const main = doc.querySelector('main') || doc.body;
                                return (main && main.innerText) ? main.innerText.trim() : '';
                            } catch (_) {
                                return '';
                            }
                        }
                        """,
                        job_url_for_description
                    )
                    if description_text and len(description_text) > len(job_data.get('summary', '') or ''):
                        job_data['summary'] = description_text
            except:
                # If anything goes wrong, keep the short summary already captured
                pass
            
            logger.debug(f"Extracted job data: {job_data['job_title']} at {job_data['company_name']}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {str(e)}")
            return None
    
    def save_job_to_database_sync(self, job_data):
        """Synchronous database save function to be called from thread."""
        try:
            # Close any existing connections to ensure fresh connection
            connections.close_all()
            
            with transaction.atomic():
                # Enhanced duplicate detection: Check both URL and title+company
                job_url = job_data['job_url']
                job_title = job_data['job_title']
                company_name = job_data['company_name']
                
                # Check 1: URL-based duplicate
                if JobPosting.objects.filter(external_url=job_url).exists():
                    logger.info(f"Duplicate job skipped (URL): {job_title} at {company_name}")
                    self.duplicate_count += 1
                    return False
                
                # Check 2: Title + Company duplicate (semantic duplicate)
                if JobPosting.objects.filter(
                    title=job_title, 
                    company__name=company_name
                ).exists():
                    logger.info(f"Duplicate job skipped (Title+Company): {job_title} at {company_name}")
                    self.duplicate_count += 1
                    return False
                
                # Parse and get or create location
                location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
                location_obj = None
                if location_name:
                    location_obj, created = Location.objects.get_or_create(
                        name=location_name,
                        defaults={
                            'city': city,
                            'state': state,
                            'country': country
                        }
                    )
                
                # Get or create company
                company_name = job_data.get('company_name', 'Unknown Company')
                company_slug = slugify(company_name)
                
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Jobs from Seek.com.au',
                        'website': '',
                        'company_size': 'medium'  # Default assumption
                    }
                )
                
                # Parse salary
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Parse date
                date_posted = self.parse_date(job_data.get('posted_ago', ''))
                
                # Determine job type and work mode from badges
                job_type = "full_time"  # Default
                work_mode = ""
                experience_level = ""
                
                badges = job_data.get('badges', []) + job_data.get('keywords', [])
                for badge in badges:
                    badge_lower = badge.lower()
                    if badge_lower in ['full-time', 'full time']:
                        job_type = "full_time"
                    elif badge_lower in ['part-time', 'part time']:
                        job_type = "part_time"
                    elif badge_lower in ['contract']:
                        job_type = "contract"
                    elif badge_lower in ['temporary']:
                        job_type = "temporary"
                    elif badge_lower in ['internship']:
                        job_type = "internship"
                    elif badge_lower in ['remote', 'hybrid', 'work from home']:
                        work_mode = badge
                    elif badge_lower in ['senior', 'junior', 'mid-level', 'graduate', 'entry level']:
                        experience_level = badge
                
                # Combine badges and keywords as tags
                all_tags = list(set(badges))  # Remove duplicates
                tags_string = ', '.join(all_tags)
                
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
                
                # Create the JobPosting object
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
                    external_source='seek.com.au',
                    external_url=job_data.get('job_url', ''),
                    status='active',
                    posted_ago=job_data.get('posted_ago', ''),
                    date_posted=date_posted,
                    tags=tags_string,
                    additional_info=job_data  # Store all extracted data
                )
                
                logger.info(f"Saved job: {job_posting.title} at {job_posting.company.name}")
                logger.info(f"  Category: {job_posting.job_category}")
                logger.info(f"  Location: {job_posting.location.name if job_posting.location else 'Not specified'}")
                logger.info(f"  Salary: {job_posting.salary_display}")
                
                self.scraped_count += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job to database: {str(e)}")
            self.error_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job data using thread-safe approach."""
        # Use ThreadPoolExecutor to run database operations in a separate thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.save_job_to_database_sync, job_data)
            try:
                result = future.result(timeout=30)  # 30 second timeout
                return result
            except concurrent.futures.TimeoutError:
                logger.error("Database save operation timed out")
                self.error_count += 1
                return False
            except Exception as e:
                logger.error(f"Error in threaded database save: {str(e)}")
                self.error_count += 1
                return False
    
    def scrape_page(self, page):
        """Scrape all job listings from the current page."""
        # Wait for job listings to load
        try:
            page.wait_for_selector('[data-automation="normalJob"]', timeout=10000)
        except:
            logger.warning("No job listings found on page")
            return 0
        
        # Scroll down to load all jobs
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        self.human_delay(2, 4)
        
        # Find all job card elements
        job_elements = page.query_selector_all('[data-automation="normalJob"]')
        logger.info(f"Found {len(job_elements)} job listings on current page")
        
        # Extract data from each job
        for i, job_element in enumerate(job_elements):
            try:
                # Check if we've reached the job limit
                if self.job_limit and self.scraped_count >= self.job_limit:
                    logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                    return -1  # Special return value to indicate limit reached
                
                # Scroll job into view
                job_element.scroll_into_view_if_needed()
                self.human_delay(0.5, 1.5)
                
                # Extract job data
                job_data = self.extract_job_data(job_element, page)
                if job_data and job_data.get('job_url'):
                    self.save_job_to_database(job_data)
                else:
                    logger.warning(f"Failed to extract data for job {i+1}")
                    
            except Exception as e:
                logger.error(f"Error processing job {i+1}: {str(e)}")
                self.error_count += 1
                continue
        
        return len(job_elements)
    
    def has_next_page(self, page):
        """Check if there's a next page available."""
        try:
            next_selectors = [
                'a[aria-label="Next"]',
                'a[data-automation="page-next"]', 
                'a:has-text("Next")',
                'a:has-text(">")',
                '[data-automation="pagination-next"]',
                '.pagination a:last-child',
                '[data-automation="pagination"] a:last-child',
                'nav a[aria-label="Next page"]',
                'button[aria-label="Next"]'
            ]
            
            for selector in next_selectors:
                next_element = page.query_selector(selector)
                if next_element and next_element.is_enabled():
                    return True
            
            return False
        except:
            return False
    
    def go_to_next_page(self, page):
        """Navigate to the next page of results."""
        try:
            next_selectors = [
                'a[aria-label="Next"]',
                'a[data-automation="page-next"]',
                'a:has-text("Next")',
                'a:has-text(">")',
                '[data-automation="pagination-next"]',
                '.pagination a:last-child',
                'nav a[aria-label="Next page"]',
                'button[aria-label="Next"]'
            ]
            
            for selector in next_selectors:
                next_element = page.query_selector(selector)
                if next_element and next_element.is_enabled():
                    logger.info("Clicking next page...")
                    
                    # Scroll to element and click
                    next_element.scroll_into_view_if_needed()
                    self.human_delay(1, 2)
                    next_element.click()
                    
                    # Wait for page to load with longer timeout
                    self.human_delay(3, 5)
                    page.wait_for_load_state('domcontentloaded', timeout=30000)
                    
                    return True
            
            logger.warning("No next page button found")
            return False
            
        except Exception as e:
            logger.error(f"Error navigating to next page: {str(e)}")
            return False
    
    def run(self):
        """Main method to run the complete scraping process."""
        logger.info("Starting Professional Seek.com.au job scraper...")
        logger.info(f"Target URL: {self.start_url}")
        logger.info(f"Job limit: {self.job_limit}")
        
        with sync_playwright() as p:
            # Launch browser with extended timeouts for Celery
            browser = p.chromium.launch(
                headless=self.headless,
                timeout=60000,  # 60 second timeout for browser launch
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
            
            # Create new page with realistic settings
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
            page = context.new_page()
            
            # Set extended timeouts for Celery environment
            page.set_default_timeout(90000)  # 90 seconds for all operations
            page.set_default_navigation_timeout(120000)  # 2 minutes for navigation
            
            try:
                # Navigate to starting URL with retry logic
                logger.info("Navigating to Seek.com.au...")
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        page.goto(self.start_url, wait_until='domcontentloaded', timeout=60000)
                        logger.info(f"Successfully loaded page on attempt {attempt + 1}")
                        break
                    except Exception as e:
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                        if attempt == max_retries - 1:
                            raise
                        self.human_delay(5, 10)
                
                self.human_delay(3, 5)
                
                page_number = 1
                total_jobs_found = 0
                
                while True:
                    logger.info(f"Scraping page {page_number}...")
                    
                    # Scrape current page
                    jobs_on_page = self.scrape_page(page)
                    
                    # Check if we reached the job limit
                    if jobs_on_page == -1:
                        logger.info("Job limit reached, stopping scraping.")
                        break
                    
                    total_jobs_found += jobs_on_page if jobs_on_page > 0 else 0
                    
                    if jobs_on_page == 0:
                        logger.warning("No jobs found on current page, stopping...")
                        break
                    
                    # Check if we've reached our job limit
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Scraping complete!")
                        break
                    
                    # Check if there's a next page
                    if not self.has_next_page(page):
                        logger.info("No more pages available, scraping complete!")
                        break
                    
                    # Navigate to next page
                    if not self.go_to_next_page(page):
                        logger.warning("Failed to navigate to next page, stopping...")
                        break
                    
                    page_number += 1
                    
                    # Add a longer delay between pages
                    self.human_delay(5, 8)
                
                # Final statistics
                logger.info("="*50)
                logger.info("PROFESSIONAL SCRAPING COMPLETED!")
                logger.info(f"Total pages scraped: {page_number}")
                logger.info(f"Total jobs found: {total_jobs_found}")
                logger.info(f"Jobs saved to database: {self.scraped_count}")
                logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
                logger.info(f"Errors encountered: {self.error_count}")
                # Get total job count using thread-safe approach
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(lambda: JobPosting.objects.count())
                        total_jobs_in_db = future.result(timeout=10)
                        logger.info(f"Total job postings in database: {total_jobs_in_db}")
                except:
                    logger.info("Total job postings in database: (count unavailable)")
                logger.info("="*50)
                
            except Exception as e:
                logger.error(f"Fatal error during scraping: {str(e)}")
                raise
            finally:
                browser.close()


def main():
    """Main function to run the professional scraper."""
    print("🔍 Professional Seek.com.au Job Scraper")
    print("="*50)
    
    # Parse command line arguments
    max_jobs = 30  # Default
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            print("Invalid number of jobs. Using default: 30")
    
    print(f"Target: {max_jobs} jobs from all categories")
    print("Database: Professional structure with JobPosting, Company, Location")
    print("="*50)
    
    # Create scraper instance with professional settings
    scraper = ProfessionalSeekScraper(
        headless=True, 
        job_category="all", 
        job_limit=max_jobs
    )
    
    try:
        # Run the scraping process
        scraper.run()
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


def run():
    """Standalone run function for Celery task execution."""
    try:
        # Create scraper instance with professional settings
        scraper = ProfessionalSeekScraper(
            headless=True, 
            job_category="all", 
            job_limit=300  # Default for scheduled runs
        )
        
        # Run the scraping process
        scraper.run()
        
        # Return summary for Celery task
        return {
            'success': True,
            'scraped_count': scraper.scraped_count,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count,
            'message': f'Successfully scraped {scraper.scraped_count} jobs'
        }
        
    except Exception as e:
        logger.error(f"Scraping failed in run(): {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'message': f'Scraping failed: {str(e)}'
        }


if __name__ == "__main__":
    main()