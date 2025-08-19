#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import logging
from decimal import Decimal
import concurrent.futures
import json

# Django setup (same as your professional scraper)
print("Setting up Django environment...")
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("Initializing Django...")
import django
django.setup()
print("Django setup completed")

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

print("Django imports completed")

User = get_user_model()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper_workinaus_fixed.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class WorkinAUSScraperFixed:
    def __init__(self, headless=True, job_limit=5, job_category="all", location="all"):
        """Initialize the scraper with improved validation and limits."""
        logger.info("Initializing WorkinAUS scraper...")
        
        self.headless = headless
        self.base_url = "https://workinaus.com.au"
        self.search_path = "/job/searched"
        self.page_param = "pageNo"
        self.job_limit = job_limit
        self.job_category = job_category
        self.location = location

        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0

        logger.info(f"Job limit: {job_limit} | Category: {job_category} | Location: {location}")
        
        # Get or create system user
        self.system_user = self.get_or_create_system_user()
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()

    def get_or_create_system_user(self):
        """Get or create system user for job postings."""
        try:
            user, created = User.objects.get_or_create(
                username='workinaus_scraper_system',
                defaults=dict(
                    email='system@workinaus-scraper.com',
                    first_name='WorkinAUS',
                    last_name='Scraper',
                    is_staff=True,
                    is_active=True,
                )
            )
            
            if created:
                logger.info("Created new system user: workinaus_scraper_system")
            else:
                logger.info("Found existing system user: workinaus_scraper_system")
            
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {e}")
            return None

    def human_delay(self, a=1.0, b=3.0):
        """Add a human-like delay between actions."""
        delay = random.uniform(a, b)
        time.sleep(delay)

    def parse_date(self, date_string):
        """Parse date string to extract date information."""
        if not date_string:
            return None
        
        s = date_string.lower().strip()
        now = timezone.now()
        
        if 'today' in s:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        if 'yesterday' in s:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        m = re.search(r'(\d+)\s*(hour|day|week|month)s?\s*ago', s)
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            delta = dict(hour=timedelta(hours=n),
                         day=timedelta(days=n),
                         week=timedelta(weeks=n),
                         month=timedelta(days=30*n)).get(unit, None)
            if delta:
                return now - delta
        
        return now.replace(hour=9, minute=0, second=0, microsecond=0)

    def parse_salary(self, salary_text):
        """Parse salary text to extract min and max values."""
        if not salary_text:
            return None, None
        
        # Remove common words and clean up
        salary_text = salary_text.lower().replace('salary', '').replace('package', '').strip()
        
        # Look for salary ranges like "$77,000 - $80,000 Annual" or "$77k - $80k"
        range_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)\s*-\s*\$?(\d+(?:,\d+)?(?:k|000)?)', salary_text)
        if range_match:
            min_sal = self._normalize_salary(range_match.group(1))
            max_sal = self._normalize_salary(range_match.group(2))
            return min_sal, max_sal
        
        # Look for single salary like "$60,000" or "$60k"
        single_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)', salary_text)
        if single_match:
            salary = self._normalize_salary(single_match.group(1))
            return salary, salary
        
        return None, None

    def _normalize_salary(self, salary_str):
        """Convert salary string to numeric value."""
        try:
            salary_str = salary_str.lower().replace(',', '').strip()
            if 'k' in salary_str:
                return int(salary_str.replace('k', '')) * 1000
            return int(salary_str)
        except (ValueError, AttributeError):
            return None

    def extract_job_data(self, job_card):
        """Extract job data from a job card element using WorkinAUS-specific structure."""
        try:
            job_data = {}
            
            # Get the full card text to parse manually
            card_text = job_card.evaluate("el => el.innerText")
            lines = [line.strip() for line in card_text.split('\n') if line.strip()]
            
            # WorkinAUS Structure Analysis:
            # - h2 contains COMPANY name
            # - Job title is in the text between company and "Full time"/"Part time"
            # - Location contains state abbreviations
            # - Salary contains $ and "Annual"/"Hourly"
            
            # Extract Company name (from h2)
            job_data['company_name'] = ""
            try:
                company_element = job_card.query_selector('h2')
                if company_element:
                    company_name = company_element.evaluate("el => el.innerText").strip()
                    if company_name and len(company_name) > 2:
                        job_data['company_name'] = company_name
            except:
                pass
            
            # Extract Job Title (text analysis approach)
            job_data['job_title'] = ""
            try:
                # Look for the job title in the text structure
                # It's typically after company name and before job type
                company_name = job_data.get('company_name', '')
                
                # Find job title by process of elimination
                australian_states = [
                    'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT',
                    'New South Wales', 'Victoria', 'Queensland', 'Western Australia',
                    'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory'
                ]
                
                for line in lines:
                    # Skip company name, job type, location, category, and other non-title lines
                    if (line and len(line) > 3 and len(line) < 80 and
                        line != company_name and
                        line != 'FEATURED' and
                        not any(word in line for word in ['Full time', 'Part time', 'Casual', 'Contract']) and
                        not any(state in line for state in australian_states) and
                        not '/' in line and  # Skip category lines like "Hospitality & Tourism / Chefs/Cooks"
                        not '$' in line and  # Skip salary lines
                        not 'seeking' in line.lower() and  # Skip description lines
                        not 'Apply' in line and
                        not ',' in line):  # Skip location lines with commas
                        job_data['job_title'] = line
                        break
            except:
                pass
            
            # Extract Location (text analysis approach)
            job_data['location_text'] = ""
            try:
                # Look for lines containing Australian state names or abbreviations
                australian_states = [
                    'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT',
                    'New South Wales', 'Victoria', 'Queensland', 'Western Australia',
                    'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory'
                ]
                
                for line in lines:
                    if any(state in line for state in australian_states):
                        # This line contains location information
                        if len(line) > 5 and len(line) < 100:
                            job_data['location_text'] = line
                            break
            except:
                pass
            
            # Extract Job URL (look for job detail links)
            job_data['job_url'] = ""
            try:
                # Look for any links in the job card
                links = job_card.query_selector_all('a[href]')
                for link in links:
                    href = link.get_attribute('href')
                    if href and '/job/details' in href:
                        if href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        else:
                            full_url = href
                        job_data['job_url'] = full_url
                        break
            except:
                pass
            
            # Extract Job Summary (text analysis approach)
            job_data['summary'] = ""
            try:
                # Look for descriptive text (longer lines that describe the job)
                australian_states = [
                    'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT',
                    'New South Wales', 'Victoria', 'Queensland', 'Western Australia',
                    'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory'
                ]
                
                for line in lines:
                    if (len(line) > 30 and len(line) < 300 and
                        'seeking' in line.lower() and
                        line != job_data.get('job_title', '') and
                        line != job_data.get('company_name', '') and
                        not '$' in line and
                        not any(state in line for state in australian_states)):
                        job_data['summary'] = line
                        break
            except:
                pass
            
            # Extract Salary information (text analysis approach)
            job_data['salary_text'] = ""
            try:
                # Look for lines containing $ and Annual/Hourly
                for line in lines:
                    if '$' in line and ('Annual' in line or 'Hourly' in line):
                        job_data['salary_text'] = line
                        break
            except:
                pass
            
            # Extract Job Type (text analysis approach)
            job_data['job_type_text'] = ""
            try:
                # Look for employment type
                for line in lines:
                    if any(word in line for word in ['Full time', 'Part time', 'Casual', 'Contract']):
                        job_data['job_type_text'] = line
                        break
            except:
                pass
            
            # Posted date - not easily available in WorkinAUS, set empty
            job_data['posted_ago'] = ""
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None

    def is_valid_job_data(self, job_data):
        """Validate job data before saving - improved validation."""
        if not job_data:
            return False
        
        # Must have a valid job title
        if not job_data.get('job_title') or len(job_data['job_title'].strip()) < 3:
            logger.debug(f"Invalid: No title or title too short")
            return False
        
        # Title should not be obviously wrong
        title = job_data['job_title'].lower()
        if any(word in title for word in ['unknown', 'error', 'loading', 'searching', 'filter', 'searched jobs']):
            logger.debug(f"Invalid: Suspicious title: {job_data['job_title']}")
            return False
        
        # Must have a company name
        if not job_data.get('company_name') or len(job_data['company_name'].strip()) < 2:
            logger.debug(f"Invalid: No company name")
            return False
        
        # Company name should not be obviously wrong
        company = job_data['company_name'].lower()
        if any(word in company for word in ['unknown', 'error', 'loading', 'searching', 'filter']):
            logger.debug(f"Invalid: Suspicious company name: {job_data['company_name']}")
            return False
        
        # Must have a location
        if not job_data.get('location_text') or len(job_data['location_text'].strip()) < 3:
            logger.debug(f"Invalid: No location")
            return False
        
        # Must have a valid URL if present
        if job_data.get('job_url'):
            if not job_data['job_url'].startswith('http') or len(job_data['job_url'].strip()) < 10:
                logger.debug(f"Invalid: Invalid URL")
                return False
        
        logger.debug(f"Valid job data: {job_data['job_title']} at {job_data['company_name']}")
        return True

    def find_job_cards(self, page):
        """Find individual job card elements within the WorkinAUS structure."""
        job_cards = []
        
        # WorkinAUS has a unique structure: all jobs are in one main section,
        # and individual jobs are nested sections with specific patterns
        
        # Strategy 1: Look for individual job sections within the main jobs container
        try:
            # Find the main jobs container first
            jobs_container = page.query_selector('.jobs-listing')
            if jobs_container:
                # Look for individual job sections within this container
                # Each job is a section with classes like "rounded-7 mb-20"
                individual_jobs = jobs_container.query_selector_all('section.rounded-7')
                if individual_jobs:
                    logger.info(f"Found {len(individual_jobs)} individual job sections")
                    return individual_jobs
                
                # Fallback: look for sections with border classes
                individual_jobs = jobs_container.query_selector_all('section[class*="border-"]')
                if individual_jobs:
                    logger.info(f"Found {len(individual_jobs)} job sections with borders")
                    return individual_jobs
                
                # Another fallback: sections with card-shadow
                individual_jobs = jobs_container.query_selector_all('section div[class*="card-shadow"]')
                if individual_jobs:
                    # Get parent sections
                    parent_sections = []
                    for job_div in individual_jobs:
                        try:
                            parent = job_div.evaluate_handle('el => el.closest("section")')
                            if parent and parent not in parent_sections:
                                parent_sections.append(parent.as_element())
                        except:
                            continue
                    if parent_sections:
                        logger.info(f"Found {len(parent_sections)} job sections via card-shadow")
                        return parent_sections
        except Exception as e:
            logger.warning(f"Error in Strategy 1: {e}")
        
        # Strategy 2: Fallback selectors
        selectors_to_try = [
            'section.rounded-7',                  # Rounded sections (job cards)
            'section[class*="border-1"]',         # Sections with border (job cards)
            'section[class*="cursor-pointer"]',   # Clickable sections (job cards)
            '[data-automation="normalJob"]',      # Seek-style selector (fallback)
            '.job-card',                          # Job card container (fallback)
        ]
        
        for selector in selectors_to_try:
            try:
                potential_cards = page.query_selector_all(selector)
                if potential_cards and len(potential_cards) > 2:  # Need at least 3 for valid results
                    # Filter cards that contain job-like content
                    valid_cards = []
                    for card in potential_cards:
                        try:
                            text = card.inner_text()
                            # Check if this looks like a job card
                            if (text and len(text) > 50 and 
                                any(word in text.lower() for word in ['apply', 'full time', 'part time', 'salary']) and
                                not any(word in text.lower() for word in ['searched jobs', 'filter', 'sort'])):
                                valid_cards.append(card)
                        except:
                            continue
                    
                    if len(valid_cards) >= 3:  # Need at least 3 valid cards
                        job_cards = valid_cards
                        logger.info(f"Found {len(job_cards)} job cards using selector: {selector}")
                        break
            except:
                continue
        
        # Strategy 2: Look for job title links as fallback
        if not job_cards:
            try:
                job_links = page.query_selector_all('h2 a, h3 a, .job-title a, a[href*="/job/"]')
                if job_links and len(job_links) > 2:
                    # Get parent containers of job links
                    parent_cards = []
                    for link in job_links:
                        try:
                            # Try to find the parent container
                            parent = link.evaluate_handle('el => el.closest("div, article, section")')
                            if parent and parent not in parent_cards:
                                parent_cards.append(parent.as_element())
                        except:
                            continue
                    
                    if len(parent_cards) >= 3:
                        job_cards = parent_cards
                        logger.info(f"Using job link parents as fallback: found {len(job_cards)}")
            except:
                pass
        
        return job_cards

    def scrape_page(self, page):
        """Scrape jobs from current page with improved element selection."""
        try:
            # Wait for page to load
            page.wait_for_load_state('networkidle', timeout=30000)
            self.human_delay(2, 4)
            
            # Find job cards
            job_cards = self.find_job_cards(page)
            
            if not job_cards:
                logger.warning("No job cards found on this page")
                return 0
            
            logger.info(f"Found {len(job_cards)} job cards on current page")
            
            jobs_processed = 0
            
            for i, job_card in enumerate(job_cards):
                try:
                    # Check job limit EARLY
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                        return -1  # Signal to stop
                    
                    # Scroll job card into view
                    job_card.scroll_into_view_if_needed()
                    self.human_delay(0.5, 1.5)
                    
                    # Extract job data
                    job_data = self.extract_job_data(job_card)
                    
                    if job_data and self.is_valid_job_data(job_data):
                        logger.info(f"Processing valid job {i+1}: {job_data['job_title']} at {job_data['company_name']}")
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            jobs_processed += 1
                            self.scraped_count += 1
                            logger.info(f"Successfully saved job {self.scraped_count}: {job_data['job_title']}")
                        else:
                            logger.error(f"Failed to save job: {job_data['job_title']}")
                    else:
                        logger.debug(f"Skipping invalid job data for card {i+1}")
                        
                except Exception as e:
                    logger.error(f"Error processing job card {i+1}: {e}")
                    self.error_count += 1
                    continue
            
            return jobs_processed
            
        except Exception as e:
            logger.error(f"Error scraping page: {e}")
            return 0

    def save_job_to_database_sync(self, job_data):
        """Save job data to database synchronously."""
        try:
            with transaction.atomic():
                # Check for duplicates by title + company + location combination
                # since WorkinAUS doesn't provide reliable URLs
                existing = JobPosting.objects.filter(
                    title=job_data.get('job_title', ''),
                    company__name=job_data.get('company_name', ''),
                    external_source='workinaus.com.au'
                ).first()
                
                if existing:
                    self.duplicate_count += 1
                    logger.info(f"Duplicate job found: {job_data.get('job_title', 'Unknown')} at {job_data.get('company_name', 'Unknown')}")
                    return existing
                
                # Get or create company
                company = None
                if job_data.get('company_name'):
                    company, _ = Company.objects.get_or_create(
                        name=job_data['company_name'],
                        defaults={
                            'description': f'Company from WorkinAUS scraper',
                            'website': '',
                            'company_size': 'medium'
                        }
                    )
                
                # Get or create location
                location = None
                if job_data.get('location_text'):
                    location_name = job_data['location_text']
                    location, _ = Location.objects.get_or_create(
                        name=location_name,
                        defaults={
                            'country': 'Australia',
                            'state': location_name if any(state in location_name for state in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) else ''
                        }
                    )
                
                # Parse salary if available
                salary_min, salary_max = None, None
                if job_data.get('salary_text'):
                    salary_min, salary_max = self.parse_salary(job_data['salary_text'])
                
                # Generate a unique external URL if none exists
                external_url = job_data.get('job_url', '')
                if not external_url:
                    # Create a unique identifier based on job data
                    import hashlib
                    unique_data = f"{job_data.get('job_title', '')}-{job_data.get('company_name', '')}-{job_data.get('location_text', '')}"
                    url_hash = hashlib.md5(unique_data.encode()).hexdigest()[:8]
                    external_url = f"https://workinaus.com.au/job/generated-{url_hash}"
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data.get('job_title', 'Unknown Position'),
                    company=company,
                    location=location,
                    description=job_data.get('summary', ''),
                    external_url=external_url,
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency='AUD',
                    salary_type='yearly',
                    salary_raw_text=job_data.get('salary_text', ''),
                    job_type=self._map_job_type(job_data.get('job_type_text', '')),
                    job_category='other',  # Will be auto-categorized
                    date_posted=self.parse_date(job_data.get('posted_ago')) or timezone.now(),
                    posted_by=self.system_user,
                    external_source='workinaus.com.au',
                    status='active',
                    posted_ago=job_data.get('posted_ago', ''),
                    additional_info=job_data
                )
                
                return job_posting
                
        except Exception as e:
            logger.error(f"Error saving job to database: {e}")
            return None

    def save_job_to_database(self, job_data):
        """Save job data to database with thread safety."""
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.save_job_to_database_sync, job_data)
                result = future.result(timeout=30)
                return result
        except Exception as e:
            logger.error(f"Error in threaded save: {e}")
            return None

    def _map_job_type(self, job_type_text):
        """Map job type text to model choices."""
        if not job_type_text:
            return 'full_time'
        
        job_type_lower = job_type_text.lower()
        
        if any(word in job_type_lower for word in ['full-time', 'full time', 'permanent']):
            return 'full_time'
        elif any(word in job_type_lower for word in ['part-time', 'part time', 'casual']):
            return 'part_time'
        elif any(word in job_type_lower for word in ['contract', 'temporary', 'temp']):
            return 'contract'
        elif any(word in job_type_lower for word in ['internship', 'graduate']):
            return 'internship'
        elif any(word in job_type_lower for word in ['freelance']):
            return 'freelance'
        else:
            return 'full_time'

    def _search_url_for_page(self, page_no):
        """Generate search URL for a specific page."""
        params = {self.page_param: page_no}
        if self.job_category != "all":
            params['category'] = self.job_category
        if self.location != "all":
            params['location'] = self.location
        
        query_string = urlencode(params)
        url = f"{self.base_url}{self.search_path}?{query_string}"
        return url

    def run(self):
        """Main scraping method with improved pagination and limits."""
        logger.info("Starting WorkinAUS scraper")
        logger.info(f"Job limit: {self.job_limit} | Category: {self.job_category} | Location: {self.location}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            try:
                page_no = 1
                total_processed = 0

                while True:
                    # Check limit before loading new page
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Stopping.")
                        break
                    
                    url = self._search_url_for_page(page_no)
                    logger.info(f"Navigating to page {page_no}: {url}")
                    
                    try:
                        page.goto(url, wait_until='domcontentloaded', timeout=60000)
                        self.human_delay(2.0, 3.5)
                    except Exception as e:
                        logger.error(f"Error navigating to page {page_no}: {e}")
                        break

                    # Scrape current page
                    jobs_on_page = self.scrape_page(page)
                    
                    if jobs_on_page == -1:
                        logger.info("Job limit reached during page processing.")
                        break
                    if jobs_on_page == 0:
                        logger.info(f"No jobs found on page {page_no}. Stopping.")
                        break

                    total_processed += jobs_on_page
                    logger.info(f"Page {page_no} processed: {jobs_on_page} jobs (total: {self.scraped_count})")

                    # Check if we should continue to next page
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info("Reached job limit. Stopping pagination.")
                        break

                    page_no += 1
                    self.human_delay(4.0, 7.0)

                logger.info("=" * 50)
                logger.info("WORKINAUS SCRAPING COMPLETED")
                logger.info(f"Pages visited: {page_no}")
                logger.info(f"Jobs saved: {self.scraped_count}")
                logger.info(f"Duplicates skipped: {self.duplicate_count}")
                logger.info(f"Errors: {self.error_count}")
                logger.info("=" * 50)

            except Exception as e:
                logger.error(f"Fatal error: {e}")
                raise
            finally:
                browser.close()


def main():
    """Main function to run the scraper with command line arguments."""
    print("🔍 Fixed WorkinAUS Job Scraper")
    print("="*50)
    
    # Parse command line arguments
    job_limit = 5  # Default
    job_category = "all"
    location = "all"
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"Job limit from command line: {job_limit}")
        except ValueError:
            print("Invalid job limit. Using default: 5")
    
    if len(sys.argv) > 2:
        job_category = sys.argv[2].lower()
        print(f"Job category from command line: {job_category}")
    
    if len(sys.argv) > 3:
        location = sys.argv[3].lower()
        print(f"Location from command line: {location}")
    
    print(f"Target: {job_limit} jobs")
    print(f"Category: {job_category}")
    print(f"Location: {location}")
    print("Improvements: Better element selection, early limit checking, proper validation")
    print("="*50)

    scraper = WorkinAUSScraperFixed(
        headless=True,
        job_limit=job_limit,
        job_category=job_category,
        location=location
    )
    
    try:
        scraper.run()
        print("Scraper completed successfully!")
    except KeyboardInterrupt:
        print("Interrupted by user")
        logger.info("Interrupted by user")
    except Exception as e:
        print(f"Run failed: {e}")
        logger.error(f"Run failed: {e}")
        raise


if __name__ == "__main__":
    main()
