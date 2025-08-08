#!/usr/bin/env python
"""
Seek.com.au Job Scraper using Playwright

This script scrapes all job listings from Seek.com.au for Python developer positions.
It mimics human behavior to avoid detection and saves all data to a Django database.

Features:
- Human-like behavior (delays, scrolling, clicking)
- Extracts all available job data
- Handles pagination to scrape all pages
- Avoids duplicates by checking job URLs
- Saves to normalized Django database
- Visible browser for monitoring progress

Usage:
    python scrape_seek.py

Requirements:
    - Django project setup
    - Playwright installed
    - Models migrated
"""

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import logging

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'seek_scraper.settings')

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connection
from playwright.sync_api import sync_playwright
from jobs.models import JobLocation, Salary, SeekJob
import threading
import concurrent.futures

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SeekScraper:
    """
    Main scraper class for Seek.com.au job listings.
    Handles browser automation, data extraction, and database operations.
    """
    
    def __init__(self, headless=False, job_category="all", job_limit=None):
        """Initialize the scraper with browser settings."""
        self.headless = headless
        self.base_url = "https://www.seek.com.au"
        self.job_limit = job_limit  # Maximum number of jobs to scrape
        
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
        
    def human_delay(self, min_seconds=1, max_seconds=3):
        """
        Add human-like delay between actions.
        
        Args:
            min_seconds: Minimum delay time
            max_seconds: Maximum delay time
        """
        delay = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def parse_date(self, date_string):
        """
        Parse relative date strings like '2 days ago' into datetime objects.
        
        Args:
            date_string: String like '2 days ago', '1 week ago', etc.
            
        Returns:
            datetime object or None if parsing fails
        """
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
        """
        Parse location string into city, state, country components.
        
        Args:
            location_string: String like "Sydney NSW", "Melbourne VIC", etc.
            
        Returns:
            tuple: (city, state, country)
        """
        if not location_string:
            return ("", "", "Australia")
            
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
        
        return (city, state, country)
    
    def parse_salary(self, salary_text):
        """
        Parse salary information from job postings.
        
        Args:
            salary_text: Raw salary text from job posting
            
        Returns:
            dict: Parsed salary information
        """
        if not salary_text:
            return None
            
        salary_text = salary_text.strip()
        
        # Common patterns for salary extraction
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80-100k"
            r'(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80k"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, salary_text.lower().replace(',', ''))
            if match:
                groups = match.groups()
                if len(groups) == 3:  # Range with period
                    min_amount = float(groups[0].replace(',', ''))
                    max_amount = float(groups[1].replace(',', ''))
                    period = groups[2]
                    return {
                        'min_amount': min_amount,
                        'max_amount': max_amount,
                        'period': period,
                        'currency': 'AUD',
                        'raw_text': salary_text
                    }
                elif len(groups) == 2 and 'k' in salary_text.lower():  # Range in thousands
                    min_amount = float(groups[0].replace(',', '')) * 1000
                    max_amount = float(groups[1].replace(',', '')) * 1000
                    return {
                        'min_amount': min_amount,
                        'max_amount': max_amount,
                        'period': 'year',
                        'currency': 'AUD',
                        'raw_text': salary_text
                    }
                elif len(groups) == 2:  # Single amount with period
                    amount = float(groups[0].replace(',', ''))
                    period = groups[1]
                    return {
                        'min_amount': amount,
                        'max_amount': None,
                        'period': period,
                        'currency': 'AUD',
                        'raw_text': salary_text
                    }
                elif len(groups) == 1 and 'k' in salary_text.lower():  # Single amount in thousands
                    amount = float(groups[0].replace(',', '')) * 1000
                    return {
                        'min_amount': amount,
                        'max_amount': None,
                        'period': 'year',
                        'currency': 'AUD',
                        'raw_text': salary_text
                    }
        
        # If no pattern matches, return raw text
        return {
            'min_amount': None,
            'max_amount': None,
            'period': 'year',
            'currency': 'AUD',
            'raw_text': salary_text
        }
    
    def extract_job_data(self, job_element, page):
        """
        Extract all available data from a job card element.
        
        Args:
            job_element: Playwright element for the job card
            page: Playwright page object
            
        Returns:
            dict: Extracted job data
        """
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
            
            # Try to extract additional tags or keywords
            try:
                all_text = job_element.inner_text()
                # Extract common job-related keywords
                keywords = []
                common_terms = ['remote', 'hybrid', 'full-time', 'part-time', 'contract', 'permanent', 
                               'senior', 'junior', 'mid-level', 'graduate', 'internship']
                for term in common_terms:
                    if term.lower() in all_text.lower():
                        keywords.append(term)
                job_data['keywords'] = keywords
            except:
                job_data['keywords'] = []
            
            logger.debug(f"Extracted job data: {job_data['job_title']} at {job_data['company_name']}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {str(e)}")
            return None
    
    def save_job_to_db_sync(self, job_data):
        """
        Synchronous database save function to be called from thread.
        """
        try:
            # Close any existing connections to ensure fresh connection
            from django.db import connections
            connections.close_all()
            
            with transaction.atomic():
                # Check if job already exists
                if SeekJob.objects.filter(job_url=job_data['job_url']).exists():
                    logger.debug(f"Duplicate job skipped: {job_data['job_url']}")
                    self.duplicate_count += 1
                    return False
                
                # Parse and get or create location
                city, state, country = self.parse_location(job_data.get('location_text', ''))
                location, created = JobLocation.objects.get_or_create(
                    city=city,
                    state=state,
                    country=country
                )
                
                # Parse and create salary if available
                salary_obj = None
                if job_data.get('salary_text'):
                    salary_info = self.parse_salary(job_data['salary_text'])
                    if salary_info:
                        salary_obj = Salary.objects.create(**salary_info)
                
                # Parse date
                date_posted = self.parse_date(job_data.get('posted_ago', ''))
                
                # Determine job type and work mode from badges
                job_type = ""
                work_mode = ""
                experience_level = ""
                
                badges = job_data.get('badges', []) + job_data.get('keywords', [])
                for badge in badges:
                    badge_lower = badge.lower()
                    if badge_lower in ['full-time', 'part-time', 'contract', 'permanent', 'temporary']:
                        job_type = badge
                    elif badge_lower in ['remote', 'hybrid', 'on-site', 'work from home']:
                        work_mode = badge
                    elif badge_lower in ['senior', 'junior', 'mid-level', 'graduate', 'entry level']:
                        experience_level = badge
                
                # Combine badges and keywords as tags
                all_tags = list(set(badges))  # Remove duplicates
                tags_string = ', '.join(all_tags)
                
                # Create the SeekJob object
                seek_job = SeekJob.objects.create(
                    job_title=job_data.get('job_title', ''),
                    company_name=job_data.get('company_name', ''),
                    location=location,
                    job_type=job_type,
                    work_mode=work_mode,
                    summary=job_data.get('summary', ''),
                    experience_level=experience_level,
                    salary=salary_obj,
                    tags=tags_string,
                    posted_ago=job_data.get('posted_ago', ''),
                    date_posted=date_posted,
                    job_url=job_data.get('job_url', ''),
                    source='seek.com.au',
                    additional_info=job_data  # Store all extracted data
                )
                
                logger.info(f"Saved job: {seek_job.job_title} at {seek_job.company_name}")
                self.scraped_count += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job to database: {str(e)}")
            self.error_count += 1
            return False
    
    def save_job_to_db(self, job_data):
        """
        Save extracted job data to Django database using thread-safe approach.
        
        Args:
            job_data: Dictionary containing job information
            
        Returns:
            bool: True if saved successfully, False if duplicate or error
        """
        # Use ThreadPoolExecutor to run database operations in a separate thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.save_job_to_db_sync, job_data)
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
        """
        Scrape all job listings from the current page.
        
        Args:
            page: Playwright page object
            
        Returns:
            int: Number of jobs found on this page
        """
        # Wait for job listings to load
        try:
            page.wait_for_selector('[data-automation="normalJob"]', timeout=10000)
        except:
            logger.warning("No job listings found on page")
            return 0
        
        # Scroll down to load all jobs (some sites use lazy loading)
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
                    self.save_job_to_db(job_data)
                else:
                    logger.warning(f"Failed to extract data for job {i+1}")
                    
            except Exception as e:
                logger.error(f"Error processing job {i+1}: {str(e)}")
                self.error_count += 1
                continue
        
        return len(job_elements)
    
    def has_next_page(self, page):
        """
        Check if there's a next page available.
        
        Args:
            page: Playwright page object
            
        Returns:
            bool: True if next page exists, False otherwise
        """
        try:
            # Look for next page button or link
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
        """
        Navigate to the next page of results.
        
        Args:
            page: Playwright page object
            
        Returns:
            bool: True if successfully navigated, False otherwise
        """
        try:
            # Try different selectors for next page
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
        """
        Main method to run the complete scraping process.
        """
        logger.info("Starting Seek.com.au job scraper...")
        logger.info(f"Target URL: {self.start_url}")
        
        with sync_playwright() as p:
            # Launch browser (visible for monitoring)
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            # Create new page with realistic user agent
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
            
            try:
                # Navigate to starting URL with longer timeout and retry logic
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
                logger.info("SCRAPING COMPLETED!")
                logger.info(f"Total pages scraped: {page_number}")
                logger.info(f"Total jobs found: {total_jobs_found}")
                logger.info(f"Jobs saved to database: {self.scraped_count}")
                logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
                logger.info(f"Errors encountered: {self.error_count}")
                logger.info("="*50)
                
            except Exception as e:
                logger.error(f"Fatal error during scraping: {str(e)}")
                raise
            finally:
                browser.close()


def main():
    """Main function to run the scraper."""
    print("🔍 Seek.com.au Job Scraper - FIRST 30 JOBS")
    print("="*50)
    
    # Create scraper instance for ALL jobs with limit of 30
    scraper = SeekScraper(headless=False, job_category="all", job_limit=30)  # Set to True for headless mode
    
    try:
        # Run the scraping process
        scraper.run()
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()