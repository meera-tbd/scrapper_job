#!/usr/bin/env python
"""
The Creative Store Australia Job Scraper using Playwright

This script scrapes job listings from thecreativestore.com.au/jobs/ using a robust approach
that handles job details and comprehensive data extraction.

Features:
- Professional database structure integration
- Human-like behavior to avoid detection
- Advanced salary and location extraction
- Robust error handling and logging
- Integration with existing Django models

Usage:
    python thecreativestore_australia_scraper.py [max_jobs]

Example:
    python thecreativestore_australia_scraper.py 50
"""

import os
import sys
import re
import time
import random
import uuid
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs
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

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('thecreativestore_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TheCreativeStoreScraper:
    """Professional scraper for The Creative Store job listings."""
    
    def __init__(self, max_jobs=None, headless=False):
        """Initialize the scraper with configuration."""
        self.base_url = "https://thecreativestore.com.au"
        self.jobs_url = "https://thecreativestore.com.au/jobs/"
        self.max_jobs = max_jobs
        self.headless = headless
        self.scraped_count = 0
        self.skipped_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        
        # Initialize database user
        self.user, created = User.objects.get_or_create(
            username='scraper_user',
            defaults={'email': 'scraper@example.com'}
        )
        
        # Initialize company
        self.company, created = Company.objects.get_or_create(
            name='The Creative Store',
            defaults={
                'website': 'https://thecreativestore.com.au',
                'description': 'Creative talent agency and job board',
                'company_size': 'medium'
            }
        )
        
        logger.info(f"Initialized TheCreativeStoreScraper - Max jobs: {max_jobs}")
    
    def extract_salary_info(self, salary_text):
        """Extract salary information from text."""
        if not salary_text:
            return None, None, 'yearly', None
        
        # Clean the text
        salary_text = re.sub(r'\s+', ' ', salary_text.strip())
        
        # Common patterns for Australian salaries
        patterns = [
            # $50,000 - $60,000
            r'\$?([\d,]+)\s*-\s*\$?([\d,]+)',
            # $50,000
            r'\$?([\d,]+)',
            # 50k - 60k
            r'([\d,]+)k\s*-\s*([\d,]+)k',
            # 50k
            r'([\d,]+)k',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, salary_text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    # Range
                    min_sal = re.sub(r'[,$]', '', groups[0])
                    max_sal = re.sub(r'[,$]', '', groups[1])
                    
                    # Handle 'k' suffix
                    if 'k' in salary_text.lower():
                        min_sal = str(int(min_sal) * 1000)
                        max_sal = str(int(max_sal) * 1000)
                    
                    return Decimal(min_sal), Decimal(max_sal), 'yearly', salary_text
                else:
                    # Single value
                    sal = re.sub(r'[,$]', '', groups[0])
                    if 'k' in salary_text.lower():
                        sal = str(int(sal) * 1000)
                    return Decimal(sal), Decimal(sal), 'yearly', salary_text
        
        return None, None, 'yearly', salary_text
    
    def extract_location_info(self, location_text):
        """Extract and get/create location from text."""
        if not location_text:
            return None
        
        # Clean location text
        location_text = location_text.strip()
        
        # Try to get existing location
        try:
            location = Location.objects.get(name__iexact=location_text)
            return location
        except Location.DoesNotExist:
            pass
        
        # Parse Australian location format
        parts = [part.strip() for part in location_text.split(',')]
        
        if len(parts) >= 2:
            city = parts[0]
            state = parts[1]
            
            # Create new location
            location = Location.objects.create(
                name=location_text,
                city=city,
                state=state,
                country='Australia'
            )
            return location
        else:
            # Single location name
            location = Location.objects.create(
                name=location_text,
                country='Australia'
            )
            return location
    
    def extract_job_data_from_card(self, element, index):
        """Extract job data directly from job card element."""
        try:
            job_data = {}
            
            # Extract title
            try:
                title_element = element.locator('.title').first
                job_data['title'] = title_element.inner_text().strip()
            except:
                job_data['title'] = f'Job Position {index + 1}'
            
            # Extract description
            try:
                description_element = element.locator('.description').first
                job_data['description'] = description_element.inner_text().strip()
            except:
                job_data['description'] = 'No description available'
            
            # Extract date and job ID
            try:
                date_element = element.locator('.date').first
                date_text = date_element.inner_text().strip()
                job_data['posted_ago'] = date_text
                
                # Extract job ID if present
                if '#' in date_text:
                    parts = date_text.split('#')
                    if len(parts) > 1:
                        job_data['external_id'] = parts[1].strip()
            except:
                job_data['posted_ago'] = None
                job_data['external_id'] = None
            
            # Extract tags (location, job type, salary)
            try:
                tag_elements = element.locator('.tags li').all()
                tags = []
                location = None
                job_type = None
                salary = None
                
                for tag in tag_elements:
                    tag_text = tag.inner_text().strip()
                    tags.append(tag_text)
                    
                    # Determine what type of tag this is
                    if any(city in tag_text for city in ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Remote', 'Marrickville', 'Surry Hills', 'Cheltenham']):
                        location = tag_text
                    elif any(job_type_word in tag_text.lower() for job_type_word in ['permanent', 'contract', 'part time', 'full time', 'casual', 'freelance']):
                        job_type = tag_text
                    elif '$' in tag_text or 'k' in tag_text.lower():
                        salary = tag_text
                
                job_data['location'] = location
                job_data['job_type'] = job_type  
                job_data['salary'] = salary
                job_data['tags'] = ', '.join(tags)
                
            except Exception as e:
                logger.warning(f"Error extracting tags: {str(e)}")
                job_data['location'] = None
                job_data['job_type'] = None
                job_data['salary'] = None
                job_data['tags'] = ''
            
            # Additional info
            job_data['additional_info'] = {
                'card_index': index,
                'source': 'thecreativestore.com.au',
                'extraction_method': 'job_card'
            }
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data from card: {str(e)}")
            return None
    
    def parse_job_details(self, page, job_url):
        """Parse detailed job information from job detail page."""
        try:
            logger.info(f"Parsing job details from: {job_url}")
            
            # Navigate to job detail page
            page.goto(job_url, wait_until='networkidle')
            time.sleep(random.uniform(1, 3))
            
            # Extract job details - these selectors will need to be updated
            # based on the actual HTML structure you provide
            job_data = {}
            
            # Basic job information
            try:
                job_data['title'] = page.locator('h1').first.inner_text().strip()
            except:
                job_data['title'] = 'Unknown Position'
            
            try:
                job_data['description'] = page.locator('.job-description, .description, .content').first.inner_text().strip()
            except:
                job_data['description'] = 'No description available'
            
            # Location
            try:
                location_text = page.locator('.location, .job-location').first.inner_text().strip()
                job_data['location'] = location_text
            except:
                job_data['location'] = None
            
            # Salary
            try:
                salary_text = page.locator('.salary, .job-salary, .pay').first.inner_text().strip()
                job_data['salary'] = salary_text
            except:
                job_data['salary'] = None
            
            # Job type
            try:
                job_type_text = page.locator('.job-type, .employment-type').first.inner_text().strip()
                job_data['job_type'] = job_type_text
            except:
                job_data['job_type'] = None
            
            # Posted date
            try:
                posted_text = page.locator('.posted, .date-posted, .job-date').first.inner_text().strip()
                job_data['posted_ago'] = posted_text
            except:
                job_data['posted_ago'] = None
            
            # Additional information
            try:
                # Extract any additional job details
                job_data['additional_info'] = {
                    'scraped_from': job_url,
                    'source': 'thecreativestore.com.au'
                }
            except:
                job_data['additional_info'] = {}
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error parsing job details from {job_url}: {str(e)}")
            return None
    
    def save_job_to_database(self, job_data, job_url):
        """Save job data to database using Django models."""
        from concurrent.futures import ThreadPoolExecutor
        import concurrent.futures
        
        def save_in_thread():
            try:
                # Close any existing database connections to avoid async issues
                from django.db import connection
                connection.close()
                
                with transaction.atomic():
                    # Check if job already exists
                    if JobPosting.objects.filter(external_url=job_url).exists():
                        logger.info(f"Job already exists: {job_url}")
                        return 'skipped'
                    
                    # Process location
                    location = None
                    if job_data.get('location'):
                        location = self.extract_location_info(job_data['location'])
                    
                    # Process salary
                    salary_min, salary_max, salary_type, salary_raw = self.extract_salary_info(job_data.get('salary'))
                    
                    # Determine job category (default to 'other' for now)
                    job_category = 'other'  # Could be enhanced with keyword matching
                    
                    # Determine job type
                    job_type_mapping = {
                        'full time': 'full_time',
                        'full-time': 'full_time',
                        'part time': 'part_time',
                        'part-time': 'part_time',
                        'contract': 'contract',
                        'temporary': 'temporary',
                        'permanent': 'permanent',
                        'casual': 'casual',
                        'freelance': 'freelance',
                        'internship': 'internship',
                    }
                    
                    job_type = 'full_time'  # default
                    if job_data.get('job_type'):
                        job_type_text = job_data['job_type'].lower()
                        for key, value in job_type_mapping.items():
                            if key in job_type_text:
                                job_type = value
                                break
                    
                    # Create job posting
                    job_posting = JobPosting.objects.create(
                        title=job_data.get('title', 'Unknown Position'),
                        description=job_data.get('description', 'No description available'),
                        company=self.company,
                        posted_by=self.user,
                        location=location,
                        job_category=job_category,
                        job_type=job_type,
                        salary_min=salary_min,
                        salary_max=salary_max,
                        salary_type=salary_type,
                        salary_raw_text=salary_raw,
                        salary_currency='AUD',
                        external_source='thecreativestore.com.au',
                        external_url=job_url,
                        external_id=job_data.get('external_id', ''),
                        posted_ago=job_data.get('posted_ago', ''),
                        tags=job_data.get('tags', ''),
                        additional_info=job_data.get('additional_info', {}),
                        status='active'
                    )
                    
                    logger.info(f"Saved job: {job_posting.title} ({job_posting.id})")
                    return 'saved'
                    
            except Exception as e:
                logger.error(f"Error saving job to database: {str(e)}")
                return 'error'
        
        # Execute the database operation in a thread
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(save_in_thread)
                result = future.result(timeout=30)  # 30 second timeout
                
                if result == 'saved':
                    self.scraped_count += 1
                    return True
                elif result == 'skipped':
                    self.skipped_count += 1
                    return False
                else:
                    self.error_count += 1
                    return False
                    
        except Exception as e:
            logger.error(f"Error in threaded database operation: {str(e)}")
            self.error_count += 1
            return False
    
    def scrape_jobs_listing(self, page):
        """Scrape job listings from the main jobs page."""
        try:
            logger.info("Scraping jobs from main listing page")
            
            # Navigate to jobs page
            page.goto(self.jobs_url, wait_until='networkidle')
            time.sleep(random.uniform(3, 5))
            
            # Wait for any dynamic content to load
            try:
                page.wait_for_load_state('domcontentloaded')
                time.sleep(2)
            except:
                pass
            
            # First, let's inspect the page structure
            logger.info("Inspecting page structure...")
            
            # Save page content for debugging
            page_content = page.content()
            with open('debug_page_content.html', 'w', encoding='utf-8') as f:
                f.write(page_content)
            logger.info("Saved page content to debug_page_content.html")
            
            # Look for various job-related elements
            job_links = []
            
            # Try different common job listing patterns
            job_selectors = [
                # Common job card selectors
                '.job-listing',
                '.job-item', 
                '.job-card',
                '.job',
                '.position',
                '.listing',
                '.opportunity',
                # Div-based job containers
                'div[class*="job"]',
                'div[class*="position"]',
                'div[class*="listing"]',
                # List items
                'li[class*="job"]',
                'li[class*="position"]',
                # Article elements
                'article',
                # Generic containers that might contain jobs
                '.content .item',
                '.main .item',
                '#jobs .item',
                '#content .item'
            ]
            
            logger.info("Searching for job elements...")
            found_elements = False
            
            for selector in job_selectors:
                try:
                    elements = page.locator(selector).all()
                    if elements:
                        logger.info(f"Found {len(elements)} elements with selector: {selector}")
                        found_elements = True
                        
                        # For The Creative Store, job data is embedded in job cards, not separate pages
                        # Extract job data directly from job cards
                        if selector == '.job-card':
                            logger.info("Extracting job data from job cards...")
                            for i, element in enumerate(elements):
                                try:
                                    job_data = self.extract_job_data_from_card(element, i)
                                    if job_data:
                                        # Create a unique URL for this job
                                        job_url = f"{self.jobs_url}#job-{i+1}"
                                        job_links.append((job_url, job_data))
                                        logger.info(f"Extracted job {i+1}: {job_data.get('title', 'Unknown')}")
                                except Exception as e:
                                    logger.error(f"Error extracting job data from card {i+1}: {str(e)}")
                                    continue
                            break  # We found job cards, no need to check other selectors
                        else:
                            # For other selectors, look for links
                            for element in elements:
                                try:
                                    # Look for links within each element
                                    links = element.locator('a').all()
                                    for link in links:
                                        href = link.get_attribute('href')
                                        if href:
                                            full_url = urljoin(self.base_url, href)
                                            
                                            # Filter for Australian job links
                                            if (
                                                'thecreativestore.com.au' in full_url and
                                                full_url != self.jobs_url and
                                                not any(domain in full_url for domain in ['.co.nz', '.uk', '.com.sg'])
                                            ):
                                                if full_url not in job_links:
                                                    job_links.append(full_url)
                                                    logger.info(f"Found job link: {full_url}")
                                        
                                except Exception as e:
                                    continue
                        
                        if job_links:
                            break  # Found some links, use these
                            
                except Exception as e:
                    continue
            
            # If still no job links, let's check all links on the page more broadly
            if not job_links:
                logger.warning("No job elements found. Checking all page links...")
                all_links = page.locator('a').all()
                
                for link in all_links:
                    try:
                        href = link.get_attribute('href')
                        text = link.inner_text().strip()
                        
                        if href and text:
                            full_url = urljoin(self.base_url, href)
                            
                            # Look for any link that might be a job
                            if (
                                'thecreativestore.com.au' in full_url and
                                (
                                    '/job' in href.lower() or
                                    '/position' in href.lower() or
                                    '/role' in href.lower() or
                                    any(keyword in text.lower() for keyword in ['designer', 'creative', 'marketing', 'art', 'digital', 'brand', 'copywriter', 'producer'])
                                ) and
                                full_url != self.jobs_url
                            ):
                                if full_url not in job_links:
                                    job_links.append(full_url)
                                    logger.info(f"Found potential job link: {full_url} - Text: {text[:50]}")
                    except Exception as e:
                        continue
            
            # If STILL no links, let's see what's actually on the page
            if not job_links:
                logger.warning("Still no job links found. Analyzing page content...")
                
                # Check page title
                title = page.title()
                logger.info(f"Page title: {title}")
                
                # Check for any text that mentions jobs
                page_text = page.inner_text()
                if 'job' in page_text.lower() or 'position' in page_text.lower():
                    logger.info("Page contains job-related text")
                    
                    # Look for any clickable elements with job-related text
                    clickable_elements = page.locator('a, button, div[onclick], [data-href]').all()
                    for element in clickable_elements[:20]:  # Check first 20
                        try:
                            text = element.inner_text().strip()
                            if text and any(keyword in text.lower() for keyword in ['view', 'apply', 'more', 'details', 'read']):
                                href = element.get_attribute('href') or element.get_attribute('data-href')
                                onclick = element.get_attribute('onclick')
                                logger.info(f"Clickable element: Text='{text[:30]}', href='{href}', onclick='{onclick}'")
                        except:
                            continue
                else:
                    logger.warning("No job-related text found on page")
                
                # Show first few links found
                all_links = page.locator('a').all()
                logger.info("First 15 links on page:")
                for i, link in enumerate(all_links[:15]):
                    try:
                        href = link.get_attribute('href')
                        text = link.inner_text().strip()
                        logger.info(f"{i+1}. {href} - '{text[:40]}'")
                    except:
                        continue
            
            logger.info(f"Total job links found: {len(job_links)}")
            return job_links
            
        except Exception as e:
            logger.error(f"Error scraping jobs listing: {str(e)}")
            return []
    
    def run_scraper(self):
        """Main scraper execution method."""
        logger.info("Starting The Creative Store job scraper")
        
        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            
            # Create context with realistic settings
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            
            try:
                # Get job links from main page
                job_links = self.scrape_jobs_listing(page)
                
                if not job_links:
                    logger.warning("No job links found")
                    return
                
                # Limit jobs if specified
                if self.max_jobs:
                    job_links = job_links[:self.max_jobs]
                
                logger.info(f"Processing {len(job_links)} job links")
                
                # Process each job
                for i, job_item in enumerate(job_links, 1):
                    if self.max_jobs and i > self.max_jobs:
                        break
                    
                    # Handle both tuple (job_url, job_data) and string (job_url) formats
                    if isinstance(job_item, tuple):
                        job_url, job_data = job_item
                        logger.info(f"Processing embedded job {i}/{len(job_links)}: {job_data.get('title', 'Unknown')}")
                        
                        # Save job data directly (already extracted from job card)
                        if job_data:
                            self.save_job_to_database(job_data, job_url)
                        else:
                            logger.warning(f"No job data for: {job_url}")
                            self.error_count += 1
                    else:
                        job_url = job_item
                        logger.info(f"Processing job {i}/{len(job_links)}: {job_url}")
                        
                        # Parse job details from separate page
                        job_data = self.parse_job_details(page, job_url)
                        
                        if job_data:
                            # Save to database
                            self.save_job_to_database(job_data, job_url)
                        else:
                            logger.warning(f"Failed to parse job data for: {job_url}")
                            self.error_count += 1
                        
                        # Random delay between requests
                        time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"Error in main scraper loop: {str(e)}")
            
            finally:
                browser.close()
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print scraping summary."""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Source: The Creative Store (thecreativestore.com.au)")
        logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Jobs scraped: {self.scraped_count}")
        logger.info(f"Jobs skipped (duplicates): {self.skipped_count}")
        logger.info(f"Errors: {self.error_count}")
        logger.info(f"Total processed: {self.scraped_count + self.skipped_count + self.error_count}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    max_jobs = None
    headless = True
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid max_jobs argument. Please provide a number.")
            sys.exit(1)
    
    # For development, run with visible browser
    if len(sys.argv) > 2 and sys.argv[2] == '--visible':
        headless = False
    
    # Create and run scraper
    scraper = TheCreativeStoreScraper(max_jobs=max_jobs, headless=headless)
    scraper.run_scraper()


if __name__ == "__main__":
    main()
