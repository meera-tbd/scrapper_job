#!/usr/bin/env python
"""
Professional Mumbrella.com.au Jobs Scraper using Playwright

This script scrapes job listings from Mumbrella's job board, focusing on media,
marketing, advertising, and communications roles in Australia.

Features:
- Professional database structure with proper relationships
- Automatic job categorization using AI-like keyword matching
- Human-like behavior to avoid detection
- Complete data extraction and normalization
- Playwright for modern web scraping
- Configurable job limits

Usage:
    python mumbrella_australia_scraper.py [max_jobs]

Example:
    python mumbrella_australia_scraper.py 50
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
import requests
from bs4 import BeautifulSoup

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
        logging.FileHandler('mumbrella_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MumbrellaJobsScraper:
    """
    Professional Mumbrella.com.au jobs scraper using the advanced database structure.
    Focuses on media, marketing, advertising, and communications roles.
    """
    
    def __init__(self, headless=True, job_limit=50):
        """Initialize the Mumbrella jobs scraper."""
        self.headless = headless
        self.base_url = "https://mumbrella.com.au"
        self.start_url = "https://mumbrella.com.au/jobs"
        self.job_limit = job_limit
        
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
        # Browser configuration for stealth
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='mumbrella_scraper_system',
                defaults={
                    'email': 'system@mumbrellascraper.com',
                    'first_name': 'Mumbrella',
                    'last_name': 'Scraper System'
                }
            )
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {e}")
            return None

    def human_delay(self, min_seconds=1, max_seconds=3):
        """Simulate human-like delay."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def setup_browser_context(self, browser):
        """Set up browser context with stealth settings."""
        context = browser.new_context(
            user_agent=random.choice(self.user_agents),
            viewport={'width': random.randint(1200, 1920), 'height': random.randint(800, 1080)},
            locale='en-AU',
            timezone_id='Australia/Sydney',
            extra_http_headers={
                'Accept-Language': 'en-AU,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }
        )
        
        # Add stealth settings to bypass detection
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-AU', 'en'],
            });
            
            window.chrome = {
                runtime: {},
            };
        """)
        
        return context
    
    def try_requests_fallback(self):
        """Try to scrape using requests and BeautifulSoup as fallback."""
        try:
            logger.info("Attempting fallback scraping with requests...")
            
            # Set up session with headers
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-AU,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            # Try to get the page
            response = session.get(self.start_url, timeout=30)
            response.raise_for_status()
            
            # Check if we got a Cloudflare challenge
            if "Just a moment" in response.text or "Verifying you are human" in response.text:
                logger.warning("Requests also hit Cloudflare challenge")
                return []
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            logger.info("Successfully loaded page with requests, parsing jobs...")
            
            jobs = []
            
            # Look for job listings based on the website structure
            # Try multiple selectors to find job containers
            job_containers = (
                soup.find_all('div', string=re.compile(r'FULLTIME|PARTTIME|INTERNSHIP')) +
                soup.find_all('h3') + soup.find_all('h4') +
                soup.find_all('a', href=True) 
            )
            
            current_category = None
            current_job_type = 'full_time'
            
            for container in job_containers:
                try:
                    text = container.get_text(strip=True) if container else ''
                    
                    # Check for job type indicators
                    if text in ['FULLTIME', 'PARTTIME', 'INTERNSHIP', 'CONTRACT']:
                        current_job_type = self.normalize_job_type(text)
                        continue
                    
                    # Check for category headers
                    if text in ['Account management', 'Digital Marketing', 'PR', 'Marketing', 'Sales']:
                        current_category = text
                        continue
                    
                    # Look for actual job links
                    if container.name == 'a' and container.get('href'):
                        href = container.get('href')
                        title = text
                        
                        # Skip navigation links
                        if any(skip in title.lower() for skip in ['post job', 'filter', 'home', 'about', 'news']):
                            continue
                        
                        if len(title) > 5:  # Reasonable job title length
                            # Extract company name from surrounding text
                            parent = container.parent
                            company_name = 'Unknown Company'
                            location = 'Australia'
                            
                            if parent:
                                parent_text = parent.get_text()
                                # Try to extract company name (usually after " – ")
                                company_match = re.search(r'–\s*([^_\n\r]+?)(?:\s*_|$)', parent_text)
                                if company_match:
                                    company_name = self.clean_text(company_match.group(1))
                            
                            job_data = {
                                'title': self.clean_text(title)[:200],
                                'company_name': company_name,
                                'location': location,
                                'url': urljoin(self.base_url, href) if not href.startswith('http') else href,
                                'job_category': current_category or 'marketing',
                                'job_type': current_job_type,
                                'description': f"Job listing from Mumbrella Jobs in {current_category or 'Media/Marketing'} category.",
                                'posted_ago': ''
                            }
                            
                            jobs.append(job_data)
                            logger.info(f"Found job: {title} at {company_name}")
                            
                            if self.job_limit and len(jobs) >= self.job_limit:
                                break
                                
                except Exception as e:
                    logger.warning(f"Error processing container: {e}")
                    continue
            
            logger.info(f"Extracted {len(jobs)} jobs using requests fallback")
            return jobs
            
        except Exception as e:
            logger.error(f"Requests fallback failed: {e}")
            return []

    def wait_for_cloudflare(self, page, timeout=30000):
        """Wait for Cloudflare challenge to complete."""
        try:
            logger.info("Checking for Cloudflare challenge...")
            
            # Check if we're on a Cloudflare challenge page
            page_content = page.content()
            if "Just a moment" in page.title() or "Verifying you are human" in page_content:
                logger.info("Cloudflare challenge detected, waiting for completion...")
                
                # Wait for either the challenge to complete or timeout
                try:
                    # Wait for the title to change away from Cloudflare
                    page.wait_for_function(
                        "document.title !== 'Just a moment...' && !document.title.includes('Verifying')",
                        timeout=timeout
                    )
                    logger.info("Cloudflare challenge completed successfully")
                    self.human_delay(2, 4)  # Extra delay after challenge
                    return True
                    
                except Exception as e:
                    logger.warning(f"Cloudflare challenge timeout or error: {e}")
                    # Try to continue anyway
                    return False
            else:
                logger.info("No Cloudflare challenge detected")
                return True
                
        except Exception as e:
            logger.warning(f"Error checking for Cloudflare: {e}")
            return False
    
    def get_or_create_company(self, company_name):
        """Get or create a company object."""
        if not company_name or company_name.strip() == '':
            company_name = 'Unknown Company'
        
        company_name = company_name.strip()
        
        try:
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'description': f'Company information for {company_name}',
                    'company_size': 'medium'
                }
            )
            if created:
                logger.info(f"Created new company: {company_name}")
            return company
        except Exception as e:
            logger.error(f"Error creating company {company_name}: {e}")
            # Fallback to a default company
            company, _ = Company.objects.get_or_create(
                name='Unknown Company',
                defaults={'description': 'Default company for unknown employers'}
            )
            return company

    def get_or_create_location(self, location_name):
        """Get or create a location object."""
        if not location_name or location_name.strip() == '':
            location_name = 'Australia'
        
        location_name = location_name.strip()
        
        try:
            # Try to parse Australian location patterns
            if ',' in location_name:
                parts = [part.strip() for part in location_name.split(',')]
                if len(parts) >= 2:
                    city = parts[0]
                    state = parts[1] if len(parts) > 1 else ''
                else:
                    city = location_name
                    state = ''
            else:
                city = location_name
                state = ''
                
            location, created = Location.objects.get_or_create(
                name=location_name,
                defaults={
                    'city': city,
                    'state': state,
                    'country': 'Australia'
                }
            )
            if created:
                logger.info(f"Created new location: {location_name}")
            return location
        except Exception as e:
            logger.error(f"Error creating location {location_name}: {e}")
            # Fallback to default location
            location, _ = Location.objects.get_or_create(
                name='Australia',
                defaults={'city': '', 'state': '', 'country': 'Australia'}
            )
            return location

    def normalize_job_type(self, job_type_text):
        """Normalize job type text to match our choices."""
        if not job_type_text:
            return 'full_time'
        
        job_type_lower = job_type_text.lower().strip()
        
        # Map common variations
        type_mappings = {
            'fulltime': 'full_time',
            'full time': 'full_time',
            'full-time': 'full_time',
            'parttime': 'part_time',
            'part time': 'part_time',
            'part-time': 'part_time',
            'contract': 'contract',
            'contractor': 'contract',
            'freelance': 'freelance',
            'temporary': 'temporary',
            'temp': 'temporary',
            'casual': 'casual',
            'internship': 'internship',
            'intern': 'internship',
            'permanent': 'permanent'
        }
        
        return type_mappings.get(job_type_lower, 'full_time')

    def clean_text(self, text):
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove unwanted characters
        text = re.sub(r'[^\w\s\-.,()&]', '', text)
        return text

    def parse_relative_date(self, date_text):
        """Parse relative date strings like '2 days ago' into datetime."""
        if not date_text:
            return None
        
        date_text = date_text.lower().strip()
        now = timezone.now()
        
        try:
            # Handle "X days ago", "X hours ago", etc.
            if 'ago' in date_text:
                if 'just now' in date_text or 'moment' in date_text:
                    return now
                elif 'hour' in date_text:
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
            
            # Handle specific dates if needed
            # Add more date parsing logic here if the site uses specific date formats
            
        except Exception as e:
            logger.warning(f"Error parsing date '{date_text}': {e}")
        
        return None

    def extract_job_listings(self, page):
        """Extract job listings from the current page."""
        try:
            logger.info("Extracting job listings from page...")
            
            # Wait for job listings to load
            page.wait_for_selector('div:has-text("FULLTIME"), div:has-text("INTERNSHIP"), h3, h4', timeout=10000)
            self.human_delay(1, 2)
            
            jobs = []
            
            # Look for job containers - Mumbrella seems to have different sections for each category
            job_sections = page.query_selector_all('h3, h4')
            
            current_category = None
            current_job_type = 'full_time'
            
            for element in job_sections:
                try:
                    text = element.text_content().strip()
                    
                    # Check if this is a category header
                    if text in ['Account management', 'Accounting', 'Agency: Creative', 'Agency: Media', 
                               'Agency: Production', 'Digital', 'Digital Marketing', 'Digital: Content',
                               'Digital: Social Media', 'Executive', 'Marketing', 'PR', 'Production crew',
                               'Publishing', 'Sales']:
                        current_category = text
                        logger.info(f"Found category: {current_category}")
                        continue
                    
                    # Check if this is a job type indicator
                    if text in ['FULLTIME', 'INTERNSHIP', 'PARTTIME', 'CONTRACT']:
                        current_job_type = self.normalize_job_type(text)
                        continue
                    
                    # Look for job listings after category/type headers
                    # Find the next sibling or parent that contains job information
                    parent = element.locator('xpath=./..')
                    
                    # Look for job title patterns (typically links)
                    job_links = parent.query_selector_all('a')
                    
                    for link in job_links:
                        try:
                            link_text = link.text_content().strip()
                            href = link.get_attribute('href')
                            
                            # Skip if this doesn't look like a job title
                            if not link_text or len(link_text) < 5:
                                continue
                            
                            # Skip navigation links
                            if any(skip in link_text.lower() for skip in ['post job', 'filter', 'category', 'home', 'about']):
                                continue
                            
                            # Extract job info
                            job_data = self.extract_job_info_from_element(link, parent, current_category, current_job_type)
                            
                            if job_data and job_data.get('title'):
                                jobs.append(job_data)
                                logger.info(f"Extracted job: {job_data['title']} at {job_data.get('company', 'Unknown')}")
                                
                                # Respect job limit
                                if self.job_limit and len(jobs) >= self.job_limit:
                                    logger.info(f"Reached job limit: {self.job_limit}")
                                    return jobs
                                    
                        except Exception as e:
                            logger.warning(f"Error processing job link: {e}")
                            continue
                
                except Exception as e:
                    logger.warning(f"Error processing element: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(jobs)} job listings")
            return jobs
            
        except Exception as e:
            logger.error(f"Error extracting job listings: {e}")
            return []

    def extract_job_info_from_element(self, link_element, parent_element, category, job_type):
        """Extract job information from a job listing element."""
        try:
            job_data = {}
            
            # Extract title and URL
            title = self.clean_text(link_element.text_content())
            href = link_element.get_attribute('href')
            
            if not title or len(title) < 3:
                return None
            
            job_data['title'] = title[:200]  # Truncate to fit database
            
            # Build full URL
            if href:
                if href.startswith('http'):
                    job_data['url'] = href
                else:
                    job_data['url'] = urljoin(self.base_url, href)
            else:
                # Generate a placeholder URL
                job_data['url'] = f"{self.base_url}/jobs#{slugify(title)}"
            
            # Extract company name and other details from surrounding text
            parent_text = parent_element.text_content()
            
            # Try to extract company name (usually appears after " – " or similar)
            company_match = re.search(r'–\s*([^_\n]+?)(?:\s*_|$)', parent_text)
            if company_match:
                company_name = self.clean_text(company_match.group(1))
            else:
                # Fallback: look for text after the job title
                remaining_text = parent_text.replace(title, '', 1).strip()
                if remaining_text:
                    # Take the first reasonable-looking company name
                    parts = remaining_text.split('_')[0].strip()
                    if parts and len(parts) > 2:
                        company_name = self.clean_text(parts)
                    else:
                        company_name = 'Unknown Company'
                else:
                    company_name = 'Unknown Company'
            
            job_data['company_name'] = company_name
            
            # Extract location if present
            location_match = re.search(r'_Posted\s+\d+/\d+/\d+_\s*([^_\n]+)', parent_text)
            if location_match:
                location = self.clean_text(location_match.group(1))
            else:
                # Look for common Australian location patterns
                location_patterns = [
                    r'\b(Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Darwin|Hobart)\b',
                    r'\b(NSW|VIC|QLD|WA|SA|ACT|NT|TAS)\b',
                    r'\b([A-Z][a-z]+,\s*[A-Z][a-z]+)\b'
                ]
                location = None
                for pattern in location_patterns:
                    match = re.search(pattern, parent_text)
                    if match:
                        location = match.group()
                        break
                
                if not location:
                    location = 'Australia'  # Default location
            
            job_data['location'] = location
            
            # Extract posting date
            date_match = re.search(r'Posted\s+(\d+/\d+/\d+)', parent_text)
            if date_match:
                date_str = date_match.group(1)
                try:
                    # Parse date in DD/MM/YYYY format
                    job_data['posted_ago'] = f"Posted {date_str}"
                    # Could convert to actual datetime if needed
                except:
                    job_data['posted_ago'] = date_str
            else:
                job_data['posted_ago'] = ''
            
            # Set job category and type
            job_data['job_category'] = category or 'marketing'  # Default to marketing for Mumbrella
            job_data['job_type'] = job_type
            
            # Basic job description (will be enhanced when visiting individual pages)
            job_data['description'] = f"Job listing from Mumbrella Jobs in {category or 'Media/Marketing'} category."
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job info: {e}")
            return None

    def get_job_details(self, job_url, page):
        """Get detailed job information from individual job page."""
        try:
            logger.info(f"Fetching job details from: {job_url}")
            
            # Navigate to job detail page
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(1, 2)
            
            job_details = {}
            
            # Try to extract detailed description
            description_selectors = [
                'article',
                '.job-description',
                '.content',
                'main',
                '.post-content',
                'div[role="main"]'
            ]
            
            description = ""
            for selector in description_selectors:
                try:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        description = self.clean_text(desc_element.text_content())
                        if len(description) > 100:  # Found substantial content
                            break
                except:
                    continue
            
            if description:
                job_details['description'] = description[:5000]  # Limit description length
            
            # Extract additional details if available
            # (salary, requirements, etc. - depends on Mumbrella's job detail page structure)
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Error getting job details from {job_url}: {e}")
            return {}

    def save_job(self, job_data):
        """Save a job to the database."""
        try:
            # Skip if no essential data
            if not job_data.get('title') or not job_data.get('company_name'):
                logger.warning("Skipping job - missing title or company")
                return False
            
            # Check for duplicates
            if JobPosting.objects.filter(external_url=job_data['url']).exists():
                logger.info(f"Duplicate job found: {job_data['title']}")
                self.duplicate_count += 1
                return False
            
            # Get or create related objects
            company = self.get_or_create_company(job_data['company_name'])
            location = self.get_or_create_location(job_data.get('location', 'Australia'))
            
            # Determine job category using AI-like categorization
            job_category = JobCategorizationService.categorize_job(
                job_data['title'], 
                job_data.get('description', '')
            )
            
            # Create job posting
            with transaction.atomic():
                job_posting = JobPosting.objects.create(
                    title=job_data['title'],
                    description=job_data.get('description', 'Job description not available.'),
                    company=company,
                    posted_by=self.system_user,
                    location=location,
                    job_category=job_category,
                    job_type=job_data.get('job_type', 'full_time'),
                    external_source='mumbrella.com.au',
                    external_url=job_data['url'],
                    posted_ago=job_data.get('posted_ago', ''),
                    status='active'
                )
                
                logger.info(f"Saved job: {job_posting.title} at {company.name}")
                self.scraped_count += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.error_count += 1
            return False

    def run(self):
        """Main scraping execution."""
        logger.info("Starting Mumbrella Jobs scraper...")
        logger.info(f"Target URL: {self.start_url}")
        logger.info(f"Job limit: {self.job_limit}")
        
        start_time = time.time()
        
        try:
            with sync_playwright() as p:
                # Launch browser
                browser = p.chromium.launch(
                    headless=self.headless,
                    args=[
                        '--no-sandbox',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                
                # Set up stealth context
                context = self.setup_browser_context(browser)
                page = context.new_page()
                
                # Navigate to jobs page
                logger.info("Navigating to Mumbrella Jobs page...")
                page.goto(self.start_url, wait_until='domcontentloaded', timeout=30000)
                
                # Handle Cloudflare challenge
                if not self.wait_for_cloudflare(page, timeout=60000):  # Extended timeout
                    logger.warning("Could not complete Cloudflare challenge automatically.")
                    logger.info("Browser is open - you can manually complete the challenge if needed.")
                    logger.info("Press Enter once you see the jobs page loaded...")
                    input("Waiting for manual intervention (press Enter to continue): ")
                
                self.human_delay(2, 4)
                
                # Extract job listings
                jobs = self.extract_job_listings(page)
                
                # If no jobs found with Playwright, try requests fallback
                if not jobs:
                    logger.warning("No jobs found with Playwright, trying requests fallback...")
                    jobs = self.try_requests_fallback()
                
                if not jobs:
                    logger.warning("No jobs found with either method")
                    return
                
                logger.info(f"Found {len(jobs)} job listings, now processing...")
                
                # Process each job
                for i, job_data in enumerate(jobs):
                    try:
                        if self.job_limit and self.scraped_count >= self.job_limit:
                            logger.info(f"Reached job limit: {self.job_limit}")
                            break
                        
                        logger.info(f"Processing job {i+1}/{len(jobs)}: {job_data['title']}")
                        
                        # Get detailed job information if URL is available
                        if job_data.get('url') and job_data['url'] != self.start_url:
                            try:
                                # Create new page for job details to avoid navigation issues
                                detail_page = context.new_page()
                                job_details = self.get_job_details(job_data['url'], detail_page)
                                job_data.update(job_details)
                                detail_page.close()
                                self.human_delay(1, 2)
                            except Exception as e:
                                logger.warning(f"Could not get details for {job_data['title']}: {e}")
                        
                        # Save job to database
                        self.save_job(job_data)
                        
                        # Human delay between jobs
                        self.human_delay(0.5, 1.5)
                        
                    except Exception as e:
                        logger.error(f"Error processing job {i+1}: {e}")
                        self.error_count += 1
                        continue
                
                # Close browser
                context.close()
                browser.close()
                
        except Exception as e:
            logger.error(f"Critical error in scraper: {e}")
            raise
        
        finally:
            # Print summary
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info("=" * 50)
            logger.info("SCRAPING SUMMARY")
            logger.info("=" * 50)
            logger.info(f"Total runtime: {duration:.2f} seconds")
            logger.info(f"Jobs scraped: {self.scraped_count}")
            logger.info(f"Duplicates skipped: {self.duplicate_count}")
            logger.info(f"Errors encountered: {self.error_count}")
            logger.info(f"Source: {self.start_url}")
            logger.info("=" * 50)


def main():
    """Main function to run the scraper."""
    job_limit = 50  # Default limit
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid job limit provided. Using default of 50.")
    
    scraper = MumbrellaJobsScraper(
        headless=False,  # Set to False for debugging
        job_limit=job_limit
    )
    
    try:
        scraper.run()
    except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
