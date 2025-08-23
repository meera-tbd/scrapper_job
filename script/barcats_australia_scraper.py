#!/usr/bin/env python3
"""
Professional Barcats Australia Hospitality Jobs Scraper
=======================================================

Advanced Playwright-based scraper for Barcats Australia (https://www.barcats.com.au/staff/hospitality-jobs/) 
that integrates with the existing australia_job_scraper database structure.

Barcats specializes in hospitality jobs across Australia including:
- Restaurant positions (waitstaff, bartenders, chefs)
- Hotel and accommodation roles
- Café and coffee shop positions
- Event and catering services
- Kitchen and food service jobs

Features:
- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian hospitality job optimization
- Support for both over 18 and under 18 job sections

Usage:
    python barcats_australia_scraper.py [job_limit]
    
Examples:
    python barcats_australia_scraper.py 30    # Scrape 30 hospitality jobs
    python barcats_australia_scraper.py       # Scrape all available jobs
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


class BarcatsAustraliaJobScraper:
    """Professional Barcats Australia hospitality job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://www.barcats.com.au"
        self.search_url = "https://www.barcats.com.au/staff/hospitality-jobs/"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        self.pages_scraped = 0
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # Barcats specific job type mapping
        self.job_type_mapping = {
            'full time': 'full_time',
            'part time': 'part_time',
            'casual': 'part_time',
            'part time / casual': 'part_time',
            'contract': 'contract',
            'temporary': 'temporary',
            '1 shift': 'temporary',
            'rockstar': 'part_time',  # Barcats specific category
            'rockstar / 1 shift': 'temporary'
        }
        
        # Common hospitality position categories for better categorization
        self.hospitality_categories = {
            'chef': ['chef', 'cook', 'kitchen', 'culinary', 'food preparation'],
            'service': ['waiter', 'waitress', 'waitstaff', 'server', 'front of house', 'foh'],
            'bar': ['bartender', 'barista', 'bar', 'coffee', 'mixologist'],
            'management': ['manager', 'supervisor', 'coordinator', 'head', 'senior'],
            'housekeeping': ['housekeeper', 'cleaner', 'dishwasher', 'dish hand'],
            'reception': ['receptionist', 'front desk', 'concierge', 'host', 'hostess']
        }

    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,  # Back to normal logging level
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('barcats_australia_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_or_create_bot_user(self):
        """Get or create a bot user for job postings."""
        try:
            user, created = User.objects.get_or_create(
                username='barcats_scraper_bot',
                defaults={
                    'email': 'bot@barcats-scraper.com',
                    'first_name': 'Barcats',
                    'last_name': 'Scraper Bot',
                    'is_staff': True,
                    'is_active': True,
                }
            )
            
            if created:
                self.logger.info("Created new bot user: barcats_scraper_bot")
            else:
                self.logger.info("Using existing bot user: barcats_scraper_bot")
            
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return None

    def human_delay(self, min_seconds=1.0, max_seconds=3.0):
        """Add a human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def initialize_browser(self):
        """Initialize Playwright browser with proper configuration."""
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=True,  # Set to True for better performance during pagination testing
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            )
            
            self.page = self.context.new_page()
            
            # Set longer timeout for slower connections
            self.page.set_default_timeout(60000)
            
            self.logger.info("Browser initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize browser: {e}")
            return False

    def navigate_to_jobs_page(self, page_num=1):
        """Navigate to the Barcats jobs page."""
        try:
            if page_num == 1:
                # Always use page=1 explicitly to ensure consistent behavior
                url = f"{self.search_url}?page=1"
            else:
                url = f"{self.search_url}?page={page_num}"
            
            self.logger.info(f"Navigating to page {page_num}: {url}")
            
            response = self.page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            if response.status != 200:
                self.logger.error(f"Failed to load page {page_num}. Status: {response.status}")
                return False
            
            # Wait for job listings to load
            self.page.wait_for_selector('.job-listing, .job-card, .job-item, [class*="job"]', timeout=15000)
            
            # Handle age verification popup if it appears
            self.handle_age_verification()
            
            self.human_delay(2, 4)
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to jobs page {page_num}: {e}")
            return False

    def handle_age_verification(self):
        """Handle age verification popup if it appears."""
        try:
            # Look for "I am over 18" button
            over_18_button = self.page.locator('text="I am over 18"').first
            if over_18_button.is_visible(timeout=3000):
                over_18_button.click()
                self.logger.info("Clicked 'I am over 18' button")
                self.human_delay(1, 2)
        except Exception as e:
            # Age verification may not always appear
            self.logger.debug(f"No age verification popup found: {e}")

    def extract_jobs_from_page(self):
        """Extract job listings from the current page."""
        jobs = []
        
        try:
            # Wait for job listings to be present (try multiple selectors)
            wait_selectors = [
                '.job-listing-block',
                '[class*="job-listing"]', 
                '.job-card',
                '[class*="job"]'
            ]
            
            for selector in wait_selectors:
                try:
                    self.page.wait_for_selector(selector, timeout=5000)
                    self.logger.debug(f"Page loaded - found elements with selector: {selector}")
                    break
                except:
                    continue
            else:
                self.logger.warning("Could not find job elements with any selector")
                
            # Additional wait for dynamic content
            self.human_delay(2, 3)
            
            # Use flexible selectors that work on all pages
            job_selectors = [
                '.job-listing-block',  # Main container from provided HTML
                'div.job-listing-block',
                '.job-listing-block.white-block',  # More specific fallback
                '[class*="job-listing"]'  # Broader fallback
            ]
            
            job_elements = None
            for selector in job_selectors:
                job_elements = self.page.locator(selector).all()
                if job_elements:
                    self.logger.info(f"Found {len(job_elements)} jobs using selector: {selector}")
                    break
            
            if not job_elements:
                # Fallback: look for any element that might contain job info
                job_elements = self.page.locator('div:has-text("Full time"), div:has-text("Part time"), div:has-text("Casual")').all()
                self.logger.info(f"Fallback selector found {len(job_elements)} potential jobs")
            
            for element in job_elements:
                try:
                    job_data = self.extract_job_from_element(element)
                    if job_data:
                        jobs.append(job_data)
                        self.jobs_scraped += 1
                        
                        if self.job_limit and self.jobs_scraped >= self.job_limit:
                            self.logger.info(f"Reached job limit of {self.job_limit}")
                            break
                            
                except Exception as e:
                    self.logger.error(f"Error extracting job from element: {e}")
                    self.errors_count += 1
                    continue
            
            self.logger.info(f"Extracted {len(jobs)} jobs from current page")
            
        except Exception as e:
            self.logger.error(f"Error extracting jobs from page: {e}")
        
        return jobs

    def extract_job_from_element(self, element) -> Optional[ScrapedJob]:
        """Extract job information from a job element."""
        try:
            # First, get the element's HTML to understand its structure better
            element_html = element.inner_html()[:500] if element else ""
            self.logger.debug(f"Processing element HTML snippet: {element_html[:200]}...")
            
            # Extract using exact selectors from the provided HTML structure
            title = self.extract_text_by_selectors(element, [
                'h3.name a.job-title-link',                 # Exact from HTML: <h3 class="name"><a href="..." class="job-title-link">
                'h3.name a',                                # Fallback without class
                '.job-title-link',                          # Just the link class
                'h3.name',                                  # Just the h3 name
                'h3', 'h2'                                  # Generic headings
            ])
            
            # Company name will be extracted from job detail page
            company_name = "Unknown Company"  # Default, will be updated from job detail page
            
            # Extract location from the specific location block with more comprehensive selectors
            location = self.extract_text_by_selectors(element, [
                '.lhd-details-block.color-red .text',       # Exact from HTML: <div class="lhd-details-block color-red"><span class="text">Coolangatta, QLD</span>
                '.color-red .text',                         # Fallback
                '.type-outline-red',                        # From entry-meta section
                'p.type-outline-red',                       # More specific
                '.fa-map-marker + .text',                   # Location icon + text
                '.fa-map-marker ~ .text',                   # Location icon sibling text
                'i.fa-map-marker + span',                   # Icon followed by span
                'i.fa-map-marker ~ span',                   # Icon sibling span
                '[class*="location"]',                      # Any location class
                'span:has-text("NSW")', 'span:has-text("VIC")', 'span:has-text("QLD")',  # Spans containing states
                'span:has-text("WA")', 'span:has-text("SA")', 'span:has-text("TAS")',
                'span:has-text("NT")', 'span:has-text("ACT")'
            ])
            
            # If still no location, try extracting from the whole element text
            if not location:
                element_text = element.inner_text()
                # Look for Australian state patterns in the text
                import re
                state_pattern = r'\b([A-Za-z\s,]+(?:NSW|VIC|QLD|WA|SA|TAS|NT|ACT))\b'
                match = re.search(state_pattern, element_text)
                if match:
                    location = match.group(1).strip()
            
            # Extract job type from the specific job type block  
            job_type = self.extract_text_by_selectors(element, [
                '.lhd-details-block.color-blue .text',      # Exact from HTML: <div class="lhd-details-block color-blue"><span class="text">Part time / casual</span>
                '.color-blue .text',                        # Fallback
                '.type-outline-blue',                       # From entry-meta section
                'p.type-outline-blue'                       # More specific
            ])
            
            # Extract salary from the specific salary block
            salary = self.extract_text_by_selectors(element, [
                '.lhd-details-block.color-green .text',     # Exact from HTML: <div class="lhd-details-block color-green"><span class="text">On application</span>
                '.color-green .text',                       # Fallback  
                '.type-outline-green',                      # From entry-meta section
                'p.type-outline-green'                      # More specific
            ])
            
            # Posted date - not clearly shown in this HTML structure
            posted_ago = "Recently posted"  # Default since not visible in provided HTML
            
            # Extract job URL using specific selectors from HTML
            job_url = ""
            try:
                url_selectors = [
                    'h3.name a.job-title-link',            # Primary job title link
                    '.job-title-link',                     # Job title link class
                    'a.stretched-link',                    # Stretched link at bottom
                    'a[href*="/job/"]',                    # Any link containing /job/
                    '.btn[href]'                           # Read More button
                ]
                
                for selector in url_selectors:
                    link = element.locator(selector).first
                    if link.is_visible(timeout=1000):
                        href = link.get_attribute('href')
                        if href and href.strip():
                            job_url = urljoin(self.base_url, href.strip())
                            break
            except Exception as e:
                self.logger.debug(f"Could not extract URL: {e}")
            
            # Get description from specific description area
            description = self.extract_text_by_selectors(element, [
                '.profile-excerpt .intro',                 # Main intro paragraph
                'p.intro',                                 # Intro paragraph
                '.job-formatted-text',                     # Formatted job text
                '.review-text',                           # Review text section
                '.profile-excerpt p'                      # Any paragraph in profile excerpt
            ])
            
            # Note: Company name will be extracted from job detail page, not from title
            
            # Fallback: try to extract any meaningful text if primary extraction failed
            if not title:
                # Get all text and try to identify the title as the first meaningful text
                all_text = element.inner_text()
                if all_text:
                    lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                    if lines:
                        title = lines[0]
                        if len(lines) > 1:
                            company_name = company_name or lines[1]
                        if len(lines) > 2:
                            location = location or lines[2]
            
            # Clean and validate data
            title = self.clean_text(title)
            company_name = self.clean_text(company_name) or "Unknown Company"
            location = self.clean_text(location) or "Australia"
            job_type = self.clean_text(job_type) or "Not specified"
            salary = self.clean_text(salary) or "Not specified"
            description = self.clean_text(description) or "Job description not available."
            posted_ago = self.clean_text(posted_ago) or "Recently posted"
            
            # Skip if no meaningful title found
            if not title or len(title) < 3:
                self.logger.debug("Skipping job - no valid title found")
                return None
            
            # Filter out navigation/UI elements that aren't actual jobs (be more selective)
            invalid_titles = ['change site', 'find staff', 'find jobs', 'login', 'signup', 'search by location', 'filter by job type', 'search options']
            title_lower = title.lower()
            
            # Only filter obvious UI elements, not actual job titles
            if (title_lower in invalid_titles or 
                title_lower.startswith('filter by') or 
                title_lower.startswith('search by') or
                'filter options' in title_lower or
                len(title_lower) < 3):
                self.logger.debug(f"Skipping non-job element: {title}")
                return None
            
            # Create URL if none found
            if not job_url:
                job_url = f"{self.search_url}#{slugify(title)}"
            
            # Try to get detailed information from the job detail page
            detailed_description = description  # Default to summary description
            if job_url and job_url != f"{self.search_url}#{slugify(title)}":
                try:
                    self.logger.debug(f"Attempting to get detailed info for: {title}")
                    detailed_info = self.get_detailed_job_info(job_url)
                    if detailed_info:
                        if detailed_info.get('description'):
                            detailed_description = detailed_info['description']
                            self.logger.info(f"Retrieved detailed description for: {title}")
                        
                        # Update company name from detailed page if found
                        if detailed_info.get('company_name'):
                            company_name = detailed_info['company_name']
                    else:
                        self.logger.debug(f"No detailed info found for: {title}")
                except Exception as e:
                    self.logger.debug(f"Error getting detailed info for {title}: {e}")
            
            job_data = ScrapedJob(
                title=title,
                company_name=company_name,
                location_text=location,
                job_type=job_type,
                salary_text=salary,
                description=detailed_description,  # Use detailed description
                posted_ago=posted_ago,
                job_url=job_url
            )
            
            self.logger.debug(f"Extracted job: {title} at {company_name}")
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job from element: {e}")
            return None

    def extract_text_by_selectors(self, element, selectors):
        """Try multiple selectors to extract text, similar to other scrapers."""
        for selector in selectors:
            try:
                target = element.locator(selector).first
                if target.is_visible(timeout=1000):
                    text = target.inner_text()
                    if text and text.strip():
                        return text.strip()
            except Exception as e:
                self.logger.debug(f"Selector '{selector}' failed: {e}")
                continue
        return ""

    def safe_extract_text(self, element, selector):
        """Safely extract text from an element using a selector."""
        try:
            target = element.locator(selector).first
            if target.is_visible(timeout=1000):
                return target.inner_text().strip()
        except:
            pass
        return ""

    def clean_text(self, text):
        """Clean and normalize text."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove special characters that might cause issues
        text = re.sub(r'[^\w\s\-.,!?$()&]', '', text)
        return text

    def get_detailed_job_info(self, job_url):
        """Get detailed job information from individual job page."""
        try:
            self.logger.debug(f"Getting detailed info from: {job_url}")
            
            # Create a new page for job details to avoid navigation issues
            detail_page = self.context.new_page()
            detail_page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait a moment for content to load
            self.human_delay(1, 2)
            
            # Extract job description using the specific HTML structure provided
            description_parts = []
            
            # Extract main job description
            job_description_selectors = [
                'h4:has-text("JOB DESCRIPTION") + .job-formatted-text',  # Exact structure from HTML
                'h4:has-text("JOB DESCRIPTION") ~ .job-formatted-text',  # Sibling selector
                '.job-formatted-text',                                  # Any job formatted text
                '[class*="job-description"]',                          # Any job description class
                '.job-description'                                     # Standard job description
            ]
            
            job_description = ""
            for selector in job_description_selectors:
                try:
                    element = detail_page.locator(selector).first
                    if element.is_visible(timeout=3000):
                        job_description = element.inner_text().strip()
                        if len(job_description) > 50:  # Good description found
                            break
                except:
                    continue
            
            # Extract requirements section
            requirements_selectors = [
                'h4:has-text("REQUIREMENTS") + .job-formatted-text',    # Exact structure from HTML
                'h4:has-text("REQUIREMENTS") ~ .job-formatted-text',    # Sibling selector
                'h4:has-text("Requirements") + .job-formatted-text',    # Case variation
                'h4:has-text("Requirements") ~ .job-formatted-text',    # Case variation sibling
            ]
            
            requirements = ""
            for selector in requirements_selectors:
                try:
                    element = detail_page.locator(selector).first
                    if element.is_visible(timeout=3000):
                        requirements = element.inner_text().strip()
                        if len(requirements) > 20:  # Good requirements found
                            break
                except:
                    continue
            
            # Combine description and requirements into full description
            if job_description:
                description_parts.append("JOB DESCRIPTION:\n" + job_description)
            
            if requirements:
                description_parts.append("REQUIREMENTS:\n" + requirements)
            
            # Try to get any other job-related content as fallback
            if not description_parts:
                fallback_selectors = [
                    '.job-content',
                    '.job-details',
                    '.content',
                    'main .job-formatted-text',
                    '[class*="description"]'
                ]
                
                for selector in fallback_selectors:
                    try:
                        element = detail_page.locator(selector).first
                        if element.is_visible(timeout=2000):
                            fallback_description = element.inner_text().strip()
                            if len(fallback_description) > 100:
                                description_parts.append(fallback_description)
                                break
                    except:
                        continue
            
            # Extract company name from venue details section (before closing page)
            company_name = ""
            
            # Wait for the page content to load properly
            self.human_delay(1, 2)
            
            company_selectors = [
                '.venue-details h3',           # Exact from HTML: <div class="venue-details"><h3>Black Dingo Cafe</h3>
                'div.venue-details h3',        # More specific
                '.venue-details > h3',         # Direct child
                '[class*="venue"] h3',         # Any venue class with h3
                'h3:first-of-type',            # First h3 on the page (often company)
                'h3'                           # Any h3 as fallback
            ]
            
            for selector in company_selectors:
                try:
                    element = detail_page.locator(selector).first
                    if element.is_visible(timeout=3000):
                        company_name = element.inner_text().strip()
                        if len(company_name) > 1 and len(company_name) < 100:  # Valid company name found
                            self.logger.debug(f"Found company name '{company_name}' using selector '{selector}'")
                            break
                except Exception as e:
                    self.logger.debug(f"Selector '{selector}' failed: {e}")
                    continue
            
            # Debug: Try to find all h3 elements to see what's available
            if not company_name:
                try:
                    all_h3s = detail_page.locator('h3').all()
                    self.logger.debug(f"Found {len(all_h3s)} h3 elements on page")
                    for i, h3 in enumerate(all_h3s[:3]):  # Check first 3 h3s
                        text = h3.inner_text().strip() if h3.is_visible() else "not visible"
                        self.logger.debug(f"H3 {i+1}: '{text}'")
                        if not company_name and len(text) > 1 and len(text) < 100:
                            company_name = text
                            self.logger.debug(f"Using first valid h3 as company: '{company_name}'")
                            break
                except Exception as e:
                    self.logger.debug(f"Error finding h3 elements: {e}")
            
            # Close the detail page after extraction
            detail_page.close()
            
            # Return the combined description and company info
            full_description = "\n\n".join(description_parts) if description_parts else ""
            
            if full_description or company_name:
                return {
                    'description': full_description,
                    'requirements': requirements,
                    'company_name': company_name
                }
            else:
                return None
            
        except Exception as e:
            self.logger.debug(f"Could not get detailed info from {job_url}: {e}")
            return None

    def has_next_page(self):
        """Check if there's a next page available using Barcats pagination structure."""
        try:
            # Look for the specific pagination structure from Barcats
            # <ul class="archive-pagination">
            pagination_container = self.page.locator('ul.archive-pagination').first
            
            if not pagination_container.is_visible(timeout=3000):
                self.logger.debug("No pagination container found")
                return False
            
            # Look for next page link with specific structure
            # <li><a href="?page=3" aria-label="Next"><span class="next"></span></a></li>
            next_selectors = [
                'ul.archive-pagination li a[aria-label="Next"]',    # Exact structure from HTML
                'ul.archive-pagination a:has(.next)',              # Link containing next span
                'ul.archive-pagination .next',                     # Just the next class
                '.archive-pagination a[aria-label="Next"]'         # Fallback without ul
            ]
            
            for selector in next_selectors:
                next_element = self.page.locator(selector).first
                if next_element.is_visible(timeout=2000):
                    href = next_element.get_attribute('href')
                    if href and 'page=' in href:
                        self.logger.debug(f"Found next page link: {href}")
                        return True
            
            # Alternative: Check if we can find numbered pagination links for higher page numbers
            current_page_num = self.pages_scraped + 1
            page_links = pagination_container.locator('li a').all()
            
            for page_link in page_links:
                try:
                    href = page_link.get_attribute('href')
                    if href and 'page=' in href:
                        # Extract page number from URL like ?page=3
                        page_num_match = re.search(r'page=(\d+)', href)
                        if page_num_match:
                            page_num = int(page_num_match.group(1))
                            if page_num > current_page_num:
                                self.logger.debug(f"Found higher page number: {page_num}")
                                return True
                except Exception as e:
                    self.logger.debug(f"Error checking page link: {e}")
                    continue
            
            self.logger.debug("No next page found")
            return False
            
        except Exception as e:
            self.logger.debug(f"Error checking for next page: {e}")
            return False

    def go_to_next_page(self):
        """Navigate to the next page using Barcats pagination."""
        try:
            current_page = self.pages_scraped + 1
            next_page = current_page + 1
            
            self.logger.info(f"Attempting to navigate from page {current_page} to page {next_page}")
            
            # First try clicking the "Next" button if available
            next_selectors = [
                'ul.archive-pagination li a[aria-label="Next"]',    # Exact next button
                'ul.archive-pagination a:has(.next)',              # Link with next span
                '.archive-pagination a[aria-label="Next"]'         # Fallback
            ]
            
            for selector in next_selectors:
                next_element = self.page.locator(selector).first
                if next_element.is_visible(timeout=2000):
                    href = next_element.get_attribute('href')
                    self.logger.debug(f"Clicking next button with href: {href}")
                    next_element.click()
                    self.page.wait_for_load_state('domcontentloaded', timeout=30000)
                    self.human_delay(2, 4)
                    return True
            
            # Alternative: Try clicking on the specific page number
            page_number_selector = f'ul.archive-pagination li a[href*="page={next_page}"]'
            page_link = self.page.locator(page_number_selector).first
            if page_link.is_visible(timeout=2000):
                self.logger.debug(f"Clicking page number {next_page}")
                page_link.click()
                self.page.wait_for_load_state('domcontentloaded', timeout=30000)
                self.human_delay(2, 4)
                return True
            
            # Last resort: Direct navigation to next page URL
            self.logger.debug(f"Using direct navigation to page {next_page}")
            return self.navigate_to_jobs_page(next_page)
            
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False

    def parse_salary_text(self, salary_text):
        """Parse salary text to extract min, max, currency, and type."""
        if not salary_text or salary_text.lower() in ['not specified', 'competitive', 'on application']:
            return None, None, 'AUD', 'yearly'
        
        # Extract currency
        currency = 'AUD'  # Default for Australian jobs
        if '$' in salary_text:
            currency = 'AUD'
        
        # Extract salary type (limited to 10 characters to match model)
        salary_type = 'yearly'  # Default
        if any(term in salary_text.lower() for term in ['hour', 'hourly', '/hr', 'per hour']):
            salary_type = 'hourly'
        elif any(term in salary_text.lower() for term in ['week', 'weekly', '/wk', 'per week']):
            salary_type = 'weekly'
        elif any(term in salary_text.lower() for term in ['month', 'monthly', '/mo', 'per month']):
            salary_type = 'monthly'
        elif any(term in salary_text.lower() for term in ['day', 'daily', '/day', 'per day']):
            salary_type = 'daily'
        
        # Extract numbers
        numbers = re.findall(r'[\d,]+', salary_text.replace('$', ''))
        if numbers:
            try:
                if len(numbers) >= 2:
                    # Range
                    salary_min = Decimal(numbers[0].replace(',', ''))
                    salary_max = Decimal(numbers[1].replace(',', ''))
                elif len(numbers) == 1:
                    # Single value
                    salary_min = Decimal(numbers[0].replace(',', ''))
                    salary_max = salary_min
                else:
                    return None, None, currency, salary_type
                
                return salary_min, salary_max, currency, salary_type
            except:
                pass
        
        return None, None, currency, 'yearly'

    def map_job_type(self, job_type_text):
        """Map job type text to standardized job type."""
        if not job_type_text:
            return 'full_time'
        
        job_type_lower = job_type_text.lower()
        
        for key, value in self.job_type_mapping.items():
            if key in job_type_lower:
                return value
        
        return 'full_time'  # Default

    def get_or_create_company(self, company_name):
        """Get or create a company object."""
        try:
            connections.close_all()  # Ensure clean database connection
            
            company_name = company_name.strip()
            if not company_name:
                company_name = "Unknown Company"
            
            company, created = Company.objects.get_or_create(
                slug=slugify(company_name),
                defaults={
                    'name': company_name,
                    'description': f"Hospitality employer posting jobs on Barcats Australia"
                }
            )
            
            if created:
                self.logger.debug(f"Created new company: {company_name}")
            
            return company
            
        except Exception as e:
            self.logger.error(f"Error creating company {company_name}: {e}")
            # Return a default company
            connections.close_all()
            default_company, _ = Company.objects.get_or_create(
                slug='unknown-company',
                defaults={'name': 'Unknown Company'}
            )
            return default_company

    def get_or_create_location(self, location_text):
        """Get or create a location object."""
        try:
            connections.close_all()  # Ensure clean database connection
            
            if not location_text or location_text.strip() == "":
                location_text = "Australia"
            
            # Parse location components
            location_name, city, state, country = self.parse_location(location_text)
            
            location, created = Location.objects.get_or_create(
                name=location_name,
                defaults={
                    'city': city,
                    'state': state,
                    'country': country
                }
            )
            
            if created:
                self.logger.debug(f"Created new location: {location_name}")
            
            return location
            
        except Exception as e:
            self.logger.error(f"Error creating location {location_text}: {e}")
            # Return a default location
            connections.close_all()
            default_location, _ = Location.objects.get_or_create(
                name='Australia',
                defaults={'country': 'Australia'}
            )
            return default_location

    def parse_location(self, location_text):
        """Parse location text into components."""
        if not location_text:
            return "Australia", "", "", "Australia"
        
        location_text = location_text.strip()
        
        # Australian states mapping
        australian_states = {
            'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland',
            'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
            'NT': 'Northern Territory', 'ACT': 'Australian Capital Territory'
        }
        
        # Extract state
        state = ""
        for abbrev, full_name in australian_states.items():
            if abbrev in location_text.upper() or full_name.lower() in location_text.lower():
                state = full_name
                break
        
        # Extract city (usually the part before the state)
        parts = location_text.split(',')
        city = parts[0].strip() if parts else location_text
        
        # Clean city name
        for abbrev in australian_states.keys():
            city = city.replace(abbrev, '').strip()
        
        return location_text, city, state, "Australia"

    def save_jobs_to_database(self, scraped_jobs: List[ScrapedJob]):
        """Save scraped jobs to the database."""
        for job_data in scraped_jobs:
            try:
                # Close any existing database connections to avoid async issues
                connections.close_all()
                
                # Check for duplicates
                if JobPosting.objects.filter(external_url=job_data.job_url).exists():
                    self.duplicates_found += 1
                    self.logger.info(f"Duplicate job found: {job_data.title}")
                    continue
                
                # Get or create company and location
                company = self.get_or_create_company(job_data.company_name)
                location = self.get_or_create_location(job_data.location_text)
                
                # Parse salary
                salary_min, salary_max, currency, salary_type = self.parse_salary_text(job_data.salary_text)
                
                # Map job type
                job_type = self.map_job_type(job_data.job_type)
                
                # Get job category using the categorization service
                category = JobCategorizationService.categorize_job(job_data.title, job_data.description)
                
                # Get keywords
                keywords = JobCategorizationService.get_job_keywords(job_data.title, job_data.description)
                tags = ", ".join(keywords[:3])[:10] if keywords else ""  # Limit to 10 characters total
                
                # Create job posting
                with transaction.atomic():
                    job_posting = JobPosting.objects.create(
                        title=job_data.title,
                        description=job_data.description,
                        company=company,
                        posted_by=self.bot_user,
                        location=location,
                        job_category=category,
                        job_type=job_type,
                        salary_min=salary_min,
                        salary_max=salary_max,
                        salary_currency=currency,
                        salary_type=salary_type,
                        external_source='barcats.com.au',
                        external_url=job_data.job_url,
                        posted_ago=job_data.posted_ago,
                        tags=tags,
                        status='active'
                    )
                    
                    self.jobs_saved += 1
                    self.logger.info(f"Saved job: {job_posting.title} at {company.name}")
                    
            except Exception as e:
                self.logger.error(f"Error saving job {job_data.title}: {e}")
                self.errors_count += 1

    def cleanup(self):
        """Clean up browser resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if hasattr(self, 'playwright'):
                self.playwright.stop()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def print_summary(self):
        """Print scraping summary."""
        self.logger.info("\n" + "="*50)
        self.logger.info("BARCATS AUSTRALIA SCRAPING SUMMARY")
        self.logger.info("="*50)
        self.logger.info(f"Pages scraped: {self.pages_scraped}")
        self.logger.info(f"Jobs found: {self.jobs_scraped}")
        self.logger.info(f"Jobs saved: {self.jobs_saved}")
        self.logger.info(f"Duplicates found: {self.duplicates_found}")
        self.logger.info(f"Errors encountered: {self.errors_count}")
        self.logger.info(f"Success rate: {(self.jobs_saved / max(1, self.jobs_scraped)) * 100:.1f}%")
        self.logger.info("="*50)

    def run(self):
        """Main scraping method with enhanced pagination support."""
        self.logger.info("Starting Barcats Australia hospitality jobs scraper...")
        self.logger.info(f"Configuration: job_limit={self.job_limit}")
        
        if not self.initialize_browser():
            self.logger.error("Failed to initialize browser. Exiting.")
            return
        
        try:
            page_num = 1
            max_pages = 50  # Increased safety limit for larger sites
            consecutive_empty_pages = 0
            max_empty_pages = 3  # Stop after 3 consecutive pages with no jobs
            
            while page_num <= max_pages and consecutive_empty_pages < max_empty_pages:
                self.logger.info(f"Scraping page {page_num} (Pages completed: {self.pages_scraped}, Jobs found: {self.jobs_scraped})")
                
                # Navigate to the specific page
                if not self.navigate_to_jobs_page(page_num):
                    self.logger.error(f"Failed to navigate to page {page_num}")
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= max_empty_pages:
                        self.logger.warning(f"Failed to navigate to {consecutive_empty_pages} consecutive pages. Stopping.")
                        break
                    page_num += 1
                    continue
                
                # Extract jobs from current page
                jobs_on_page = self.extract_jobs_from_page()
                
                if not jobs_on_page:
                    consecutive_empty_pages += 1
                    self.logger.warning(f"No jobs found on page {page_num}. Empty pages count: {consecutive_empty_pages}")
                    
                    if consecutive_empty_pages >= max_empty_pages:
                        self.logger.info(f"Found {consecutive_empty_pages} consecutive pages with no jobs. Ending scrape.")
                        break
                else:
                    # Reset counter if we found jobs
                    consecutive_empty_pages = 0
                    
                    # Save jobs from this page
                    self.save_jobs_to_database(jobs_on_page)
                
                self.pages_scraped += 1
                
                # Check if we've reached our job limit
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    self.logger.info(f"Reached job limit of {self.job_limit}")
                    break
                
                # Check if there's a next page using the pagination structure
                if not self.has_next_page():
                    self.logger.info("No more pages available according to pagination")
                    break
                
                # Add delay between pages to be respectful
                self.human_delay(3, 6)
                page_num += 1
                
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error during scraping: {e}")
        finally:
            self.cleanup()
            self.print_summary()


def main():
    """Main function to run the scraper."""
    # Parse command line arguments
    job_limit = None
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"Job limit set to: {job_limit}")
        except ValueError:
            print("Invalid job limit. Using unlimited.")
    
    # Create and run scraper
    scraper = BarcatsAustraliaJobScraper(job_limit=job_limit)
    scraper.run()


if __name__ == "__main__":
    main()
