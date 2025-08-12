#!/usr/bin/env python
"""
Professional Work180.com Job Scraper using Playwright

This script uses the professional database structure similar to the Seek scraper
with JobPosting, Company, and Location models, plus automatic job categorization.

Features:
- Professional database structure with proper relationships
- Automatic job categorization using AI-like keyword matching
- Human-like behavior to avoid detection
- Complete data extraction and normalization
- Playwright for modern web scraping
- Configurable job limits and categories
- Support for Work180's discipline and industry filters
- Location-based filtering (Australia, UK, US)

Usage:
    python work180_job_scraper_advanced.py [max_jobs] [category] [location]

Examples:
    python work180_job_scraper_advanced.py 50
    python work180_job_scraper_advanced.py 20 engineering
    python work180_job_scraper_advanced.py 30 it sydney
    python work180_job_scraper_advanced.py 25 healthcare melbourne

Available Categories:
    Disciplines: accounting, administration, business_development, customer_service,
                 engineering, executive_management, it, legal, marketing, operations,
                 product_management, project_management, procurement, retail, trades
    
    Industries: automotive, banking_finance, construction, consulting, healthcare,
                it_digital, manufacturing, mining_energy, retail_fashion,
                telecommunications, transport_logistics

Available Locations:
    Australia: australia, sydney, melbourne, brisbane, adelaide, perth, hobart
    UK: uk, london, manchester, birmingham
    US: us
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
        logging.FileHandler('work180_scraper_professional.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProfessionalWork180Scraper:
    """
    Professional Work180.com scraper using the advanced database structure.
    """
    
    def __init__(self, headless=False, job_category="all", job_limit=30, location="all"):
        """Initialize the professional scraper."""
        self.headless = headless
        self.base_url = "https://work180.com/en-us/for-women/job-search"
        self.job_limit = job_limit
        
        # Initialize counters
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Set start URL based on job category and location
        self.start_url = self._build_search_url(job_category, location)
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
        logger.info(f"Initialized Work180 scraper with URL: {self.start_url}")
    
    def _build_search_url(self, job_category, location):
        """Build the search URL based on category and location filters."""
        base_url = "https://work180.com/en-us/for-women/job-search"
        
        # Map categories to Work180's actual disciplines and industries
        category_mappings = {
            "all": "",
            # Disciplines
            "accounting": "?discipline=accounting",
            "administration": "?discipline=administration", 
            "business_development": "?discipline=business-development-and-sales",
            "customer_service": "?discipline=customer-service-and-support",
            "engineering": "?discipline=engineering",
            "executive_management": "?discipline=executive-and-general-management",
            "it": "?discipline=it",
            "legal": "?discipline=legal",
            "marketing": "?discipline=marketing-and-communications",
            "operations": "?discipline=operations",
            "product_management": "?discipline=product-management",
            "project_management": "?discipline=program-and-project-management",
            "procurement": "?discipline=purchasing-and-procurement",
            "retail": "?discipline=retail",
            "trades": "?discipline=trades-and-technicians",
            # Industries
            "automotive": "?industry=auto",
            "banking_finance": "?industry=banking-investment-and-finance",
            "construction": "?industry=construction",
            "consulting": "?industry=consulting-and-professional-services",
            "healthcare": "?industry=healthcare-and-medical",
            "it_digital": "?industry=it-digital-and-online-media-services",
            "manufacturing": "?industry=manufacturing-and-operations",
            "mining_energy": "?industry=mining-resources-and-energy",
            "retail_fashion": "?industry=retail-and-fashion",
            "telecommunications": "?industry=telecommunications",
            "transport_logistics": "?industry=transport-shipping-and-logistics",
            # Legacy mappings
            "technology": "?discipline=it",
            "finance": "?industry=banking-investment-and-finance",
            "sales": "?discipline=business-development-and-sales"
        }
        
        # Map locations to Work180's location parameters
        location_mappings = {
            "all": "",
            # Australia
            "australia": "&location=all-australia",
            "sydney": "&location=sydney",
            "melbourne": "&location=melbourne", 
            "brisbane": "&location=brisbane",
            "adelaide": "&location=adelaide",
            "perth": "&location=perth",
            "hobart": "&location=hobart",
            # UK
            "uk": "&location=all-united-kingdom",
            "london": "&location=london",
            "manchester": "&location=manchester",
            "birmingham": "&location=birmingham",
            # US
            "us": "&location=all-united-states"
        }
        
        # Build URL with category and location
        category_param = category_mappings.get(job_category, "")
        location_param = location_mappings.get(location, "")
        
        if category_param and location_param:
            return base_url + category_param + location_param
        elif category_param:
            return base_url + category_param
        elif location_param:
            return base_url + "?" + location_param[1:]  # Remove & and add ?
        else:
            return base_url
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='work180_scraper_system',
                defaults={
                    'email': 'system@work180scraper.com',
                    'first_name': 'Work180',
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
        
        # Australian cities for better location parsing
        australian_cities = [
            'sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 
            'canberra', 'darwin', 'hobart', 'gold coast', 'newcastle',
            'wollongong', 'geelong', 'townsville', 'cairns', 'toowoomba',
            'ballarat', 'bendigo', 'albury', 'mackay', 'rockhampton',
            'launceston', 'wayville', 'alexandria', 'osborne', 'coffs harbour',
            'port hedland'
        ]
        
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
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*-\s*AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
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
            
            # Extract job title - Based on Work180 website structure
            try:
                title_selectors = [
                    # From the image: job titles are prominent in the cards
                    'h2 a', 'h3 a', 'a h2', 'a h3',
                    '[data-testid*="title"] a', '.job-title a',
                    'a[href*="/job/"]', 'a[href*="/employer/"]',
                    # Work180 specific selectors
                    'a[href*="/en-us/for-women/employer/"]',
                    'div[class*="title"] a', 'span[class*="title"] a'
                ]
                
                job_data['job_title'] = ""
                for selector in title_selectors:
                    title_element = job_element.query_selector(selector)
                    if title_element:
                        title_text = title_element.inner_text().strip()
                        if title_text and len(title_text) > 5:
                            job_data['job_title'] = title_text
                            break
            except:
                job_data['job_title'] = ""
            
            # Extract company name - From "Posted by [Company Name]" pattern
            try:
                company_selectors = [
                    # From the image: "Posted by Aristocrat 5 hours ago"
                    'span:has-text("Posted by")', 
                    '[class*="company"]', '[class*="employer"]',
                    '.job-company', '.employer-name',
                    'strong', 'b', 'span[class*="name"]',
                    # Look for text containing "Posted by"
                    'span', 'div', 'p'
                ]
                
                job_data['company_name'] = ""
                for selector in company_selectors:
                    company_element = job_element.query_selector(selector)
                    if company_element:
                        company_text = company_element.inner_text().strip()
                        # Extract company name from "Posted by Company Name" pattern
                        if "Posted by" in company_text:
                            company_match = re.search(r'Posted by\s+([^0-9]+?)\s+\d+', company_text)
                            if company_match:
                                job_data['company_name'] = company_match.group(1).strip()
                                break
                        elif company_text and len(company_text) > 2 and len(company_text) < 100:
                            job_data['company_name'] = company_text
                            break
            except:
                job_data['company_name'] = ""
            
            # Extract location - From the red pin icon pattern
            try:
                location_selectors = [
                    # From the image: location with red pin icon
                    'p.pl-1', '[class*="pl-1"]', '[data-testid*="location"]',
                    '.location', '[class*="location"]', '.job-location',
                    # Look for elements with pin icons or location text
                    'span[class*="location"]', 'div[class*="location"]',
                    'p:has-text(",")', 'span:has-text(",")'
                ]
                
                job_data['location_text'] = ""
                for selector in location_selectors:
                    location_element = job_element.query_selector(selector)
                    if location_element:
                        location_text = location_element.inner_text().strip()
                        if location_text and len(location_text) > 1 and len(location_text) < 50:
                            job_data['location_text'] = location_text
                            break
            except:
                job_data['location_text'] = ""
            
            # Extract job URL
            try:
                link_selectors = [
                    'a[href*="/job/"]', 'a[href*="/employer/"]',
                    'a[href*="/jobs/"]', 'a[href*="/position/"]',
                    'h2 a', 'h3 a', '.card a', '.job-card a'
                ]
                
                job_data['job_url'] = ""
                for selector in link_selectors:
                    link_element = job_element.query_selector(selector)
                    if link_element:
                        href = link_element.get_attribute('href')
                        if href:
                            if href.startswith('http'):
                                job_data['job_url'] = href
                            elif href.startswith('/'):
                                job_data['job_url'] = urljoin(self.base_url, href)
                            else:
                                job_data['job_url'] = urljoin(self.base_url, '/' + href)
                            break
            except:
                job_data['job_url'] = ""
            
            # Extract posting date
            try:
                date_selectors = [
                    '[data-testid*="date"]', '.posted-date', '[class*="date"]',
                    '[class*="posted"]', '[aria-label*="date"]', 'time'
                ]
                
                job_data['posted_ago'] = ""
                for selector in date_selectors:
                    date_element = job_element.query_selector(selector)
                    if date_element:
                        date_text = date_element.inner_text().strip()
                        if date_text:
                            job_data['posted_ago'] = date_text
                            break
            except:
                job_data['posted_ago'] = ""
            
            # Extract job summary/description
            try:
                summary_selectors = [
                    '[data-testid*="description"]', '.job-description',
                    '[class*="description"]', '.job-summary', '.summary'
                ]
                
                job_data['summary'] = ""
                for selector in summary_selectors:
                    summary_element = job_element.query_selector(selector)
                    if summary_element:
                        summary_text = summary_element.inner_text().strip()
                        if summary_text:
                            job_data['summary'] = summary_text
                            break
            except:
                job_data['summary'] = ""
            
            # Extract salary information
            try:
                salary_selectors = [
                    '[data-testid*="salary"]', '.salary', '[class*="salary"]',
                    '.job-salary', '.compensation', '[class*="pay"]'
                ]
                
                job_data['salary_text'] = ""
                for selector in salary_selectors:
                    salary_element = job_element.query_selector(selector)
                    if salary_element:
                        salary_text = salary_element.inner_text().strip()
                        if salary_text:
                            job_data['salary_text'] = salary_text
                            break
            except:
                job_data['salary_text'] = ""
            
            # Extract job type and work mode from badges/tags
            try:
                badge_selectors = [
                    '[data-testid*="badge"]', '.badge', '[class*="badge"]',
                    '.job-type', '[class*="type"]', '.work-mode', '[class*="mode"]'
                ]
                
                badges = []
                for selector in badge_selectors:
                    badge_elements = job_element.query_selector_all(selector)
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
            
            logger.debug(f"Extracted job data: {job_data['job_title']} at {job_data['company_name']}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {str(e)}")
            return None
    
    def extract_job_details(self, job_url, page, current_page_url):
        """Extract detailed information from job detail page and return to original page."""
        try:
            logger.info(f"Loading job details: {job_url}")
            
            # Store current page URL to return to
            original_url = current_page_url
            
            # Navigate to job detail page
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(3, 5)
            
            details = {}
            
            # Extract detailed description - Based on Work180 job detail page structure
            try:
                desc_selectors = [
                    # From the job detail page image: main content area
                    '[data-testid*="description"]', '.job-description',
                    '[class*="description"]', '.job-details', '.description',
                    '.job-content', '.content', '[class*="content"]',
                    '.job-detail-content', '[role="main"]', 'main',
                    # Work180 specific selectors
                    'div[class*="job-description"]', 'div[class*="content"]',
                    'section[class*="description"]', 'article[class*="content"]',
                    # Look for the main text content area
                    'div:has-text("What You\'ll Do")', 'div:has-text("Requirements")',
                    'div:has-text("Responsibilities")', 'div:has-text("About")',
                    # Additional selectors for job content
                    '[class*="job-body"]', '[class*="job-content"]',
                    '[class*="description-content"]', '[class*="job-details-content"]',
                    'div[class*="main-content"]', 'div[class*="content-body"]'
                ]
                
                for selector in desc_selectors:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        desc_text = desc_element.inner_text().strip()
                        if desc_text and len(desc_text) > 100:
                            details['description'] = desc_text
                            logger.info(f"Found description with selector: {selector} (length: {len(desc_text)})")
                            break
                
                # If no description found, try to get the main body text
                if not details.get('description'):
                    body_selectors = [
                        'body', 'main', '[role="main"]', '.main-content',
                        'div[class*="main"]', 'div[class*="body"]',
                        'div[class*="container"]', 'div[class*="wrapper"]'
                    ]
                    for selector in body_selectors:
                        body_element = page.query_selector(selector)
                        if body_element:
                            body_text = body_element.inner_text().strip()
                            if body_text and len(body_text) > 200:
                                details['description'] = body_text
                                logger.info(f"Found description in body with selector: {selector} (length: {len(body_text)})")
                                break
                
                # If still no description, try to extract from any text content
                if not details.get('description'):
                    # Get all text content from the page
                    all_text = page.inner_text()
                    if all_text and len(all_text) > 500:
                        details['description'] = all_text
                        logger.info(f"Using full page text as description (length: {len(all_text)})")
                        
            except Exception as e:
                logger.error(f"Error extracting description: {str(e)}")
                details['description'] = ""
            
            # Extract detailed salary information
            try:
                salary_selectors = [
                    '[data-testid*="salary"]', '.salary', '[class*="salary"]',
                    '.job-salary', '.compensation', '[class*="pay"]'
                ]
                
                for selector in salary_selectors:
                    salary_element = page.query_selector(selector)
                    if salary_element:
                        salary_text = salary_element.inner_text().strip()
                        if salary_text:
                            details['salary_text'] = salary_text
                            break
            except:
                details['salary_text'] = ""
            
            # Extract company information from detail page
            try:
                company_selectors = [
                    '[data-testid*="company"]', '.company-name', '.employer-name',
                    '.organization', '[class*="company"]', '[class*="employer"]',
                    'h1', 'h2:not(:contains("Job"))', '.brand', '[class*="brand"]'
                ]
                
                for selector in company_selectors:
                    company_element = page.query_selector(selector)
                    if company_element:
                        company_text = company_element.inner_text().strip()
                        if company_text and len(company_text) > 2 and len(company_text) < 100:
                            details['company_name'] = company_text
                            break
            except:
                details['company_name'] = ""
            
            # Extract location from detail page
            try:
                location_selectors = [
                    'p.pl-1', '[class*="pl-1"]', '[data-testid*="location"]',
                    '.job-detail-header [class*="location"]', '.job-header [class*="location"]',
                    '.job-summary [class*="location"]', '[class*="location"]', '.location'
                ]
                
                for selector in location_selectors:
                    location_element = page.query_selector(selector)
                    if location_element:
                        location_text = location_element.inner_text().strip()
                        if location_text and len(location_text) > 1 and len(location_text) < 100:
                            details['location_text'] = location_text
                            break
            except:
                details['location_text'] = ""
            
            # IMPORTANT: Return to the original job search page
            logger.info(f"Returning to job search page: {original_url}")
            page.goto(original_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(2, 4)
            
            return details
            
        except Exception as e:
            logger.error(f"Error extracting job details: {str(e)}")
            # Try to return to original page even if there was an error
            try:
                page.goto(original_url, wait_until='domcontentloaded', timeout=30000)
                self.human_delay(2, 4)
            except:
                logger.error("Failed to return to original page after error")
            return {}
    
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
                if not company_name or company_name == "Work180 Client":
                    company_name = "Work180 Partner Company"
                
                company_slug = slugify(company_name)
                
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Jobs from Work180.com',
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
                    external_source='work180.com',
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
        # Get current page URL for navigation back
        current_page_url = page.url
        logger.info(f"Scraping page: {current_page_url}")
        
        # Wait for job listings to load
        try:
            # Based on the Work180 website structure from the actual image
            selectors_to_try = [
                # Work180 specific selectors from the actual website structure
                "li.sc-d9bcd693-1",  # Main job card container (from image)
                ".sc-9469ea43-0",    # Job link container (from image)
                "li[class*='sc-d9bcd693']",  # Job list items
                "div[class*='sc-9469ea43']", # Job card divs
                # Alternative selectors based on the website structure
                "div[class*='job-card']", "div[class*='listing']",
                "li[class*='job']", "article[class*='job']",
                "[data-testid*='job']", "a[href*='/job/']",
                # Generic fallbacks
                ".job-card", ".job-item", ".listing-item",
                "div[class*='job']", "article[class*='job']",
                "[data-testid*='search-result']", "[data-testid*='job-card']",
                # Additional selectors for job cards
                "div[role='listitem']", "li[role='listitem']",
                "div[class*='search-result']", "div[class*='job-listing']"
            ]
            
            job_elements = []
            for selector in selectors_to_try:
                try:
                    # Wait for selector with shorter timeout
                    page.wait_for_selector(selector, timeout=5000)
                    elements = page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        # Filter out elements that are not visible or have no content
                        visible_elements = []
                        for element in elements:
                            try:
                                if element.is_visible():
                                    # Check if element has some content
                                    text = element.inner_text().strip()
                                    if text and len(text) > 10:  # At least some meaningful content
                                        visible_elements.append(element)
                            except:
                                continue
                        
                        if visible_elements:
                            job_elements = visible_elements
                            logger.info(f"Found {len(job_elements)} valid jobs using selector: {selector}")
                            break
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {str(e)}")
                    continue
            
            if not job_elements:
                logger.warning("No valid job listings found on page")
                return 0
                
        except Exception as e:
            logger.warning(f"Error finding job listings: {str(e)}")
            return 0
        
        # Scroll down to load all jobs
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        self.human_delay(2, 4)
        
        logger.info(f"Found {len(job_elements)} job listings on current page")
        
        # Track processed jobs to avoid duplicates
        processed_urls = set()
        jobs_processed = 0
        
        # Extract data from each job - PROCESS ALL JOBS ON THE PAGE
        for i, job_element in enumerate(job_elements):
            try:
                # Check if we've reached the job limit
                if self.job_limit and self.scraped_count >= self.job_limit:
                    logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                    return -1  # Special return value to indicate limit reached
                
                # Check if element is still attached to DOM before interacting
                try:
                    if not job_element.is_visible():
                        logger.warning(f"Job element {i+1} is not visible, skipping...")
                        continue
                except:
                    logger.warning(f"Job element {i+1} is no longer attached to DOM, skipping...")
                    continue
                
                # Extract job data first (before scrolling to avoid DOM detachment)
                job_data = self.extract_job_data(job_element, page)
                if job_data and job_data.get('job_url'):
                    # Check if we've already processed this URL on this page
                    if job_data['job_url'] in processed_urls:
                        logger.debug(f"Already processed URL: {job_data['job_url']}")
                        continue
                    
                    processed_urls.add(job_data['job_url'])
                    
                    # Now try to scroll (with error handling)
                    try:
                        job_element.scroll_into_view_if_needed()
                        self.human_delay(0.5, 1.5)
                    except Exception as scroll_error:
                        logger.debug(f"Scroll error for job {i+1}: {str(scroll_error)}")
                        # Continue without scrolling
                    
                    # Get detailed information from job page (with navigation back)
                    details = self.extract_job_details(job_data['job_url'], page, current_page_url)
                    
                    # Merge details with job data
                    job_data.update(details)
                    
                    # Save the job to database
                    if self.save_job_to_database(job_data):
                        jobs_processed += 1
                        logger.info(f"Successfully processed job {i+1}/{len(job_elements)}: {job_data.get('job_title', 'Unknown')}")
                    else:
                        logger.warning(f"Failed to save job {i+1}: {job_data.get('job_title', 'Unknown')}")
                else:
                    logger.warning(f"Failed to extract data for job {i+1}")
                    
            except Exception as e:
                logger.error(f"Error processing job {i+1}: {str(e)}")
                self.error_count += 1
                continue
        
        logger.info(f"Successfully processed {jobs_processed} jobs out of {len(job_elements)} found on this page")
        return jobs_processed
    
    def has_next_page(self, page):
        """Check if there's a next page available."""
        try:
            # Based on Work180 website structure - check for pagination controls
            next_selectors = [
                # Standard pagination selectors
                'a[aria-label="Next"]',
                'a[data-automation="page-next"]', 
                'a:has-text("Next")',
                'a:has-text(">")',
                '[data-automation="pagination-next"]',
                '.pagination a:last-child',
                '[data-automation="pagination"] a:last-child',
                'nav a[aria-label="Next page"]',
                'button[aria-label="Next"]',
                # Work180 specific pagination patterns
                'a[href*="page="]',  # URL-based pagination
                'a[href*="&page="]',
                'a[href*="?page="]',
                # Work180 specific selectors
                '[data-testid*="pagination"] a:last-child',
                '.pagination-next',
                '[class*="pagination"] a:last-child',
                'button[class*="next"]',
                'a[class*="next"]',
                # Check for "Load More" functionality (common in modern job sites)
                'button:has-text("Load More")',
                'button:has-text("Show More")',
                'button:has-text("Load More Jobs")',
                '[data-testid*="load-more"]',
                '.load-more',
                '[class*="load-more"]',
                # Additional pagination selectors
                'a[aria-label*="next"]',
                'button[aria-label*="next"]',
                'a[title*="next"]',
                'button[title*="next"]',
                # Look for pagination numbers
                'a:has-text("2")', 'a:has-text("3")', 'a:has-text("4")', 'a:has-text("5")',
                # Look for pagination containers
                '[class*="pagination"] a:not([aria-current="true"])',
                'nav a:not([aria-current="true"])'
            ]
            
            for selector in next_selectors:
                try:
                    next_element = page.query_selector(selector)
                    if next_element and next_element.is_enabled() and next_element.is_visible():
                        logger.debug(f"Found next page button with selector: {selector}")
                        return True
                except:
                    continue
            
            # Also check if there are more job cards loaded
            try:
                # Check if there's a "Load More" button
                load_more_selectors = [
                    'button:has-text("Load More")',
                    'button:has-text("Show More")',
                    'button:has-text("Load More Jobs")',
                    '[data-testid*="load-more"]',
                    '.load-more',
                    '[class*="load-more"]'
                ]
                
                for selector in load_more_selectors:
                    load_more_element = page.query_selector(selector)
                    if load_more_element and load_more_element.is_enabled() and load_more_element.is_visible():
                        logger.debug(f"Found load more button with selector: {selector}")
                        return True
            except:
                pass
            
            # Check if we can find pagination numbers that are clickable
            try:
                pagination_numbers = page.query_selector_all('a[href*="page="]')
                for num in pagination_numbers:
                    if num.is_enabled() and num.is_visible():
                        text = num.inner_text().strip()
                        if text.isdigit() and int(text) > 1:
                            logger.debug(f"Found pagination number: {text}")
                            return True
            except:
                pass
            
            return False
        except Exception as e:
            logger.debug(f"Error checking for next page: {str(e)}")
            return False
    
    def go_to_next_page(self, page):
        """Navigate to the next page of results."""
        try:
            # Work180 specific pagination selectors
            next_selectors = [
                'a[aria-label="Next"]',
                'a[data-automation="page-next"]',
                'a:has-text("Next")',
                'a:has-text(">")',
                '[data-automation="pagination-next"]',
                '.pagination a:last-child',
                'nav a[aria-label="Next page"]',
                'button[aria-label="Next"]',
                'a[href*="page="]',
                'a[href*="&page="]',
                'a[href*="?page="]',
                # Work180 specific selectors
                '[data-testid*="pagination"] a:last-child',
                '.pagination-next',
                '[class*="pagination"] a:last-child',
                'button[class*="next"]',
                'a[class*="next"]'
            ]
            
            for selector in next_selectors:
                try:
                    next_element = page.query_selector(selector)
                    if next_element and next_element.is_enabled() and next_element.is_visible():
                        logger.info(f"Clicking next page using selector: {selector}")
                        
                        # Scroll to element and click
                        next_element.scroll_into_view_if_needed()
                        self.human_delay(1, 2)
                        next_element.click()
                        
                        # Wait for page to load with longer timeout
                        self.human_delay(3, 5)
                        page.wait_for_load_state('domcontentloaded', timeout=30000)
                        
                        return True
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {str(e)}")
                    continue
            
            # Try "Load More" button if pagination doesn't work
            try:
                load_more_selectors = [
                    'button:has-text("Load More")',
                    'button:has-text("Show More")',
                    'button:has-text("Load More Jobs")',
                    '[data-testid*="load-more"]',
                    '.load-more',
                    '[class*="load-more"]'
                ]
                
                for selector in load_more_selectors:
                    load_more_element = page.query_selector(selector)
                    if load_more_element and load_more_element.is_enabled() and load_more_element.is_visible():
                        logger.info(f"Clicking load more using selector: {selector}")
                        
                        load_more_element.scroll_into_view_if_needed()
                        self.human_delay(1, 2)
                        load_more_element.click()
                        
                        # Wait for new content to load
                        self.human_delay(3, 5)
                        page.wait_for_load_state('networkidle', timeout=30000)
                        
                        return True
            except Exception as e:
                logger.debug(f"Error with load more: {str(e)}")
            
            logger.warning("No next page or load more button found")
            return False
            
        except Exception as e:
            logger.error(f"Error navigating to next page: {str(e)}")
            return False
    
    def run(self):
        """Main method to run the complete scraping process."""
        logger.info("Starting Professional Work180.com job scraper...")
        logger.info(f"Target URL: {self.start_url}")
        logger.info(f"Job limit: {self.job_limit}")
        
        with sync_playwright() as p:
            # Launch browser
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
            
            try:
                # Navigate to starting URL with retry logic
                logger.info("Navigating to Work180.com...")
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
                max_pages = 10  # Safety limit to prevent infinite loops
                processed_urls = set()  # Track all processed URLs across pages
                
                while page_number <= max_pages:
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
                logger.info("PROFESSIONAL WORK180 SCRAPING COMPLETED!")
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
    print("🔍 Professional Work180.com Job Scraper")
    print("="*50)
    
    # Parse command line arguments
    max_jobs = 30  # Default
    job_category = "all"  # Default
    location = "all"  # Default
    
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            print("Invalid number of jobs. Using default: 30")
    
    if len(sys.argv) > 2:
        job_category = sys.argv[2]
    
    if len(sys.argv) > 3:
        location = sys.argv[3]
    
    print(f"Target: {max_jobs} jobs")
    print(f"Category: {job_category}")
    print(f"Location: {location}")
    print("Database: Professional structure with JobPosting, Company, Location")
    print("="*50)
    
    # Create scraper instance with professional settings
    scraper = ProfessionalWork180Scraper(
        headless=False, 
        job_category=job_category, 
        job_limit=max_jobs,
        location=location
    )
    
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