#!/usr/bin/env python3
"""
Professional Victorian Government Job Scraper
==============================================

Advanced scraper for Victorian Government careers website (https://www.careers.vic.gov.au/jobs) that integrates with 
your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Thread-safe database operations
- Victorian Government job portal optimization

This scraper handles the Victoria Government's official job portal which features:
- Modern web interface with job cards
- Rich job information including salary ranges
- Multiple work types and classifications
- Detailed location information
- Work type specifications

Features:
- 🎯 Smart job data extraction from Victoria Government careers site
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional government job categorization

Usage:
    python vic_government_scraper_advanced.py [job_limit]
    
Examples:
    python vic_government_scraper_advanced.py 50    # Scrape 50 jobs
    python vic_government_scraper_advanced.py       # Scrape all available jobs
"""

import os
import sys
import django
import time
import random
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()


class VictorianGovernmentJobScraper:
    """Professional Victorian Government job scraper using Playwright."""
    
    def __init__(self, job_limit=None, max_pages=None, start_page=1):
        """Initialize the scraper with optional job limit and pagination settings.
        
        Args:
            job_limit (int): Maximum number of jobs to scrape (None for unlimited)
            max_pages (int): Maximum number of pages to scrape (None for all pages)
            start_page (int): Page number to start scraping from (default: 1)
        """
        self.base_url = "https://www.careers.vic.gov.au"
        self.search_url = f"{self.base_url}/jobs?keywords="
        self.job_limit = job_limit
        self.max_pages = max_pages
        self.start_page = start_page
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
        
        # User agents for rotation (Government sites prefer standard browsers)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        # Victorian Government job categories based on the website content
        self.vic_job_categories = {
            'health': ['mental health', 'practitioner', 'nursing', 'medical', 'health', 'clinical', 'hospital', 'geriatrician', 'podiatrist'],
            'education': ['teacher', 'education', 'school', 'classroom', 'curriculum', 'training'],
            'administration': ['administrative', 'support officer', 'project officer', 'admin', 'clerical', 'officer', 'coordinator', 'assistant'],
            'justice': ['justice', 'forensic', 'legal', 'court', 'corrective', 'prison', 'security'],
            'policy': ['policy', 'senior policy', 'government', 'strategic'],
            'technology': ['it', 'digital', 'software', 'data', 'cyber', 'technology', 'systems'],
            'community_services': ['community', 'mental health team', 'social', 'welfare'],
            'emergency': ['emergency', 'safety', 'security advisor', 'risk'],
            'other': ['general', 'various', 'other']
        }
        
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('vic_government_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_or_create_bot_user(self):
        """Get or create bot user for job posting."""
        try:
            user, created = User.objects.get_or_create(
                username='vic_government_scraper_bot',
                defaults={
                    'email': 'bot@vicgovernment.scraper.com',
                    'first_name': 'Victorian Government',
                    'last_name': 'Scraper Bot',
                    'is_staff': True,
                    'is_active': True
                }
            )
            if created:
                self.logger.info("Created bot user for job posting")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {str(e)}")
            return None
    
    def human_delay(self, min_seconds=1, max_seconds=3):
        """Add human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        self.logger.debug(f"Waiting {delay:.2f} seconds...")
        time.sleep(delay)
    
    def setup_browser_context(self, browser):
        """Setup browser context with realistic settings."""
        context = browser.new_context(
            user_agent=random.choice(self.user_agents),
            viewport={'width': 1920, 'height': 1080},
            java_script_enabled=True,
            accept_downloads=False,
            has_touch=False,
            is_mobile=False,
            locale='en-AU',
            timezone_id='Australia/Melbourne',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-AU,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        return context
    
    def parse_date(self, date_string):
        """Parse date strings from Victorian Government job postings."""
        if not date_string:
            return None
            
        date_string = date_string.strip()
        now = datetime.now()
        
        try:
            # Handle "Wednesday 3 September 2025" format
            if re.match(r'[A-Za-z]+ \d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%A %d %B %Y")
            
            # Handle "Thursday 4 September 2025" format
            elif re.match(r'[A-Za-z]+ \d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%A %d %B %Y")
            
            # Handle "Sunday 14 September 2025" format
            elif re.match(r'[A-Za-z]+ \d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%A %d %B %Y")
            
            # Handle DD/MM/YYYY format
            elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_string):
                return datetime.strptime(date_string, "%d/%m/%Y")
            
            # Handle relative dates like "2 days ago"
            elif 'ago' in date_string.lower():
                match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_string.lower())
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
                        delta = timedelta(days=number * 30)
                    
                    return now - delta
            
            return None
            
        except ValueError as e:
            self.logger.warning(f"Could not parse date '{date_string}': {e}")
            return None
    
    def parse_salary(self, salary_text):
        """Parse salary information from Victorian Government job postings.""" 
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for Victorian Government salary extraction
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)',  # Range with $
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)',       # Range without $
            r'\$(\d{1,3}(?:,\d{3})*)',                                  # Single amount with $
            r'(\d{1,3}(?:,\d{3})*)',                                    # Single amount
        ]
        
        salary_min = None
        salary_max = None
        currency = "AUD"
        salary_type = "yearly"  # Default for government jobs
        
        # Check for salary type indicators
        if any(word in salary_text.lower() for word in ['hour', 'hourly', 'per hour']):
            salary_type = "hourly"
        elif any(word in salary_text.lower() for word in ['week', 'weekly', 'per week']):
            salary_type = "weekly"
        elif any(word in salary_text.lower() for word in ['month', 'monthly', 'per month']):
            salary_type = "monthly"
        elif any(word in salary_text.lower() for word in ['day', 'daily', 'per day']):
            salary_type = "daily"
        
        for pattern in patterns:
            match = re.search(pattern, salary_text.replace(',', ''))
            if match:
                groups = match.groups()
                if len(groups) == 2:  # Range
                    try:
                        salary_min = Decimal(groups[0].replace(',', ''))
                        salary_max = Decimal(groups[1].replace(',', ''))
                        break
                    except:
                        continue
                elif len(groups) == 1:  # Single amount
                    try:
                        salary_min = Decimal(groups[0].replace(',', ''))
                        break
                    except:
                        continue
        
        return salary_min, salary_max, currency, salary_type, salary_text
    
    def parse_location(self, location_string):
        """Parse location string into normalized location data for Victoria."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Victorian locations mapping
        vic_locations = {
            'melbourne': 'Melbourne',
            'geelong': 'Geelong',
            'ballarat': 'Ballarat',
            'bendigo': 'Bendigo',
            'latrobe valley': 'Latrobe Valley',
            'warrnambool': 'Warrnambool',
            'shepparton': 'Shepparton',
            'horsham': 'Horsham',
            'mildura': 'Mildura',
            'wodonga': 'Wodonga',
            'sale': 'Sale',
            'traralgon': 'Traralgon',
            'morwell': 'Morwell',
            'frankston': 'Frankston',
            'dandenong': 'Dandenong',
            'pakenham': 'Pakenham',
            'cranbourne': 'Cranbourne',
            'regional': 'Regional Victoria'
        }
        
        # Melbourne regions
        melb_regions = {
            'cbd': 'Melbourne CBD',
            'inner metro': 'Melbourne - Inner Metro',
            'eastern suburbs': 'Melbourne - Eastern Suburbs',
            'northern suburbs': 'Melbourne - Northern Suburbs',
            'western suburbs': 'Melbourne - Western Suburbs',
            'southern suburbs': 'Melbourne - Southern Suburbs',
            'south east': 'Melbourne - South East'
        }
        
        # Split by common separators
        parts = [part.strip() for part in location_string.replace(' - ', ' ').split(',')]
        
        city = ""
        state = "Victoria"
        country = "Australia"
        
        location_lower = location_string.lower()
        
        # Check for Melbourne regions first
        for key, value in melb_regions.items():
            if key in location_lower:
                city = value
                break
        
        # Then check for other Victorian locations
        if not city:
            for key, value in vic_locations.items():
                if key in location_lower:
                    city = value
                    break
        
        # Fallback to first part if no match
        if not city and parts:
            city = parts[0]
        
        # Create location name
        if city:
            if 'melbourne' in city.lower():
                location_name = city
            else:
                location_name = f"{city}, Victoria"
        else:
            location_name = location_string
        
        return location_name, city, state, country
    
    def determine_job_category(self, title, description, company_name):
        """Determine job category based on title, description, and company."""
        try:
            # Use the categorization service first
            category = JobCategorizationService.categorize_job(title, description)
            
            if category != 'other':
                return category
            
            # Victorian Government specific categorization
            title_lower = title.lower()
            desc_lower = (description or "").lower()
            company_lower = (company_name or "").lower()
            
            combined_text = f"{title_lower} {desc_lower} {company_lower}"
            
            for category, keywords in self.vic_job_categories.items():
                if any(keyword in combined_text for keyword in keywords):
                    return category
            
            return 'other'
            
        except Exception as e:
            self.logger.error(f"Error determining job category: {e}")
            return 'other'
    
    def extract_job_cards(self, page):
        """Extract job cards from the search results page."""
        try:
            # Wait for job listings to load
            self.human_delay(3, 5)
            
            # Log current page info
            self.logger.info(f"Current page URL: {page.url}")
            self.logger.info(f"Current page title: {page.title()}")
            
            # Look for job cards using the actual website structure
            job_card_selectors = [
                '.views-row',  # Based on actual HTML structure provided
                '.job-searchResult',  # Alternative selector
                'article',  # Fallback
                '.job-card',
                '.job-listing'
            ]
            
            job_cards = []
            
            for selector in job_card_selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    
                    # Filter elements that actually contain job information
                    for element in elements:
                        try:
                            text_content = element.text_content() or ""
                            # Check if this element contains job-like content with reasonable size
                            if (len(text_content) > 100 and len(text_content) < 2000 and 
                                any(keyword in text_content.lower() for keyword in [
                                    'work type', 'salary', 'grade', 'occupation', 
                                    'location', 'applications close'
                                ])):
                                job_cards.append(element)
                        except:
                            continue
                    
                    if job_cards:
                        self.logger.info(f"Selected {len(job_cards)} valid job cards")
                        break
            
            # Define job titles from the website content
            job_titles = [
                'Mental Health Practitioner', 'Administrative Project Officer',
                'Administrative Support Officer', 'Consultant Geriatrician',
                'Forensic Senior Clinician', 'Justice Officer', 'Security Advisor',
                'Senior Lawyer', 'Senior Policy Officer', 'Obstetrician Gynaecologist',
                'Podiatrist', 'Classroom Teacher'
            ]
            
            # If no job cards found, log for debugging
            if not job_cards:
                self.logger.warning("No job cards found with any selector")
            
            self.logger.info(f"Total job cards extracted: {len(job_cards)}")
            return job_cards
            
        except Exception as e:
            self.logger.error(f"Error extracting job cards: {e}")
            return []
    
    def extract_job_info(self, job_card):
        """Extract job information from a job card element."""
        try:
            job_data = {}
            
            # Debug: log the type of job_card we're processing
            self.logger.debug(f"Processing job_card type: {type(job_card)}, content: {str(job_card)[:100]}")
            
            # Handle text-based job data
            if isinstance(job_card, dict) and job_card.get('type') == 'text_data':
                job_data = job_card['data'].copy()
                
                # Generate URL for this job
                if job_data.get('title'):
                    title_slug = slugify(job_data['title'])
                    job_data['url'] = f"{self.base_url}/job/{title_slug}"
                
                self.logger.debug(f"Processing text-based job: {job_data.get('title', 'Unknown')}")
                return job_data
            
            # Handle element-based extraction
            card_text = job_card.text_content() or ""
            self.logger.debug(f"Processing job card: {card_text[:200]}...")
            
            # Safety check: if the card text is too long, it's probably the entire page
            if len(card_text) > 5000:
                self.logger.warning(f"Job card text too long ({len(card_text)} chars), skipping as it's likely the entire page")
                return {}
            
            # Extract job title using the actual HTML structure
            try:
                # Based on the provided HTML: .job-searchResult-header__title h3
                title_element = job_card.query_selector('.job-searchResult-header__title h3')
                if title_element:
                    job_data['title'] = title_element.text_content().strip()
                else:
                    # Fallback selectors
                    for selector in ['h3', 'h2', 'h1', '.rpl-text-link h3']:
                        title_element = job_card.query_selector(selector)
                        if title_element:
                            title_text = title_element.text_content().strip()
                            if len(title_text) > 5 and len(title_text) < 100:
                                job_data['title'] = title_text
                                break
                            
            except Exception as e:
                self.logger.debug(f"Error extracting title: {e}")
            
            # Extract job URL from the link
            try:
                link_element = job_card.query_selector('.job-searchResult-header__title a')
                if link_element:
                    href = link_element.get_attribute('href')
                    if href:
                        job_data['url'] = urljoin(self.base_url, href) if not href.startswith('http') else href
            except Exception as e:
                self.logger.debug(f"Error extracting URL: {e}")
            
            # Extract company/organisation using HTML structure
            try:
                # Based on HTML: <p class="rpl-type-p-small...">Department of Justice and Community Safety</p>
                company_element = job_card.query_selector('.job-searchResult-header__title .rpl-type-p-small')
                if company_element:
                    job_data['company'] = company_element.text_content().strip()
                else:
                    # Fallback pattern search
                    if 'health' in card_text.lower():
                        job_data['company'] = 'Victorian Government - Health'
                    elif 'education' in card_text.lower() or 'school' in card_text.lower():
                        job_data['company'] = 'Government schools'
                    elif 'justice' in card_text.lower():
                        job_data['company'] = 'Department of Justice and Community Safety'
                    else:
                        job_data['company'] = 'Victorian Government'
                        
            except Exception as e:
                self.logger.debug(f"Error extracting company: {e}")
            
            # Extract structured job details using the HTML pattern
            try:
                detail_elements = job_card.query_selector_all('.job-searchResult-details')
                self.logger.debug(f"Found {len(detail_elements)} detail elements")
                
                for i, detail in enumerate(detail_elements):
                    detail_text = detail.text_content() or ""
                    self.logger.debug(f"Detail {i}: {detail_text}")
                    
                    # Extract each field based on the structure
                    # Handle both single-line "Field:Value" and multi-line formats
                    if 'Work Type:' in detail_text:
                        if ':' in detail_text:
                            value = detail_text.split(':', 1)[1].strip()
                            if value:
                                job_data['work_type'] = value
                                self.logger.debug(f"Extracted work_type: {job_data['work_type']}")
                    
                    elif 'Salary:' in detail_text:
                        if ':' in detail_text:
                            value = detail_text.split(':', 1)[1].strip()
                            if value:
                                job_data['salary'] = value
                                self.logger.debug(f"Extracted salary: {job_data['salary']}")
                    
                    elif 'Grade:' in detail_text:
                        if ':' in detail_text:
                            value = detail_text.split(':', 1)[1].strip()
                            if value:
                                job_data['grade'] = value
                                self.logger.debug(f"Extracted grade: {job_data['grade']}")
                    
                    elif 'Occupation:' in detail_text:
                        if ':' in detail_text:
                            value = detail_text.split(':', 1)[1].strip()
                            if value:
                                job_data['occupation'] = value
                                self.logger.debug(f"Extracted occupation: {job_data['occupation']}")
                    
                    elif 'Location:' in detail_text:
                        if ':' in detail_text:
                            value = detail_text.split(':', 1)[1].strip()
                            if value:
                                job_data['location'] = value
                                self.logger.debug(f"Extracted location: {job_data['location']}")
                    
                    elif 'Applications close:' in detail_text:
                        if ':' in detail_text:
                            value = detail_text.split(':', 1)[1].strip()
                            if value:
                                job_data['closing_date'] = value
                                self.logger.debug(f"Extracted closing_date: {job_data['closing_date']}")
                            
            except Exception as e:
                self.logger.debug(f"Error extracting structured details: {e}")
            
            # Extract brief description from listing page
            try:
                desc_element = job_card.query_selector('.job-searchResult-description')
                if desc_element:
                    job_data['brief_description'] = desc_element.text_content().strip()
            except Exception as e:
                self.logger.debug(f"Error extracting brief description: {e}")
            
            # Generate URL if none found
            if not job_data.get('url') and job_data.get('title'):
                title_slug = slugify(job_data['title'])
                job_data['url'] = f"{self.base_url}/job/{title_slug}"
            
            # Clean up extracted data
            for key, value in job_data.items():
                if isinstance(value, str):
                    job_data[key] = ' '.join(value.split())
            
            # Ensure we have at least a title
            if not job_data.get('title'):
                self.logger.warning("No title found in job card")
                return {}
            
            # Safety check: ensure title is not too long for database
            if len(job_data['title']) > 200:
                self.logger.warning(f"Job title too long ({len(job_data['title'])} chars): {job_data['title'][:100]}...")
                return {}
            
            # Debug: log all extracted fields
            self.logger.debug(f"All extracted fields: {job_data}")
            self.logger.info(f"Extracted job: {job_data.get('title', 'Unknown')} at {job_data.get('company', 'Unknown')}")
            if job_data.get('work_type'):
                self.logger.info(f"  Work Type: {job_data['work_type']}")
            if job_data.get('salary'):
                self.logger.info(f"  Salary: {job_data['salary']}")
            if job_data.get('location'):
                self.logger.info(f"  Location: {job_data['location']}")
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job info: {e}")
            return {}
    
    def fetch_detailed_description(self, job_url, page):
        """Fetch detailed job description from the job detail page."""
        try:
            self.logger.debug(f"Fetching detailed description from: {job_url}")
            
            # Navigate to job detail page
            response = page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            if not response.ok:
                self.logger.warning(f"Failed to load job detail page: {job_url}")
                return None
            
            # Wait for content to load
            page.wait_for_timeout(2000)
            
            # Extract the detailed description using the provided HTML structure
            try:
                # Look for the main description container
                desc_selectors = [
                    '.field--name-description .field__item',  # Based on provided HTML
                    '.text-formatted.field__item',
                    '.clearfix.text-formatted.field__item',
                    '.rpl-page-component .field__item',
                    '.field--type-text-with-summary .field__item'
                ]
                
                detailed_description = None
                for selector in desc_selectors:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        # Get the HTML content to preserve formatting
                        html_content = desc_element.inner_html()
                        # Also get text content as fallback
                        text_content = desc_element.text_content()
                        
                        if html_content and len(html_content.strip()) > 100:
                            # Convert HTML to clean text while preserving structure
                            detailed_description = self.clean_html_description(html_content)
                            self.logger.debug(f"Found detailed description using selector: {selector}")
                            break
                        elif text_content and len(text_content.strip()) > 100:
                            detailed_description = text_content.strip()
                            self.logger.debug(f"Found text description using selector: {selector}")
                            break
                
                if detailed_description:
                    self.logger.info(f"Successfully extracted detailed description ({len(detailed_description)} chars)")
                    return detailed_description
                else:
                    self.logger.warning("No detailed description found on job page")
                    return None
                    
            except Exception as e:
                self.logger.warning(f"Error extracting detailed description: {e}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching job detail page {job_url}: {e}")
            return None
    
    def clean_html_description(self, html_content):
        """Clean HTML content to create readable job description."""
        try:
            import re
            
            # Remove HTML tags but preserve structure
            # Convert common HTML elements to text equivalents
            html_content = re.sub(r'<h([1-6]).*?>', r'\n\n**', html_content)
            html_content = re.sub(r'</h[1-6]>', r'**\n', html_content)
            html_content = re.sub(r'<p.*?>', r'\n', html_content)
            html_content = re.sub(r'</p>', r'\n', html_content)
            html_content = re.sub(r'<li.*?>', r'\n• ', html_content)
            html_content = re.sub(r'</li>', r'', html_content)
            html_content = re.sub(r'<ul.*?>', r'\n', html_content)
            html_content = re.sub(r'</ul>', r'\n', html_content)
            html_content = re.sub(r'<ol.*?>', r'\n', html_content)
            html_content = re.sub(r'</ol>', r'\n', html_content)
            html_content = re.sub(r'<br.*?>', r'\n', html_content)
            html_content = re.sub(r'<strong.*?>', r'**', html_content)
            html_content = re.sub(r'</strong>', r'**', html_content)
            html_content = re.sub(r'<em.*?>', r'*', html_content)
            html_content = re.sub(r'</em>', r'*', html_content)
            
            # Remove remaining HTML tags
            html_content = re.sub(r'<[^>]+>', '', html_content)
            
            # Clean up whitespace
            html_content = re.sub(r'\n\s*\n', '\n\n', html_content)
            html_content = re.sub(r'^\s+|\s+$', '', html_content, flags=re.MULTILINE)
            html_content = html_content.strip()
            
            return html_content
            
        except Exception as e:
            self.logger.warning(f"Error cleaning HTML description: {e}")
            return html_content  # Return original if cleaning fails
    
    def save_job_to_database_sync(self, job_data):
        """Synchronous database save function."""
        try:
            # Close any existing connections
            connections.close_all()
            
            with transaction.atomic():
                # Enhanced duplicate detection
                job_url = job_data.get('url', '')
                job_title = job_data.get('title', '')
                company_name = job_data.get('company', 'Victorian Government')
                
                # Check for URL-based duplicate
                if job_url and JobPosting.objects.filter(external_url=job_url).exists():
                    self.logger.info(f"Duplicate job skipped (URL): {job_title}")
                    self.duplicates_found += 1
                    return False
                
                # Check for title + company duplicate
                if JobPosting.objects.filter(title=job_title, company__name=company_name).exists():
                    self.logger.info(f"Duplicate job skipped (Title+Company): {job_title}")
                    self.duplicates_found += 1
                    return False
                
                # Parse and create location
                location_name, city, state, country = self.parse_location(job_data.get('location', ''))
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
                company_slug = slugify(company_name)
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Victorian Government careers',
                        'website': self.base_url,
                        'company_size': 'enterprise'  # Government is enterprise size
                    }
                )
                
                # Parse salary
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary', '')
                )
                
                # Parse dates
                date_posted = None
                closing_date = self.parse_date(job_data.get('closing_date', ''))
                
                # Determine job type
                job_type = "full_time"  # Default
                work_type = job_data.get('work_type', '')
                if work_type:
                    work_type_lower = work_type.lower()
                    if 'part-time' in work_type_lower or 'part time' in work_type_lower:
                        job_type = "part_time"
                    elif 'casual' in work_type_lower:
                        job_type = "casual"
                    elif 'contract' in work_type_lower:
                        job_type = "contract"
                    elif 'temporary' in work_type_lower or 'fixed-term' in work_type_lower:
                        job_type = "temporary"
                
                # Determine job category
                job_category = self.determine_job_category(
                    job_data.get('title', ''),
                    job_data.get('occupation', ''),
                    company_name
                )
                
                # Create unique slug
                base_slug = slugify(job_data.get('title', 'job'))
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Use detailed description if available, otherwise build from metadata
                if job_data.get('description') and len(job_data['description']) > 50:
                    description = job_data['description']
                else:
                    # Fallback: create description from metadata
                    description_parts = []
                    if job_data.get('occupation'):
                        description_parts.append(f"Occupation: {job_data['occupation']}")
                    if job_data.get('grade'):
                        description_parts.append(f"Grade: {job_data['grade']}")
                    if job_data.get('work_type'):
                        description_parts.append(f"Work Type: {job_data['work_type']}")
                    
                    description = '. '.join(description_parts) if description_parts else job_data.get('title', '')
                
                # Create JobPosting
                job_posting = JobPosting.objects.create(
                    title=job_data.get('title', ''),
                    slug=unique_slug,
                    description=description,
                    company=company_obj,
                    posted_by=self.bot_user,
                    location=location_obj,
                    job_category=job_category,
                    job_type=job_type,
                    experience_level=job_data.get('grade', ''),
                    work_mode='',  # Not specified in VIC gov listings
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=currency,
                    salary_type=salary_type,
                    salary_raw_text=raw_text,
                    external_source='careers.vic.gov.au',
                    external_url=job_data.get('url', ''),
                    external_id=job_data.get('job_reference', ''),
                    status='active',
                    posted_ago='',  # Not provided in VIC listings
                    date_posted=date_posted,
                    tags=job_data.get('occupation', ''),
                    additional_info={
                        'grade': job_data.get('grade', ''),
                        'occupation': job_data.get('occupation', ''),
                        'closing_date': job_data.get('closing_date', ''),
                        'scraper_version': '1.0'
                    }
                )
                
                self.logger.info(f"Saved job: {job_posting.title} at {job_posting.company.name}")
                self.logger.info(f"  Category: {job_posting.job_category}")
                self.logger.info(f"  Location: {job_posting.location.name if job_posting.location else 'Not specified'}")
                self.logger.info(f"  Salary: {job_posting.salary_display}")
                
                self.jobs_saved += 1
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving job to database: {str(e)}")
            self.errors_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job data using thread-safe approach."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.save_job_to_database_sync, job_data)
            try:
                result = future.result(timeout=30)
                return result
            except concurrent.futures.TimeoutError:
                self.logger.error("Database save operation timed out")
                self.errors_count += 1
                return False
            except Exception as e:
                self.logger.error(f"Error in threaded database save: {str(e)}")
                self.errors_count += 1
                return False
    
    def scrape_page(self, page):
        """Scrape all job listings from the current page."""
        try:
            self.logger.info("Extracting job cards from page...")
            
            # Extract job cards
            job_cards = self.extract_job_cards(page)
            
            if not job_cards:
                self.logger.warning("No job cards found on page")
                return 0
            
            self.logger.info(f"Found {len(job_cards)} job cards")
            jobs_processed = 0
            
            # First pass: Extract basic job data from all cards
            jobs_data = []
            for i, job_card in enumerate(job_cards):
                try:
                    # Check job limit
                    if self.job_limit and len(jobs_data) >= self.job_limit:
                        self.logger.info(f"Reached job limit of {self.job_limit}")
                        break
                    
                    self.logger.info(f"Extracting basic data from job {i+1}/{len(job_cards)}")
                    
                    # Extract job data
                    job_data = self.extract_job_info(job_card)
                    
                    if not job_data.get('title'):
                        self.logger.warning(f"No title found for job {i+1}")
                        continue
                    
                    jobs_data.append(job_data)
                    
                except Exception as e:
                    self.logger.error(f"Error extracting basic data from job {i+1}: {str(e)}")
                    self.errors_count += 1
                    continue
            
            self.logger.info(f"Extracted {len(jobs_data)} jobs, now fetching detailed descriptions...")
            
            # Second pass: Fetch detailed descriptions and save to database
            for i, job_data in enumerate(jobs_data):
                try:
                    self.logger.info(f"Processing job {i+1}/{len(jobs_data)}: {job_data['title']}")
                    
                    # Fetch detailed description from job detail page [[memory:6698010]]
                    if job_data.get('url'):
                        self.logger.info(f"Fetching detailed description for: {job_data['title']}")
                        detailed_desc = self.fetch_detailed_description(job_data['url'], page)
                        if detailed_desc:
                            job_data['description'] = detailed_desc
                        else:
                            # Fallback to brief description if detailed fetch fails
                            job_data['description'] = job_data.get('brief_description', 'No description available')
                    else:
                        job_data['description'] = job_data.get('brief_description', 'No description available')
                    
                    # Save to database
                    if self.save_job_to_database(job_data):
                        jobs_processed += 1
                        self.jobs_scraped += 1
                    
                    # Human delay between jobs
                    self.human_delay(1, 2)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1}: {str(e)}")
                    self.errors_count += 1
                    continue
            
            return jobs_processed
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {str(e)}")
            return 0
    
    def run(self):
        """Main method to run the scraping process."""
        self.logger.info("Starting Victorian Government job scraper...")
        self.logger.info(f"Target URL: {self.search_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        if self.max_pages:
            self.logger.info(f"Max pages: {self.max_pages}")
        self.logger.info(f"Starting from page: {self.start_page}")
        
        with sync_playwright() as p:
            # Launch browser
            self.browser = p.chromium.launch(
                headless=True,  # Set to False for debugging
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            self.context = self.setup_browser_context(self.browser)
            self.page = self.context.new_page()
            
            try:
                # Navigate to search page
                self.logger.info("Navigating to Victorian Government careers page...")
                self.page.goto(self.search_url, wait_until='domcontentloaded', timeout=30000)
                
                # Wait for page to load completely
                self.human_delay(5, 8)
                
                # Take screenshot for debugging
                self.page.screenshot(path="vic_government_debug.png")
                self.logger.info("Screenshot saved as vic_government_debug.png")
                
                # Log page info
                self.logger.info(f"Page title: {self.page.title()}")
                self.logger.info(f"Page URL: {self.page.url}")
                
                # Handle any cookie banners
                try:
                    cookie_selectors = [
                        'button:has-text("Accept")',
                        'button:has-text("OK")',
                        'button:has-text("Agree")',
                        '.cookie-accept'
                    ]
                    
                    for selector in cookie_selectors:
                        button = self.page.query_selector(selector)
                        if button and button.is_visible():
                            self.logger.info(f"Clicking cookie button: {selector}")
                            button.click()
                            self.human_delay(2, 3)
                            break
                except Exception as e:
                    self.logger.debug(f"No cookie banner found: {e}")
                
                # Scroll to load content
                self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self.human_delay(3, 5)
                self.page.evaluate("window.scrollTo(0, 0)")
                self.human_delay(2, 3)
                
                # Implement pagination support
                self.logger.info("Scraping job listings with pagination...")
                
                current_page = self.start_page
                total_jobs_found = 0
                
                while True:
                    # Check if we've reached our page limit
                    if self.max_pages and (current_page - self.start_page + 1) > self.max_pages:
                        self.logger.info(f"Reached max pages limit of {self.max_pages}")
                        break
                    
                    # Check if we've reached our job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit of {self.job_limit}")
                        break
                    
                    # Navigate to current page
                    page_url = f"{self.search_url}&page={current_page - 1}"  # Pages are 0-indexed in URL
                    self.logger.info(f"Scraping page {current_page}: {page_url}")
                    
                    if current_page > self.start_page:
                        self.logger.info(f"Navigating to page {current_page}: {page_url}")
                        self.page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                        self.human_delay(3, 5)
                        
                        # Wait for new content to load
                        try:
                            self.page.wait_for_selector('.views-row', timeout=10000)
                        except:
                            self.logger.warning(f"Job cards not loaded on page {current_page}")
                    else:
                        # For the starting page, make sure we're on the right page
                        if self.start_page > 1:
                            start_page_url = f"{self.search_url}&page={self.start_page - 1}"
                            self.logger.info(f"Navigating to starting page {self.start_page}: {start_page_url}")
                            self.page.goto(start_page_url, wait_until='domcontentloaded', timeout=30000)
                            self.human_delay(3, 5)
                            
                            # Wait for new content to load
                            try:
                                self.page.wait_for_selector('.views-row', timeout=10000)
                            except:
                                self.logger.warning(f"Job cards not loaded on starting page {self.start_page}")
                    
                    # Scrape current page
                    jobs_found = self.scrape_page(self.page)
                    total_jobs_found += jobs_found
                    self.pages_scraped += 1
                    
                    if jobs_found == 0:
                        self.logger.warning(f"No jobs found on page {current_page}")
                        break
                    
                    # Check if this is the last page by looking for pagination
                    try:
                        # Check pagination text for total pages and current position
                        page_text = self.page.text_content()
                        if 'Displaying' in page_text:
                            import re
                            match = re.search(r'Displaying (\d+) to (\d+) of (\d+) results', page_text)
                            if match:
                                start_item = int(match.group(1))
                                end_item = int(match.group(2))
                                total_results = int(match.group(3))
                                
                                self.logger.info(f"Page {current_page}: Showing items {start_item}-{end_item} of {total_results} total")
                                
                                # Check if we've reached the end (end_item equals total_results)
                                if end_item >= total_results:
                                    self.logger.info(f"Reached last page ({current_page}) - showing items {end_item} of {total_results}")
                                    break
                                
                                # Estimate total pages
                                estimated_total_pages = (total_results + 14) // 15
                                self.logger.info(f"Estimated total pages: {estimated_total_pages}")
                                
                                # Safety check - if current page is reasonable for estimated total
                                if current_page >= estimated_total_pages:
                                    self.logger.info(f"Reached estimated last page ({current_page} of ~{estimated_total_pages})")
                                    break
                        
                        # Also look for "Next" button as secondary check
                        next_button = self.page.query_selector('button[aria-label="Go to next page"]')
                        if next_button and next_button.is_enabled():
                            self.logger.info(f"Next button available - continuing to page {current_page + 1}")
                        elif next_button:
                            self.logger.info(f"Next button disabled - reached last page ({current_page})")
                            break
                        else:
                            self.logger.debug("No next button found")
                        
                    except Exception as e:
                        self.logger.debug(f"Error checking pagination: {e}")
                    
                    current_page += 1
                    
                    # Add delay between pages
                    self.human_delay(2, 4)
                
                if total_jobs_found == 0:
                    self.logger.warning("No jobs found across all pages")
                
            except Exception as e:
                self.logger.error(f"Error during scraping: {str(e)}")
                
            finally:
                self.context.close()
                self.browser.close()
        
        # Print summary
        self.logger.info("=" * 60)
        self.logger.info("VICTORIAN GOVERNMENT SCRAPING SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Pages scraped: {self.pages_scraped}")
        self.logger.info(f"Jobs processed: {self.jobs_scraped}")
        self.logger.info(f"Jobs saved to database: {self.jobs_saved}")
        self.logger.info(f"Duplicates found: {self.duplicates_found}")
        self.logger.info(f"Errors encountered: {self.errors_count}")
        
        # Get total job count
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: JobPosting.objects.count())
                total_jobs_in_db = future.result(timeout=10)
                self.logger.info(f"Total job postings in database: {total_jobs_in_db}")
        except:
            self.logger.info("Total job postings in database: (count unavailable)")
        
        self.logger.info("=" * 60)


def main():
    """Main function to run the scraper."""
    print("🔍 Victorian Government Job Scraper")
    print("=" * 60)
    
    # Show usage if help requested
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        print("Usage: python vic_government_scraper_advanced.py [job_limit] [max_pages] [start_page]")
        print("")
        print("Arguments:")
        print("  job_limit   : Maximum number of jobs to scrape (optional)")
        print("  max_pages   : Maximum number of pages to scrape (optional)")
        print("  start_page  : Page number to start from (optional, default: 1)")
        print("")
        print("Examples:")
        print("  python vic_government_scraper_advanced.py              # Scrape all jobs from all pages")
        print("  python vic_government_scraper_advanced.py 50          # Scrape up to 50 jobs")
        print("  python vic_government_scraper_advanced.py 50 3        # Scrape up to 50 jobs from first 3 pages")
        print("  python vic_government_scraper_advanced.py 100 5 2     # Scrape up to 100 jobs from 5 pages starting from page 2")
        return
    
    # Parse command line arguments
    job_limit = None
    max_pages = None
    start_page = 1
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            print("Invalid job limit. Using no limit.")
    
    if len(sys.argv) > 2:
        try:
            max_pages = int(sys.argv[2])
        except ValueError:
            print("Invalid max pages. Using no limit.")
    
    if len(sys.argv) > 3:
        try:
            start_page = int(sys.argv[3])
        except ValueError:
            print("Invalid start page. Using page 1.")
    
    print(f"Target: Victorian Government careers (careers.vic.gov.au)")
    print(f"Job limit: {job_limit or 'No limit'}")
    if max_pages:
        print(f"Max pages: {max_pages}")
    print(f"Start page: {start_page}")
    print(f"Database: Professional structure with JobPosting, Company, Location")
    print("=" * 60)
    
    # Create scraper instance
    scraper = VictorianGovernmentJobScraper(
        job_limit=job_limit,
        max_pages=max_pages,
        start_page=start_page
    )
    
    try:
        # Run the scraping process
        scraper.run()
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"Scraping failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
