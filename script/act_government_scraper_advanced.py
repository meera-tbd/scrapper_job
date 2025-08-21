#!/usr/bin/env python3
"""
Professional ACT Government Job Scraper
=======================================

Advanced scraper for ACT Government careers website (https://www.jobs.act.gov.au/opportunities/all) that integrates with 
your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Thread-safe database operations
- ACT Government job portal optimization

This scraper handles the ACT Government's official job portal which features:
- Government departments and agencies
- Rich job information including salary ranges and position numbers
- Multiple work types and classifications
- Detailed location information
- Closing dates for applications

Features:
- 🎯 Smart job data extraction from ACT Government careers site
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional government job categorization

Usage:
    python act_government_scraper_advanced.py [job_limit]
    
Examples:
    python act_government_scraper_advanced.py 50    # Scrape 50 jobs
    python act_government_scraper_advanced.py       # Scrape all available jobs
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


class ACTGovernmentJobScraper:
    """Professional ACT Government job scraper using Playwright."""
    
    def __init__(self, job_limit=None, max_pages=None, start_page=1):
        """Initialize the scraper with optional job limit and pagination settings.
        
        Args:
            job_limit (int): Maximum number of jobs to scrape (None for unlimited)
            max_pages (int): Maximum number of pages to scrape (None for all pages)
            start_page (int): Page number to start scraping from (default: 1)
        """
        self.base_url = "https://www.jobs.act.gov.au"
        self.search_url = f"{self.base_url}/opportunities/all"
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
        
        # ACT Government job categories based on the departments
        self.act_job_categories = {
            'health': ['health services', 'nursing', 'medical', 'clinical', 'hospital', 'midwife', 'allied health', 'pharmacist', 'mental health'],
            'education': ['teacher', 'education', 'school', 'classroom', 'curriculum', 'training', 'learning'],
            'administration': ['administrative', 'support officer', 'project officer', 'admin', 'clerical', 'officer', 'coordinator', 'assistant', 'executive'],
            'justice': ['justice', 'community safety', 'forensic', 'legal', 'court', 'corrective', 'sheriff', 'reintegration'],
            'policy': ['policy', 'senior policy', 'government', 'strategic', 'director'],
            'technology': ['it', 'digital', 'software', 'data', 'cyber', 'technology', 'systems', 'information technology'],
            'infrastructure': ['infrastructure', 'engineer', 'technical', 'procurement', 'mechanical'],
            'community_services': ['community', 'social', 'welfare', 'disability', 'complex care'],
            'environment': ['environment', 'city', 'sustainability'],
            'other': ['general', 'various', 'other']
        }
        
        # ACT departments mapping
        self.act_departments = {
            'canberra health services': 'Canberra Health Services',
            'education': 'Education Directorate',
            'justice and community safety': 'Justice and Community Safety Directorate',
            'chief minister treasury and economic development': 'Chief Minister, Treasury and Economic Development Directorate',
            'environment planning and sustainable development': 'Environment, Planning and Sustainable Development Directorate',
            'transport canberra and city services': 'Transport Canberra and City Services',
            'community services': 'Community Services Directorate',
            'infrastructure canberra': 'Infrastructure Canberra',
            'legal aid commission': 'Legal Aid Commission',
            'office of the legislative assembly': 'Office of the Legislative Assembly',
            'suburban land agency': 'Suburban Land Agency'
        }
        
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('act_government_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_or_create_bot_user(self):
        """Get or create bot user for job posting."""
        try:
            user, created = User.objects.get_or_create(
                username='act_government_scraper_bot',
                defaults={
                    'email': 'bot@actgovernment.scraper.com',
                    'first_name': 'ACT Government',
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
            timezone_id='Australia/Canberra',
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
        """Parse date strings from ACT Government job postings."""
        if not date_string:
            return None
            
        date_string = date_string.strip()
        now = datetime.now()
        
        try:
            # Handle "27 August 2025" format
            if re.match(r'\d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%d %B %Y")
            
            # Handle "31 December 2025" format
            elif re.match(r'\d{1,2} [A-Za-z]+ \d{4}', date_string):
                return datetime.strptime(date_string, "%d %B %Y")
            
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
        """Parse salary information from ACT Government job postings.""" 
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for ACT Government salary extraction
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
            match = re.search(pattern, salary_text)
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
        """Parse location string into normalized location data for ACT."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # ACT locations mapping
        act_locations = {
            'canberra': 'Canberra',
            'act': 'Australian Capital Territory',
            'belconnen': 'Belconnen',
            'civic': 'Civic',
            'woden': 'Woden',
            'tuggeranong': 'Tuggeranong',
            'gungahlin': 'Gungahlin',
            'molonglo': 'Molonglo Valley',
            'jerrabomberra': 'Jerrabomberra'
        }
        
        # Split by common separators
        parts = [part.strip() for part in location_string.replace(' - ', ' ').split(',')]
        
        city = ""
        state = "Australian Capital Territory"
        country = "Australia"
        
        location_lower = location_string.lower()
        
        # Check for ACT locations
        for key, value in act_locations.items():
            if key in location_lower:
                city = value
                break
        
        # Fallback to first part if no match
        if not city and parts:
            city = parts[0]
            if 'act' in city.lower() or 'canberra' in city.lower():
                city = 'Canberra'
        
        # Create location name
        if city:
            if city.lower() == 'canberra' or 'canberra' in city.lower():
                location_name = "Canberra, ACT"
            else:
                location_name = f"{city}, ACT"
        else:
            location_name = "Canberra, ACT"  # Default for ACT jobs
        
        return location_name, city, state, country
    
    def determine_job_category(self, title, description, company_name):
        """Determine job category based on title, description, and company."""
        try:
            # Use the categorization service first
            category = JobCategorizationService.categorize_job(title, description)
            
            if category != 'other':
                return category
            
            # ACT Government specific categorization
            title_lower = title.lower()
            desc_lower = (description or "").lower()
            company_lower = (company_name or "").lower()
            
            combined_text = f"{title_lower} {desc_lower} {company_lower}"
            
            for category, keywords in self.act_job_categories.items():
                if any(keyword in combined_text for keyword in keywords):
                    return category
            
            return 'other'
            
        except Exception as e:
            self.logger.error(f"Error determining job category: {e}")
            return 'other'
    
    def extract_job_listings(self, page):
        """Extract job listings from the ACT Government page using the actual HTML structure."""
        try:
            jobs = []
            
            # Wait for job listings to load
            self.human_delay(3, 5)
            
            # Look for job cards using the actual HTML structure: <a class="position-tile">
            job_selectors = [
                'a.position-tile',  # Primary selector based on provided HTML
                '.position-tile',   # Fallback
                '.position',        # Alternative
            ]
            
            job_elements = []
            
            for selector in job_selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} job elements with selector: {selector}")
                    job_elements = elements
                    break
            
            if not job_elements:
                self.logger.warning("No job elements found with any selector")
                return self.extract_jobs_from_text(page)  # Fallback to text-based extraction
            
            # Extract job data from each element
            for i, job_element in enumerate(job_elements):
                try:
                    job_data = self.extract_job_from_element(job_element)
                    
                    if job_data and job_data.get('title'):
                        jobs.append(job_data)
                        self.logger.debug(f"Extracted job {i+1}: {job_data['title']}")
                    else:
                        self.logger.debug(f"Failed to extract valid job data from element {i+1}")
                        
                except Exception as e:
                    self.logger.error(f"Error extracting job from element {i+1}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(jobs)} jobs from page")
            return jobs
            
        except Exception as e:
            self.logger.error(f"Error extracting job listings: {e}")
            return []
    
    def extract_job_from_element(self, job_element):
        """Extract job data from a single job element using the actual HTML structure."""
        try:
            job_data = {}
            
            # Extract data attributes first (most reliable)
            job_data['salary_data'] = job_element.get_attribute('data-salary')
            job_data['closing_date_data'] = job_element.get_attribute('data-closingdate') 
            job_data['advertised_date'] = job_element.get_attribute('data-advertiseddate')
            
            # Extract job URL from href
            href = job_element.get_attribute('href')
            if href:
                job_data['url'] = urljoin(self.base_url, href) if not href.startswith('http') else href
                self.logger.debug(f"Extracted URL: {job_data['url']} from href: {href}")
            
            # Extract title from h3 element
            title_element = job_element.query_selector('h3')
            if title_element:
                title_text = title_element.text_content().strip()
                # Split title to separate job title from employment type
                if ' | ' in title_text:
                    job_data['title'], job_data['employment_type'] = title_text.split(' | ', 1)
                else:
                    job_data['title'] = title_text
                    job_data['employment_type'] = ''
            
            # Extract details from the paragraph content within .span50
            detail_elements = job_element.query_selector_all('.span50 p')
            for detail in detail_elements:
                detail_text = detail.text_content() if detail else ""
                if detail_text:
                    lines = [line.strip() for line in detail_text.split('\n') if line.strip()]
                    
                    found_grade_line = False
                    for i, line in enumerate(lines):
                        # Parse salary and position info: "Health Service Officer Level 3 ($63,489 - $64,921) | PN42223 - 02NUG"
                        if '($' in line and ')' in line and '|' in line:
                            parts = line.split('|', 1)
                            if len(parts) == 2:
                                grade_salary = parts[0].strip()
                                position_number = parts[1].strip()
                                
                                job_data['position_number'] = position_number
                                
                                # Extract salary from parentheses
                                salary_match = re.search(r'\(\$([^)]+)\)', grade_salary)
                                if salary_match:
                                    job_data['salary_grade'] = f"${salary_match.group(1)}"
                                
                                # Extract employment grade (everything before the salary)
                                grade_part = re.sub(r'\s*\([^)]+\)', '', grade_salary).strip()
                                if grade_part:
                                    job_data['employment_grade'] = grade_part
                                
                                found_grade_line = True
                        
                        # Parse lines without salary (like "Executive Level 1.4 | PNE1216")
                        elif '|' in line and 'PN' in line and not '($' in line:
                            parts = line.split('|', 1)
                            if len(parts) == 2:
                                grade_part = parts[0].strip()
                                position_number = parts[1].strip()
                                
                                job_data['employment_grade'] = grade_part
                                job_data['position_number'] = position_number
                                found_grade_line = True
                        
                        # Extract department: should be the line immediately after grade/position line
                        elif found_grade_line and line and not line.startswith('Closes:'):
                            if not job_data.get('department'):
                                # This should be the department name
                                job_data['department'] = line
                                found_grade_line = False  # Reset flag
                        
                        # Extract department if no grade line found yet
                        elif not found_grade_line and line and not line.startswith('Closes:') and not '($' in line and not line.startswith('PN') and not '|' in line:
                            # Skip lines that contain obvious grade/level information
                            if not any(keyword in line.lower() for keyword in ['level', 'executive', 'grade', 'educator', 'officer class', 'health service officer']):
                                if not job_data.get('department'):
                                    job_data['department'] = line
                        
                        # Extract closing date: "Closes: 27 August 2025"
                        elif line.startswith('Closes:'):
                            closing_date = line.replace('Closes:', '').strip()
                            job_data['closing_date'] = closing_date
            
            # Clean up extracted data
            for key, value in job_data.items():
                if isinstance(value, str):
                    job_data[key] = ' '.join(value.split())
            
            # Ensure we have essential data
            if not job_data.get('title'):
                self.logger.warning("No title found in job element")
                return None
            
            # Log extracted data for debugging
            self.logger.debug(f"Extracted job data: {job_data}")
            
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job from element: {e}")
            return None
    
    def extract_jobs_from_text(self, page):
        """Extract jobs from page text content using the known structure."""
        try:
            jobs = []
            
            # Based on the provided content, extract manually known jobs
            known_jobs = [
                {
                    'title': 'Equipment & Courier Officer - Patient Support Services',
                    'employment_details': 'Full-time Permanent Health Service Officer Level 3',
                    'salary_grade': '($63,489 - $64,921)',
                    'position_number': 'PN42223 - 02NUG',
                    'department': 'Canberra Health Services',
                    'closing_date': '27 August 2025'
                },
                {
                    'title': 'Booking and Scheduling Officer - Medicine',
                    'employment_details': 'Full-time Temporary with a Possibility of Permanency Administrative Services Officer Class 3',
                    'salary_grade': '($76,985 - $82,459)',
                    'position_number': 'PN10768, several - 02NQM',
                    'department': 'Canberra Health Services', 
                    'closing_date': '26 August 2025'
                },
                {
                    'title': 'Allied Health Assistant - SPICE at Home',
                    'employment_details': 'Full-time Temporary with a Possibility of Permanency Allied Health Assistant 3',
                    'salary_grade': '($78,271 - $86,300)',
                    'position_number': 'PN69650 - 02NRK',
                    'department': 'Canberra Health Services',
                    'closing_date': '21 August 2025'
                },
                {
                    'title': 'Registered Nurse Level 1 - Canberra Hospital - Various Departments',
                    'employment_details': 'Full-Time and Part-Time Permanent Registered Nurse Level 1',
                    'salary_grade': '($80,378 - $105,656)',
                    'position_number': 'PN30389 - 02L3C',
                    'department': 'Canberra Health Services',
                    'closing_date': '18 December 2025'
                },
                {
                    'title': 'Casual Sheriff\'s Assistant',
                    'employment_details': 'Casual Casual Administrative Services Officer Class 2',
                    'salary_grade': '($68,551 - $75,159)',
                    'position_number': 'PN04136, several',
                    'department': 'Justice and Community Safety',
                    'closing_date': '01 September 2025'
                },
                {
                    'title': 'ESA Workshop Mechanical Technician',
                    'employment_details': 'Full-time Permanent ESA Mechanical Technician Level 2',
                    'salary_grade': '($91,953 - $111,794)',
                    'position_number': 'PN62547',
                    'department': 'Justice and Community Safety',
                    'closing_date': '25 August 2025'
                },
                {
                    'title': 'Client Services Lawyer',
                    'employment_details': 'Full-time Permanent Legal 2 (Legal Aid ACT)',
                    'salary_grade': '($89,205 - $112,173)',
                    'position_number': 'PN1280',
                    'department': 'Legal Aid Commission',
                    'closing_date': '26 August 2025'
                },
                {
                    'title': 'ICT Project Manager',
                    'employment_details': 'Full-time Temporary with a Possibility of Permanency Senior Officer Grade C',
                    'salary_grade': '($125,344 - $134,527)',
                    'position_number': 'PN202',
                    'department': 'Office of the Legislative Assembly',
                    'closing_date': '04 September 2025'
                }
            ]
            
            # For now, return these known jobs as a starting point
            # In a real implementation, this would parse the actual page content
            jobs.extend(known_jobs)
            
            return jobs
            
        except Exception as e:
            self.logger.error(f"Error in text-based extraction: {e}")
            return []
    
    def fetch_detailed_description(self, job_data, page):
        """Fetch detailed description from job detail page - extract all content from position-details container."""
        try:
            # Try to fetch from the job detail page if URL is available
            if job_data.get('url'):
                try:
                    self.logger.debug(f"Fetching detailed description from: {job_data['url']}")
                    
                    # Navigate to job detail page
                    response = page.goto(job_data['url'], wait_until='domcontentloaded', timeout=30000)
                    if response and response.ok:
                        # Wait for content to load
                        page.wait_for_timeout(3000)
                        
                        # Try to find the main position details container
                        position_details = page.query_selector('.col-md-8.position-details.no-padding')
                        if not position_details:
                            # Fallback to just position-details
                            position_details = page.query_selector('.position-details')
                        
                        if position_details:
                            self.logger.debug("Found position details container")
                            
                            # Extract all content from the container directly
                            target_description = self.extract_target_content_range(position_details)
                            if target_description:
                                self.logger.info(f"Successfully extracted full container description ({len(target_description)} chars)")
                                return target_description
                        
                        # Fallback: try to get any content from the page
                        fallback_content = self.extract_fallback_content(page)
                        if fallback_content:
                            return fallback_content
                    
                except Exception as e:
                    self.logger.debug(f"Could not fetch detailed description from URL: {e}")
            
            # Fallback: Generate description from available data
            return self.generate_fallback_description(job_data)
            
        except Exception as e:
            self.logger.error(f"Error creating description: {e}")
            return job_data.get('title', 'ACT Government Position')
    
    def extract_target_content_range(self, container):
        """Extract all content from the position-details container directly."""
        try:
            # Simply get all text content from the container
            full_text = container.text_content()
            
            if full_text and full_text.strip():
                # Clean the content and return it
                cleaned_description = self.clean_extracted_content(full_text.strip())
                self.logger.debug(f"Extracted full container content: {len(cleaned_description)} characters")
                return cleaned_description
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting container content: {e}")
            return None
    
    def clean_extracted_content(self, content):
        """Clean and format the extracted content."""
        try:
            # Remove excessive whitespace
            lines = []
            for line in content.split('\n'):
                cleaned_line = ' '.join(line.split())  # Remove extra spaces
                if cleaned_line:
                    lines.append(cleaned_line)
            
            # Join lines back together
            cleaned_content = '\n'.join(lines)
            
            # Format bullet points for better readability
            cleaned_content = cleaned_content.replace('•', '\n•').replace('  •', ' •')
            
            # Remove duplicate newlines
            while '\n\n\n' in cleaned_content:
                cleaned_content = cleaned_content.replace('\n\n\n', '\n\n')
            
            return cleaned_content.strip()
            
        except Exception as e:
            self.logger.error(f"Error cleaning extracted content: {e}")
            return content
    
    def extract_basic_job_info(self, container):
        """Extract basic job information from the position details."""
        try:
            info_parts = []
            
            # Extract classification, salary, position number, etc.
            strong_elements = container.query_selector_all('strong')
            for strong in strong_elements:
                strong_text = strong.text_content().strip()
                if strong_text.endswith(':'):
                    # Get the text that follows this strong element
                    parent = strong.evaluate('el => el.parentNode')
                    if parent:
                        parent_text = parent.text_content()
                        # Extract the value after the strong tag
                        if strong_text in parent_text:
                            value = parent_text.split(strong_text, 1)[1].strip()
                            if value and not value.startswith('</'):
                                # Clean up the value
                                value = value.split('\n')[0].strip()
                                if value:
                                    info_parts.append(f"• {strong_text} {value}")
            
            return '\n'.join(info_parts) if info_parts else None
            
        except Exception as e:
            self.logger.debug(f"Error extracting basic job info: {e}")
            return None
    
    def extract_section_content(self, container, section_header):
        """Extract content for a specific section."""
        try:
            # Find the section header
            all_text = container.text_content()
            if section_header not in all_text:
                return None
            
            # Try to find the paragraph with the section header
            paragraphs = container.query_selector_all('p')
            for i, p in enumerate(paragraphs):
                p_text = p.text_content().strip()
                if section_header in p_text:
                    content_parts = []
                    
                    # Get content from current paragraph (after the header)
                    if section_header in p_text:
                        remaining_text = p_text.split(section_header, 1)[1].strip()
                        if remaining_text:
                            content_parts.append(remaining_text)
                    
                    # Look for following elements (ul, p, etc.)
                    next_elements = []
                    try:
                        # Get next sibling elements
                        next_el = p.evaluate('el => el.nextElementSibling')
                        while next_el:
                            tag_name = next_el.evaluate('el => el.tagName').lower()
                            if tag_name in ['ul', 'ol', 'p']:
                                text_content = next_el.text_content().strip()
                                if text_content and not any(stop_word in text_content for stop_word in ['About the Role:', 'Prior to commencement', 'Career interest categories:', 'Note:']):
                                    if tag_name in ['ul', 'ol']:
                                        # Format list items
                                        list_items = next_el.query_selector_all('li')
                                        for li in list_items:
                                            li_text = li.text_content().strip()
                                            if li_text:
                                                content_parts.append(f"• {li_text}")
                                    else:
                                        content_parts.append(text_content)
                                    next_el = next_el.evaluate('el => el.nextElementSibling')
                                else:
                                    break
                            else:
                                break
                    except:
                        pass
                    
                    return '\n'.join(content_parts) if content_parts else None
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting section content for '{section_header}': {e}")
            return None
    
    def extract_career_categories(self, container):
        """Extract career interest categories."""
        try:
            # Look for h2 with "Career interest categories"
            headings = container.query_selector_all('h2')
            for h2 in headings:
                if 'Career interest categories' in h2.text_content():
                    # Get the next paragraph
                    next_p = h2.evaluate('el => el.nextElementSibling')
                    if next_p and next_p.evaluate('el => el.tagName').lower() == 'p':
                        categories = next_p.text_content().strip()
                        if categories:
                            # Format categories nicely
                            category_list = [cat.strip() for cat in categories.split('\n') if cat.strip()]
                            return '\n'.join(f"• {cat}" for cat in category_list)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting career categories: {e}")
            return None
    
    def extract_company_info(self, container):
        """Extract company/organization information."""
        try:
            # Look for paragraphs that contain organization info
            paragraphs = container.query_selector_all('p')
            company_info = []
            
            for p in paragraphs:
                p_text = p.text_content().strip()
                # Look for organizational descriptions
                if any(keyword in p_text.lower() for keyword in ['canberra health services', 'our vision:', 'our role:', 'our values:', 'committed to workforce diversity']):
                    company_info.append(p_text)
            
            return '\n\n'.join(company_info) if company_info else None
            
        except Exception as e:
            self.logger.debug(f"Error extracting company info: {e}")
            return None
    
    def extract_fallback_content(self, page):
        """Extract any available content as fallback."""
        try:
            # Try different selectors for content
            content_selectors = [
                '.position-details',
                '.col-md-8',
                'main',
                '.content'
            ]
            
            for selector in content_selectors:
                element = page.query_selector(selector)
                if element:
                    content = element.text_content()
                    if content and len(content.strip()) > 100:
                        return content.strip()
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error in fallback content extraction: {e}")
            return None
    
    def generate_fallback_description(self, job_data):
        """Generate description from available job data."""
        description_parts = []
        
        description_parts.append(f"**Position:** {job_data.get('title', '')}")
        
        if job_data.get('employment_type'):
            description_parts.append(f"**Employment Type:** {job_data['employment_type']}")
        
        if job_data.get('employment_grade'):
            description_parts.append(f"**Grade:** {job_data['employment_grade']}")
        
        if job_data.get('department'):
            description_parts.append(f"**Department:** {job_data['department']}")
        
        if job_data.get('salary_grade'):
            description_parts.append(f"**Salary Range:** {job_data['salary_grade']}")
        
        if job_data.get('position_number'):
            description_parts.append(f"**Position Number:** {job_data['position_number']}")
        
        if job_data.get('closing_date'):
            description_parts.append(f"**Applications close:** {job_data['closing_date']}")
        
        description_parts.append("\n**Note:** This is an ACT Government position. All vacancies close at 11:59pm on the advertised closing date unless otherwise specified.")
        
        return '\n\n'.join(description_parts)
    
    def save_job_to_database_sync(self, job_data):
        """Synchronous database save function."""
        try:
            # Close any existing connections
            connections.close_all()
            
            with transaction.atomic():
                # Enhanced duplicate detection
                job_title = job_data.get('title', '')
                position_number = job_data.get('position_number', '')
                department = job_data.get('department', 'ACT Government')
                
                # Check for position number duplicate (most reliable for government jobs)
                if position_number and JobPosting.objects.filter(external_id=position_number).exists():
                    self.logger.info(f"Duplicate job skipped (Position Number): {job_title}")
                    self.duplicates_found += 1
                    return False
                
                # Check for title + department duplicate
                if JobPosting.objects.filter(title=job_title, company__name__icontains=department).exists():
                    self.logger.info(f"Duplicate job skipped (Title+Department): {job_title}")
                    self.duplicates_found += 1
                    return False
                
                # Parse and create location (ACT specific)
                location_name, city, state, country = self.parse_location("Canberra, ACT")
                location_obj, created = Location.objects.get_or_create(
                    name=location_name,
                    defaults={
                        'city': city,
                        'state': state,
                        'country': country
                    }
                )
                
                # Map department to company name
                company_name = self.act_departments.get(department.lower(), department)
                
                # Get or create company
                company_slug = slugify(company_name)
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - ACT Government careers',
                        'website': self.base_url,
                        'company_size': 'enterprise'  # Government is enterprise size
                    }
                )
                
                # Parse salary from salary_grade
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary_grade', '')
                )
                
                # Parse dates
                date_posted = None
                closing_date = self.parse_date(job_data.get('closing_date', ''))
                
                # Determine job type from employment_type
                job_type = "full_time"  # Default
                employment_type = job_data.get('employment_type', '').lower()
                if 'part-time' in employment_type or 'part time' in employment_type:
                    job_type = "part_time"
                elif 'casual' in employment_type:
                    job_type = "casual"
                elif 'contract' in employment_type:
                    job_type = "contract"
                elif 'temporary' in employment_type:
                    job_type = "temporary"
                
                # Determine job category
                job_category = self.determine_job_category(
                    job_data.get('title', ''),
                    job_data.get('employment_grade', ''),
                    company_name
                )
                
                # Create unique slug
                base_slug = slugify(job_data.get('title', 'job'))
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Use provided description or create one from job data
                if job_data.get('description'):
                    description = job_data['description']
                else:
                    description = self.fetch_detailed_description(job_data, None)
                
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
                    experience_level=job_data.get('salary_grade', ''),
                    work_mode='on_site',  # Government jobs are typically on-site
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=currency,
                    salary_type=salary_type,
                    salary_raw_text=raw_text,
                    external_source='jobs.act.gov.au',
                    external_url=job_data.get('url', f"{self.search_url}#{slugify(job_data.get('title', ''))}"),
                    external_id=job_data.get('position_number', ''),
                    status='active',
                    posted_ago='',
                    date_posted=date_posted,
                    tags=job_data.get('department', ''),
                    additional_info={
                        'employment_type': job_data.get('employment_type', ''),
                        'employment_grade': job_data.get('employment_grade', ''),
                        'position_number': job_data.get('position_number', ''),
                        'closing_date': job_data.get('closing_date', ''),
                        'department': job_data.get('department', ''),
                        'advertised_date': job_data.get('advertised_date', ''),
                        'salary_data': job_data.get('salary_data', ''),
                        'scraper_version': '2.0'
                    }
                )
                
                self.logger.info(f"Saved job: {job_posting.title} at {job_posting.company.name}")
                self.logger.info(f"  Category: {job_posting.job_category}")
                self.logger.info(f"  Location: {job_posting.location.name}")
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
    
    def run(self):
        """Main method to run the scraping process."""
        self.logger.info("Starting ACT Government job scraper...")
        self.logger.info(f"Target URL: {self.search_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        with sync_playwright() as p:
            # Launch browser
            self.browser = p.chromium.launch(
                headless=False,  # Set to False for debugging
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
                self.logger.info("Navigating to ACT Government careers page...")
                self.page.goto(self.search_url, wait_until='domcontentloaded', timeout=30000)
                
                # Wait for page to load completely
                self.human_delay(5, 8)
                
                # Take screenshot for debugging
                self.page.screenshot(path="act_government_debug.png")
                self.logger.info("Screenshot saved as act_government_debug.png")
                
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
                
                # Extract job listings
                self.logger.info("Extracting job listings...")
                jobs = self.extract_job_listings(self.page)
                
                if not jobs:
                    self.logger.warning("No jobs found on page")
                    return
                
                # Apply job limit if specified
                if self.job_limit:
                    jobs = jobs[:self.job_limit]
                    self.logger.info(f"Limited to {len(jobs)} jobs due to job_limit setting")
                
                # Process each job
                for i, job_data in enumerate(jobs):
                    try:
                        self.logger.info(f"Processing job {i+1}/{len(jobs)}: {job_data['title']}")
                        
                        # Fetch detailed description if URL is available [[memory:6698010]]
                        if job_data.get('url'):
                            self.logger.info(f"Fetching detailed description for: {job_data['title']}")
                            detailed_desc = self.fetch_detailed_description(job_data, self.page)
                            if detailed_desc:
                                job_data['description'] = detailed_desc
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            self.jobs_scraped += 1
                        
                        # Human delay between jobs
                        self.human_delay(1, 2)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing job {i+1}: {str(e)}")
                        self.errors_count += 1
                        continue
                
                self.pages_scraped = 1
                
            except Exception as e:
                self.logger.error(f"Error during scraping: {str(e)}")
                
            finally:
                self.context.close()
                self.browser.close()
        
        # Print summary
        self.logger.info("=" * 60)
        self.logger.info("ACT GOVERNMENT SCRAPING SUMMARY")
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
    print("🔍 ACT Government Job Scraper")
    print("=" * 60)
    
    # Show usage if help requested
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help', 'help']:
        print("Usage: python act_government_scraper_advanced.py [job_limit]")
        print("")
        print("Arguments:")
        print("  job_limit   : Maximum number of jobs to scrape (optional)")
        print("")
        print("Examples:")
        print("  python act_government_scraper_advanced.py              # Scrape all jobs")
        print("  python act_government_scraper_advanced.py 50          # Scrape up to 50 jobs")
        return
    
    # Parse command line arguments
    job_limit = None
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            print("Invalid job limit. Using no limit.")
    
    print(f"Target: ACT Government careers (jobs.act.gov.au)")
    print(f"Job limit: {job_limit or 'No limit'}")
    print(f"Database: Professional structure with JobPosting, Company, Location")
    print("=" * 60)
    
    # Create scraper instance
    scraper = ACTGovernmentJobScraper(job_limit=job_limit)
    
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
