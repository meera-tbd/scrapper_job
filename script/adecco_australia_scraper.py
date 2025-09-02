#!/usr/bin/env python
"""
Professional Adecco Australia Job Scraper using Playwright

This script scrapes job listings from adecco.com/en-au/jobs using a robust approach
that handles pagination, job details, and comprehensive data extraction.

Features:
- Professional database structure integration
- Comprehensive job categorization using Adecco's industry classifications
- Human-like behavior to avoid detection
- Complete pagination handling with dynamic page detection
- Advanced salary and location extraction
- Robust error handling and logging

Usage:
    python adecco_australia_scraper.py [max_jobs]

Example:
    python adecco_australia_scraper.py 100
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
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError  # [cursor:reason] Catch navigation timeout

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('adecco_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class AdeccoAustraliaJobScraper:
    """Professional scraper for adecco.com/en-au/jobs job listings."""
    
    def __init__(self, max_jobs=None, headless=True):
        """
        Initialize the scraper.
        
        Args:
            max_jobs (int): Maximum number of jobs to scrape
            headless (bool): Whether to run browser in headless mode
        """
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.adecco.com"
        self.jobs_url = "https://www.adecco.com/en-au/jobs"
        self.scraped_jobs = []
        self.processed_urls = set()
        
        # Job categories mapped to Django model choices
        self.adecco_categories = {
            'technology': ['it', 'technology', 'software', 'developer', 'programmer', 'system', 'ux', 'ui', 'designer'],
            'finance': ['banking', 'financial', 'accounting', 'finance', 'audit', 'payroll'],
            'healthcare': ['healthcare', 'medical', 'nursing', 'clinical', 'patient', 'hospital'],
            'marketing': ['marketing', 'advertising', 'brand', 'digital', 'social media'],
            'sales': ['sales', 'business development', 'account', 'commercial'],
            'hr': ['human resources', 'hr', 'recruitment', 'talent', 'people'],
            'education': ['education', 'teaching', 'training', 'learning', 'academic'],
            'retail': ['retail', 'shop', 'store', 'customer service'],
            'hospitality': ['hospitality', 'hotel', 'restaurant', 'events', 'catering', 'tourism', 'food', 'beverages'],
            'construction': ['construction', 'infrastructure', 'building', 'project', 'site', 'civil'],
            'manufacturing': ['manufacturing', 'production', 'factory', 'assembly', 'machining', 'cnc', 'operator'],
            'consulting': ['consultant', 'analyst', 'advisor', 'business', 'professional'],
            'legal': ['legal', 'lawyer', 'paralegal', 'compliance'],
            'other': ['admin', 'office', 'reception', 'clerk', 'administrative', 'secretary', 'defence', 'defense', 'military', 'security', 'government', 'energy', 'utilities', 'power', 'oil', 'gas', 'renewable', 'mining', 'resources', 'extraction', 'drilling', 'geology', 'property', 'facilities', 'maintenance', 'real estate', 'transport', 'logistics', 'warehouse', 'driver', 'delivery', 'supply chain']
        }
        
        # Get or create the scraper user
        self.scraper_user = self._get_or_create_scraper_user()
        
        logger.info(f"Adecco Australia scraper initialized. Max jobs: {max_jobs or 'No limit'}")
    
    def _get_or_create_scraper_user(self):
        """Get or create the system user for scraped jobs."""
        user, created = User.objects.get_or_create(
            username='adecco_scraper',
            defaults={
                'email': 'scraper@adecco.system',
                'first_name': 'Adecco',
                'last_name': 'Scraper',
                'is_staff': False,
                'is_active': False,
            }
        )
        if created:
            logger.info("Created new scraper user: adecco_scraper")
        return user
    
    def _categorize_job(self, title, description, categories_text):
        """
        Categorize job based on title, description, and Adecco categories.
        
        Args:
            title (str): Job title
            description (str): Job description
            categories_text (str): Raw categories text from Adecco
            
        Returns:
            str: Job category
        """
        text = f"{title} {description} {categories_text}".lower()
        
        # First try to match Adecco's own categories
        if categories_text:
            categories_lower = categories_text.lower()
            
            # Map Adecco categories to Django model categories
            category_mapping = {
                'admin and office support': 'other',
                'banking & financial services': 'finance', 
                'business professional services': 'consulting',
                'construction & infrastructure': 'construction',
                'defence': 'other',
                'education': 'education',
                'energy & utilities': 'other',
                'government': 'other',
                'healthcare and medical': 'healthcare',
                'hospitality and events': 'hospitality',
                'information technology': 'technology',
                'manufacturing': 'manufacturing',
                'mining & resources': 'other',
                'property & facilities management': 'other',
                'retail': 'retail',
                'sales': 'sales',
                'transport & logistics': 'other',
                # Additional variations found in Adecco
                'food & beverages': 'hospitality',
                'manufacturing, transport & logistics': 'manufacturing',
                'warehousing, storage & distribution': 'other',
                'machine operators': 'manufacturing',
                'manufacturing / textile / paper / wood': 'manufacturing'
            }
            
            for adecco_cat, internal_cat in category_mapping.items():
                if adecco_cat in categories_lower:
                    return internal_cat
        
        # Fallback to keyword matching
        for category, keywords in self.adecco_categories.items():
            if any(keyword in text for keyword in keywords):
                return category
        
        return 'other'
    
    def _parse_salary_info(self, salary_text):
        """
        Extract salary information from Adecco salary format.
        
        Args:
            salary_text (str): Raw salary text like "$ 90000 - $ 100000 / Year"
            
        Returns:
            dict: Salary information with min, max, type, currency
        """
        if not salary_text:
            return {}
        
        salary_info = {
            'raw_text': salary_text,
            'currency': 'AUD',
            'type': 'yearly'  # Default for Adecco
        }
        
        try:
            text_lower = salary_text.lower()
            
            # Handle hourly rates: "$ 59 - $ 60 / Hour"
            if '/ hour' in text_lower or '/hr' in text_lower or 'per hour' in text_lower:
                numbers = re.findall(r'[\d,]+', salary_text)
                if len(numbers) >= 2:
                    min_salary = int(numbers[0].replace(',', ''))
                    max_salary = int(numbers[1].replace(',', ''))
                    salary_info['min'] = min_salary
                    salary_info['max'] = max_salary
                    salary_info['type'] = 'hourly'
                elif len(numbers) == 1:
                    salary = int(numbers[0].replace(',', ''))
                    salary_info['min'] = salary
                    salary_info['max'] = salary
                    salary_info['type'] = 'hourly'
            
            # Handle annual salaries: "$ 90000 - $ 100000 / Year"
            elif '/ year' in text_lower or 'per year' in text_lower or 'year' in text_lower:
                numbers = re.findall(r'[\d,]+', salary_text)
                if len(numbers) >= 2:
                    min_salary = int(numbers[0].replace(',', ''))
                    max_salary = int(numbers[1].replace(',', ''))
                    salary_info['min'] = min_salary
                    salary_info['max'] = max_salary
                    salary_info['type'] = 'yearly'
                elif len(numbers) == 1:
                    salary = int(numbers[0].replace(',', ''))
                    salary_info['min'] = salary
                    salary_info['max'] = salary
                    salary_info['type'] = 'yearly'
            
            # Handle K format: "$80K-$90K"
            elif 'k' in text_lower:
                numbers = re.findall(r'(\d+)k', salary_text.lower())
                if len(numbers) >= 2:
                    min_salary = int(numbers[0]) * 1000
                    max_salary = int(numbers[1]) * 1000
                    salary_info['min'] = min_salary
                    salary_info['max'] = max_salary
                    salary_info['type'] = 'yearly'
                elif len(numbers) == 1:
                    salary = int(numbers[0]) * 1000
                    salary_info['min'] = salary
                    salary_info['max'] = salary
                    salary_info['type'] = 'yearly'
            
            # Handle other formats - just extract numbers
            else:
                numbers = re.findall(r'[\d,]+', salary_text)
                if numbers:
                    nums = [int(num.replace(',', '')) for num in numbers]
                    if len(nums) >= 2:
                        salary_info['min'] = min(nums)
                        salary_info['max'] = max(nums)
                    else:
                        salary_info['min'] = salary_info['max'] = nums[0]
                    
                    # Determine type from context
                    if any(word in salary_text.lower() for word in ['hour', 'hr', 'per hour']):
                        salary_info['type'] = 'hourly'
                    elif any(word in salary_text.lower() for word in ['year', 'annual', 'pa']):
                        salary_info['type'] = 'yearly'
                    elif any(word in salary_text.lower() for word in ['week', 'weekly']):
                        salary_info['type'] = 'weekly'
                    elif any(word in salary_text.lower() for word in ['month', 'monthly']):
                        salary_info['type'] = 'monthly'
        
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing salary '{salary_text}': {e}")
        
        return salary_info
    
    def _get_or_create_company(self, company_name, logo_url=None):
        """
        Get or create a company record.
        
        Args:
            company_name (str): Company name
            logo_url (str): Optional logo URL
            
        Returns:
            Company: Company model instance
        """
        # For agency postings, if employer is not disclosed, attribute to Adecco (recruiter) not a fake client
        if not company_name or company_name.strip() == '':
            company_name = 'Adecco'
        
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                logo=logo_url or '',
                company_size='large'  # Adecco is a large recruiter
            )
            logger.info(f"Created new company: {company_name}")
        
        return company
    
    def _get_or_create_location(self, location_text):
        """
        Get or create a location record.
        
        Args:
            location_text (str): Location string like "BOHLE, Queensland" or "Tivoli, Qld"
            
        Returns:
            Location: Location model instance
        """
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
        
        location_text = location_text.strip()
        
        # Try to find existing location
        location = Location.objects.filter(name__iexact=location_text).first()
        
        if not location:
            # Parse location components
            parts = [part.strip() for part in location_text.split(',')]
            
            if len(parts) >= 2:
                city = parts[0]
                state = parts[1]
                
                # Normalize state abbreviations
                state_mapping = {
                    'qld': 'Queensland',
                    'nsw': 'New South Wales',
                    'vic': 'Victoria',
                    'wa': 'Western Australia',
                    'sa': 'South Australia',
                    'tas': 'Tasmania',
                    'nt': 'Northern Territory',
                    'act': 'Australian Capital Territory'
                }
                
                if state.lower() in state_mapping:
                    state = state_mapping[state.lower()]
            else:
                city = location_text
                state = ''
            
            location = Location.objects.create(
                name=location_text,
                city=city,
                state=state,
                country='Australia'
            )
            logger.info(f"Created new location: {location_text}")
        
        return location
    
    def _extract_job_data_from_listing(self, job_element):
        """
        Extract job data from a job listing element.
        
        Args:
            job_element: Playwright element handle
            
        Returns:
            dict: Extracted job data
        """
        try:
            # Extract all data in one go to avoid navigation issues
            element_html = job_element.inner_html()
            element_text = job_element.inner_text() or ''
            
            # Initialize job data (minimally populated; will enrich from detail page)
            job_data = {
                'title': '',
                'company_name': 'Adecco',  # Recruiter attribution
                'company_logo': '',
                'location': 'Australia',
                'categories': '',
                'employment_type': '',
                'salary_text': '',
                'work_type': '',
                'job_url': '',
                'description': ''
            }
            
            # Split text into lines for analysis
            lines = [line.strip() for line in element_text.split('\n') if line.strip()]
            
            # Extract job title (prefer link text/aria-label; avoid nav items like TIMESHEETS)
            # 1) Try anchor text
            try:
                maybe_link = job_element.query_selector('a')
                if maybe_link:
                    link_text = (maybe_link.inner_text() or '').strip()
                    aria = (maybe_link.get_attribute('aria-label') or '').strip()
                    candidate_title = aria or link_text
                    if candidate_title and 3 < len(candidate_title) < 120 and not re.search(r'timesheets|saved jobs|most recent', candidate_title, re.I):
                        job_data['title'] = candidate_title
            except Exception:
                pass

            # 2) Fallback: scan the first few lines for a reasonable title
            if not job_data['title']:
                for line in lines[:5]:
                    if (line and 5 < len(line) < 120 and
                        not any(invalid in line.lower() for invalid in [
                            'timesheets', 'saved jobs', 'jobs found', 'most recent', 'check your',
                            'temporary', 'permanent', 'casual', 'full time', 'part time',
                            'hour', 'day', 'week', 'month', 'year', 'super', 'bonus', 'adecco'
                        ])):
                        job_data['title'] = line
                        break
            
            # Extract categories (look for industry classifications)
            categories = []
            for line in lines:
                line_lower = line.lower()
                if any(category in line_lower for category in [
                    'admin', 'banking', 'business', 'construction', 'defence', 'education',
                    'energy', 'government', 'healthcare', 'hospitality', 'information technology',
                    'manufacturing', 'mining', 'property', 'retail', 'sales', 'transport'
                ]):
                    categories.append(line)
            
            if categories:
                job_data['categories'] = ' | '.join(categories)
            
            # Extract employment type
            for line in lines:
                line_lower = line.lower()
                if any(emp_type in line_lower for emp_type in ['temporary', 'permanent', 'contract', 'casual']):
                    job_data['employment_type'] = line
                    break
            
            # Extract salary directly from job card using the exact structure you provided
            # Structure: card-body > 4 <p> elements > <div class="text-salary"> (5th element)
            try:
                # Method 1: Look for salary within this specific job card
                salary_div = job_element.query_selector('div.text-salary.salary-divider')
                if salary_div:
                    salary_text = (salary_div.inner_text() or '').strip()
                    if salary_text and '$' in salary_text:
                        job_data['salary_text'] = salary_text
                        logger.info(f"Extracted salary from job card: {salary_text}")
                
                # Method 2: Alternative selector for salary div
                if not job_data.get('salary_text'):
                    salary_div = job_element.query_selector('.text-salary, [class*="salary-divider"]')
                    if salary_div:
                        salary_text = (salary_div.inner_text() or '').strip()
                        if salary_text and '$' in salary_text:
                            job_data['salary_text'] = salary_text
                            logger.info(f"Extracted salary using fallback selector: {salary_text}")
                
                # Method 3: Parse the card structure directly (5th element after 4 <p> tags)
                if not job_data.get('salary_text'):
                    # Get card body
                    card_body = job_element.query_selector('.card-body')
                    if card_body:
                        # Get all child elements
                        children = card_body.query_selector_all('p, div')
                        # The salary should be around the 5th element (index 4)
                        for i, child in enumerate(children):
                            child_text = (child.inner_text() or '').strip()
                            if '$' in child_text and any(unit in child_text.lower() for unit in ['hour', 'year', 'week']):
                                job_data['salary_text'] = child_text
                                logger.info(f"Extracted salary from card structure (element {i}): {child_text}")
                                break
                
            except Exception as e:
                logger.debug(f"Error extracting salary from job card: {e}")
                
            # Fallback to empty if still no salary found
            if not job_data.get('salary_text'):
                job_data['salary_text'] = ''
            
            # Extract location (case-insensitive, support full names and abbreviations)
            state_terms = ['queensland', 'new south wales', 'victoria', 'western australia', 'south australia', 'tasmania', 'northern territory',
                           'nsw', 'qld', 'vic', 'wa', 'sa', 'tas', 'nt', 'act']
            for line in lines:
                ll = line.lower()
                if any(term in ll for term in state_terms) and ',' in line:
                    job_data['location'] = line
                    break
            
            # Extract work type
            for line in lines:
                line_lower = line.lower()
                if any(work_type in line_lower for work_type in ['casual', 'full time', 'part time', 'shift']):
                    job_data['work_type'] = line
                    break
            
            # Try to extract job URL from href attributes
            href_match = re.search(r'href=["\']([^"\']+)["\']', element_html)
            if href_match:
                href = href_match.group(1)
                if href.startswith('http'):
                    job_data['job_url'] = href
                elif href.startswith('/'):
                    job_data['job_url'] = urljoin(self.base_url, href)
                elif href.startswith('?'):
                    job_data['job_url'] = f"{self.jobs_url}{href}"
            
            # Clean up title
            job_data['title'] = (job_data['title'] or '').replace('Logo Image', '').replace('Image', '').strip()
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None
    
    def _get_full_job_description(self, page, job_url, job_title):
        """
        Extract comprehensive job details by visiting the individual job detail page.
        This method prioritizes data from the actual job page over listing data.
        
        Args:
            page: Playwright page object
            job_url (str): URL of the job listing
            job_title (str): Job title for reference
            
        Returns:
            dict: Complete job details extracted from individual job page
        """
        if not job_url or job_url in self.processed_urls:
            return {
                'title': job_title or '',
                'description': '',
                'location': '',
                'salary_text': '',
                'employment_type': '',
                'work_type': ''
            }
        
        # Default result object to ensure we always return a consistent structure
        default_result = {
            'title': job_title or '',
            'description': '',
            'location': '',
            'salary_text': '',
            'employment_type': '',
            'work_type': ''
        }

        try:
            logger.info(f"Extracting full job details from individual page: {job_title}")
            
            # Navigate to individual job detail page
            # [cursor:reason] Use DOM load and higher timeout for detail pages to avoid timeouts
            page.goto(job_url, wait_until='domcontentloaded', timeout=60000)
            self.processed_urls.add(job_url)
            
            # Wait for job detail content to fully load
            time.sleep(random.uniform(2, 4))
            
            # Wait for specific job detail elements to be present
            try:
                page.wait_for_selector('body', timeout=10000)
            except:
                pass
            
            # Get the full page content for comprehensive extraction
            page_content = page.inner_text('body')
            page_html = page.content()
            
            # Initialize extraction variables
            extracted_data = {
                'title': '',
                'description': '',
                'location': '',
                'salary_text': '',
                'employment_type': '',
                'work_type': ''
            }
            
            # === 1. TITLE EXTRACTION (Based on Adecco HTML Structure) ===
            title_selectors = [
                '.h3',  # Main title in Adecco job details: <div class="h3">
                'div.h3',  # Alternative selector
                'h1',  # Fallback to standard heading
                '.job-title',  # Generic job title class
                'h2'  # Secondary fallback
            ]
            
            for selector in title_selectors:
                try:
                    title_element = page.query_selector(selector)
                    if title_element:
                        title_text = (title_element.inner_text() or '').strip()
                        # Clean title by removing salary info if it's included
                        # "CNC Machinist / Programmer | $80K–$90K + Super + Bonuses" -> "CNC Machinist / Programmer"
                        if '|' in title_text and '$' in title_text:
                            title_text = title_text.split('|')[0].strip()
                        
                        # Validate it's a real job title
                        if (5 <= len(title_text) <= 150 and 
                            not any(banned in title_text.lower() for banned in [
                                'timesheets', 'saved jobs', 'navigation', 'menu', 'login', 
                                'search jobs', 'apply now', 'adecco', 'privacy', 'preference', 
                                'center', 'cookie', 'policy', 'terms', 'conditions', 'job details'
                            ])):
                            extracted_data['title'] = title_text
                            logger.info(f"Extracted title from individual page: {title_text}")
                            break
                except Exception:
                    continue
            
            # === 2. LOCATION EXTRACTION (Based on Adecco HTML Structure) ===
            # Look for location icon followed by text: <span class="material-icons-outlined">location_on</span><span>Regency Park, Sa</span>
            location_selectors = [
                'span.material-icons-outlined:has-text("location_on") + span',  # Direct sibling after location icon
                '.job-icon:has(span.material-icons-outlined:has-text("location_on")) span:last-child',  # Last span in job-icon container
                '[class*="location"]',  # Any element with location in class name
                'span:has-text("location_on") + span'  # Fallback
            ]
            
            for selector in location_selectors:
                try:
                    location_element = page.query_selector(selector)
                    if location_element:
                        location_text = (location_element.inner_text() or '').strip()
                        
                        # Clean and validate location
                        if (location_text and 
                            len(location_text) >= 3 and
                            not any(invalid in location_text.lower() for invalid in ['$', 'hour', 'year', 'salary', 'apply', 'search', 'permanent', 'full time'])):
                            
                            # Convert state abbreviations to full names
                            state_mapping = {
                                'Sa': 'South Australia',
                                'SA': 'South Australia',
                                'NSW': 'New South Wales',
                                'Nsw': 'New South Wales', 
                                'QLD': 'Queensland',
                                'Qld': 'Queensland',
                                'VIC': 'Victoria',
                                'Vic': 'Victoria',
                                'WA': 'Western Australia',
                                'TAS': 'Tasmania',
                                'NT': 'Northern Territory',
                                'ACT': 'Australian Capital Territory'
                            }
                            
                            # Handle format like "Regency Park, Sa"
                            for abbr, full_name in state_mapping.items():
                                if location_text.endswith(f', {abbr}'):
                                    location_text = location_text.replace(f', {abbr}', f', {full_name}')
                                    break
                            
                            extracted_data['location'] = location_text
                            logger.info(f"Extracted location from individual page: {location_text}")
                            break
                except Exception:
                    continue
            
            # Fallback to text pattern matching if selector method fails
            if not extracted_data['location']:
                location_patterns = [
                    r'([A-Za-z][A-Za-z \-\'\.]+),\s*(South Australia|New South Wales|Queensland|Victoria|Western Australia|Tasmania|Northern Territory|Australian Capital Territory)\b',
                    r'([A-Za-z][A-Za-z \-\'\.]+),\s*(SA|NSW|QLD|VIC|WA|TAS|NT|ACT|Sa|Nsw|Qld|Vic)\b'
                ]
                
                for pattern in location_patterns:
                    location_match = re.search(pattern, page_content, re.IGNORECASE)
                    if location_match:
                        city = location_match.group(1).strip()
                        state_text = location_match.group(2).strip()
                        
                        # Convert state abbreviations to full names
                        state_mapping = {
                            'SA': 'South Australia', 'Sa': 'South Australia',
                            'NSW': 'New South Wales', 'Nsw': 'New South Wales',
                            'QLD': 'Queensland', 'Qld': 'Queensland',
                            'VIC': 'Victoria', 'Vic': 'Victoria',
                            'WA': 'Western Australia',
                            'TAS': 'Tasmania',
                            'NT': 'Northern Territory',
                            'ACT': 'Australian Capital Territory'
                        }
                        
                        full_state = state_mapping.get(state_text, state_text)
                        
                        if (not any(invalid in city.lower() for invalid in ['$', 'hour', 'year', 'salary', 'apply', 'search']) and
                            len(city) >= 3):
                            extracted_data['location'] = f"{city}, {full_state}"
                            logger.info(f"Extracted location from pattern match: {extracted_data['location']}")
                            break
            
            # === 3. SALARY EXTRACTION FROM JOB DETAIL PAGE ===
            # Extract salary using the exact HTML structure you provided
            salary_found = False
            
            # === 3. SALARY EXTRACTION FROM JOB DETAIL PAGE ===
            # Look for salary in specific Adecco elements: <div class="text-salary salary-divider text-01 roundness_1">$ 75000 - $ 90000 / Year</div>
            salary_selectors = [
                '.text-salary',  # Main salary class in Adecco: <div class="text-salary salary-divider text-01 roundness_1">
                'div.text-salary',  # Alternative selector
                '.salary-divider',  # Salary divider class
                '[class*="salary"]'  # Any element with salary in class name
            ]
            
            for selector in salary_selectors:
                try:
                    salary_element = page.query_selector(selector)
                    if salary_element:
                        salary_text = (salary_element.inner_text() or '').strip()
                        
                        # Validate it's a real salary (contains $ and reasonable units)
                        if (salary_text and 
                            '$' in salary_text and 
                            any(unit in salary_text.lower() for unit in ['year', 'hour', 'k']) and
                            not any(invalid in salary_text.lower() for invalid in ['search', 'apply', 'navigation', 'menu'])):
                            extracted_data['salary_text'] = salary_text
                            logger.info(f"Extracted salary from individual page: {salary_text}")
                            break
                except Exception:
                    continue
            
            # Method 3: Pattern matching in page content as final fallback
            if not salary_found:
                salary_patterns = [
                    r'\$\s*[\d,]+\s*-\s*\$\s*[\d,]+\s*/\s*Year',  # "$ 90000 - $ 100000 / Year"
                    r'\$\s*[\d,]+\s*-\s*\$\s*[\d,]+\s*/\s*Hour',  # "$ 59 - $ 60 / Hour"
                    r'\$\s*[\d,]+\s*/\s*Year',  # "$ 120000 / Year"
                    r'\$\s*[\d,]+\s*/\s*Hour',  # "$ 45 / Hour"
                    r'\$\s*\d+[kK]\s*-\s*\$\s*\d+[kK]',  # "$80K - $90K"
                ]
                
                for pattern in salary_patterns:
                    salary_match = re.search(pattern, page_content, re.IGNORECASE)
                    if salary_match:
                        extracted_data['salary_text'] = salary_match.group(0)
                        logger.info(f"Extracted salary from pattern: {salary_match.group(0)}")
                        break
            

            
            # === 4. EMPLOYMENT TYPE EXTRACTION (Based on Adecco HTML Structure) ===
            # Look for work-related icons: <span class="material-icons-outlined">work_outline</span><span>Permanent</span>
            work_selectors = [
                'span.material-icons-outlined:has-text("work_outline") + span',  # Direct sibling after work icon
                '.job-icon:has(span.material-icons-outlined:has-text("work_outline")) span:last-child',  # Last span in job-icon container
                'div.job-icon span:not(.material-icons-outlined)'  # Any non-icon span in job-icon div
            ]
            
            work_types_found = []
            for selector in work_selectors:
                try:
                    work_elements = page.query_selector_all(selector)
                    for element in work_elements:
                        work_text = (element.inner_text() or '').strip()
                        if work_text and work_text not in work_types_found:
                            work_types_found.append(work_text)
                except Exception:
                    continue
            
            # Categorize the found work types
            for work_text in work_types_found:
                work_lower = work_text.lower()
                if work_text and len(work_text) < 20:  # Reasonable length for work type
                    if any(emp_type in work_lower for emp_type in ['permanent', 'temporary', 'contract', 'casual']):
                        extracted_data['employment_type'] = work_text
                        logger.info(f"Extracted employment type: {work_text}")
                    elif any(work_type in work_lower for work_type in ['full time', 'part time', 'full-time', 'part-time']):
                        extracted_data['work_type'] = work_text
                        logger.info(f"Extracted work type: {work_text}")
            
            # Fallback to pattern matching if selector method fails
            if not extracted_data['employment_type']:
                employment_patterns = [
                    r'\b(Permanent)\b',
                    r'\b(Temporary)\b',
                    r'\b(Contract)\b', 
                    r'\b(Casual)\b'
                ]
                
                for pattern in employment_patterns:
                    emp_match = re.search(pattern, page_content, re.IGNORECASE)
                    if emp_match:
                        extracted_data['employment_type'] = emp_match.group(1)
                        break
                        
            if not extracted_data['work_type']:
                work_type_patterns = [
                    r'\b(Full\s*Time)\b',
                    r'\b(Part\s*Time)\b', 
                    r'\b(Full-Time)\b',
                    r'\b(Part-Time)\b'
                ]
                
                for pattern in work_type_patterns:
                    work_match = re.search(pattern, page_content, re.IGNORECASE)
                    if work_match:
                        extracted_data['work_type'] = work_match.group(1)
                        break
            
            # === 6. DESCRIPTION EXTRACTION ===
            # Try multiple approaches to get the best job description
            description_methods = [
                self._extract_description_from_selectors,
                self._extract_description_after_copy_link,
                self._extract_description_from_paragraphs
            ]
            
            for method in description_methods:
                try:
                    description = method(page, page_content)
                    if description and len(description) > 100:
                        extracted_data['description'] = description
                        logger.info(f"Extracted description using {method.__name__}: {len(description)} chars")
                        break
                except Exception as e:
                    logger.debug(f"Description method {method.__name__} failed: {e}")
                    continue
            
            # Return the extracted data, prioritizing individual page data
            return {
                'title': extracted_data['title'] or job_title or '',
                'description': extracted_data['description'],
                'location': extracted_data['location'],
                'salary_text': extracted_data['salary_text'],
                'employment_type': extracted_data['employment_type'],
                'work_type': extracted_data['work_type']
            }
            
        except Exception as e:
            logger.error(f"Error extracting job details from individual page for {job_title}: {e}")
            return default_result
    
    def _extract_description_from_selectors(self, page, page_content):
        """Extract complete job description content from individual job pages."""
        
        # Based on debugging, 'main' element contains the complete job description
        # with all sections: About Us, Responsibilities, Your Profile, Contact
        priority_selectors = [
            'main',                         # Main content area - contains complete job description
            'body',                         # Full body content as fallback
        ]
        
        # Try priority selectors first (these contain complete descriptions)
        for selector in priority_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    full_text = element.inner_text().strip()
                    if len(full_text) > 1000:  # Should be substantial for complete descriptions
                        # Extract only the job description part, removing navigation
                        clean_description = self._extract_clean_job_description(full_text)
                        if len(clean_description) > 300:  # Ensure we have substantial content
                            logger.info(f"Extracted description using {selector}: {len(clean_description)} chars")
                            return clean_description
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        # Fallback to original method if main extraction fails
        logger.warning("Main extraction failed, using fallback method")
        return self._extract_description_after_copy_link(page, page_content)
    
    def _extract_description_after_copy_link(self, page, page_content):
        """Extract description content that appears after 'Copy Link' section."""
        lines = page_content.split('\n')
        copy_link_found = False
        description_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check if we've reached the "Copy Link" section
            if 'copy link' in line.lower():
                copy_link_found = True
                continue
            
            # Start collecting meaningful content after Copy Link
            if copy_link_found:
                # Look for substantial job description content
                if (len(line) >= 30 and 
                    any(keyword in line.lower() for keyword in [
                        'join', 'our client', 'seeking', 'we are', 'position', 'role', 
                        'responsibilities', 'requirements', 'experience', 'skills'
                    ]) and
                    not any(unwanted in line.lower() for unwanted in [
                        'apply', 'save job', 'linkedin', 'facebook', 'navigation', 'menu'
                    ])):
                    description_lines.append(line)
        
        return '\n'.join(description_lines) if description_lines else ''
    
    def _is_job_description_content(self, text):
        """Check if text contains actual job description content, not navigation."""
        text_lower = text.lower()
        
        # Reject if it contains obvious navigation elements
        navigation_indicators = [
            'looking for panel beaters automotive / aerospace trades & services',
            'electrician – tram project manufacturing / textile',
            'navigate_before ... navigate_next',
            'compass_calibration full time',
            'apply for job save job copy link',
            'go to job detail page'
        ]
        
        for indicator in navigation_indicators:
            if indicator in text_lower:
                return False
        
        # Must contain job description indicators
        job_indicators = [
            'about us', 'responsibilities', 'requirements', 'your profile',
            'what we offer', 'contact', 'role', 'position', 'experience'
        ]
        
        return any(indicator in text_lower for indicator in job_indicators)
    
    def _contains_job_listings(self, text):
        """Check if element contains job listings navigation."""
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in [
            'looking for panel beaters',
            'electrician – tram project',
            'hr manager – 3-month contract',
            'qualified spray painter',
            'store manager',
            'retail sales associate'
        ])
    
    def _clean_job_description_only(self, text):
        """Clean text to contain only job description, removing navigation elements."""
        lines = text.split('\n')
        clean_lines = []
        
        # Skip lines that are clearly navigation
        skip_indicators = [
            'looking for panel beaters',
            'automotive / aerospace',
            'trades & services',
            'compass_calibration',
            'navigate_before',
            'navigate_next',
            'apply for job',
            'save job',
            'copy link',
            'go to job detail page',
            '$ 70000 - $ 80000 / year',
            '$ 60 - $ 62 / hour',
            'permanent', 'temporary', 'casual', 'full time', 'part time'
        ]
        
        for line in lines:
            line_clean = line.strip()
            if not line_clean:
                continue
                
            line_lower = line_clean.lower()
            
            # Skip navigation lines
            if any(skip in line_lower for skip in skip_indicators):
                continue
            
            # Skip single words that are UI elements
            if len(line_clean.split()) == 1 and line_lower in ['permanent', 'temporary', 'casual', 'remote', 'full', 'time']:
                continue
                
            clean_lines.append(line_clean)
        
        return '\n'.join(clean_lines)
    
    def _extract_clean_job_description(self, full_text):
        """Extract clean job description from main content, removing navigation and keeping job sections."""
        lines = full_text.split('\n')
        
        # Find the start of the actual job content (skip navigation/header)
        start_indicators = [
            'adecco',                           # Adecco company intro
            'about us',                         # About Us section
            'from high-speed trains',           # Alstom intro (common client)
            'our client',                       # Job description start
            'we are',                          # Company/role description
            'role summary',                    # Role description
            'responsibilities',                # Responsibilities section
            'key responsibilities'             # Key responsibilities
        ]
        
        # Find where job content ends (before footer/references)
        end_indicators = [
            'ref: jn-',                        # Reference number at end
            'with more than 10,000 branches',  # Adecco footer
            'privacy policy',                  # Footer links
            'terms and conditions',            # Footer links
            'security & phishing',             # Footer content
            'modern slavery statement'         # Footer content
        ]
        
        # Skip obvious navigation elements
        skip_patterns = [
            'timesheets', 'careers at adecco', 'online centre', 'payroll',
            'skip to main', 'candidate', 'employers', 'industries', 
            'about usexpand_more', 'newsJoin our team', 'contactcontact',
            'job titlelocationlocation_on', 'search jobsfilter_alt',
            'saved jobs', 'most recentexpand_more', 'compass_calibration',
            'construction workers', 'panel beaters', 'electrician',  # Job listing titles
            'hr manager', 'store manager', 'retail sales associate',
            '$ 35 - $ 48 / hour', '$ 80000 - $ 90000 / year',        # Salary ranges from listings
            'ottoway, south australia', 'townsville, queensland',    # Location listings
            'temporary', 'permanent', 'full time', 'casual'          # Employment type listings (when standalone)
        ]
        
        job_lines = []
        capture_mode = False
        
        for line in lines:
            line_clean = line.strip()
            if not line_clean:
                if capture_mode:
                    job_lines.append('')  # Preserve paragraph spacing
                continue
            
            line_lower = line_clean.lower()
            
            # Check if we should stop capturing
            if any(end_pattern in line_lower for end_pattern in end_indicators):
                break
            
            # Check if we should start capturing
            if not capture_mode:
                if any(start_pattern in line_lower for start_pattern in start_indicators):
                    capture_mode = True
                    # Include this line as it's likely the start of job content
                    if len(line_clean) > 10:  # Skip very short lines
                        job_lines.append(line_clean)
                continue
            
            # If we're in capture mode, filter out navigation/listing content
            if capture_mode:
                # Skip navigation and job listing pollution
                if any(skip_pattern in line_lower for skip_pattern in skip_patterns):
                    continue
                
                # Skip single words that are likely UI elements
                if len(line_clean.split()) == 1 and line_lower in [
                    'temporary', 'permanent', 'casual', 'remote', 'full', 'time', 'part'
                ]:
                    continue
                
                # Skip lines that look like salary ranges from job listings
                if ('$' in line_clean and ('hour' in line_lower or 'year' in line_lower) and 
                    len(line_clean) < 50):
                    continue
                
                # Include substantial content lines
                if len(line_clean) > 3:
                    job_lines.append(line_clean)
        
        # Clean up and format the result
        description = '\n'.join(job_lines)
        
        # Remove excessive whitespace but preserve structure
        import re
        description = re.sub(r'\n{3,}', '\n\n', description)
        description = re.sub(r' {2,}', ' ', description)
        
        return description.strip()
    
    def _extract_job_description_only(self, full_text):
        """Legacy method - now redirects to the improved extraction method."""
        return self._extract_clean_job_description(full_text)
    
    def _extract_description_from_paragraphs(self, page, page_content):
        """Extract description from paragraph elements."""
        try:
            paragraphs = page.query_selector_all('p')
            description_paragraphs = []
            
            for p in paragraphs:
                text = (p.inner_text() or '').strip()
                if (len(text) >= 50 and
                    any(keyword in text.lower() for keyword in [
                        'seeking', 'client', 'role', 'responsible', 'experience', 
                        'skills', 'requirements', 'join', 'position'
                    ]) and
                    not any(unwanted in text.lower() for unwanted in [
                        'apply for job', 'save job', 'copy link', 'linkedin', 
                        'facebook', 'about us', 'navigation'
                    ])):
                    description_paragraphs.append(text)
            
            return '\n\n'.join(description_paragraphs) if description_paragraphs else ''
        except Exception:
            return ''
    
    def _clean_description_text(self, text):
        """Clean and format description text while preserving comprehensive content from About Us to Contact."""
        lines = text.split('\n')
        cleaned_lines = []
        
        # More comprehensive content collection - don't skip too much
        unwanted_elements = [
            'adecco navigation', 'site navigation', 'website menu',
            'skip to main', 'candidate expand_more', 'employers expand_more',
            'timesheets', 'online centre', 'payroll language',
            'search jobs', 'filter_alt filter', 'saved jobs'
        ]
        
        # Track if we're in job content vs navigation
        in_job_content = False
        
        for line in lines:
            line = line.strip()
            if not line:
                if in_job_content:
                    cleaned_lines.append('')  # Preserve paragraph breaks in job content
                continue
            
            line_lower = line.lower()
            
            # Skip obvious navigation elements
            if any(unwanted in line_lower for unwanted in unwanted_elements):
                continue
            
            # Start collecting content when we hit job-related sections
            if not in_job_content:
                if any(keyword in line_lower for keyword in [
                    'about us', 'about the company', 'about the role', 'job details',
                    'role summary', 'what we offer', 'responsibilities', 'requirements',
                    'qualifications', 'experience', 'skills', 'benefits', 'contact',
                    'join', 'our client', 'seeking', 'we are', 'position', 'role',
                    'opportunity', 'employment', 'vacancy'
                ]):
                    in_job_content = True
            
            # Stop collecting at obvious footer/policy content
            if any(footer_element in line_lower for footer_element in [
                'privacy policy', 'website terms', 'cookie policy',
                'all rights reserved', 'modern slavery statement'
            ]):
                break
            
            # Include content if we're in the job content section
            if in_job_content:
                # Skip very short lines that are likely navigation, but keep important short ones
                if len(line) < 3 and not re.match(r'^[A-Z][a-z]*:?$', line):
                    continue
                
                # Skip lines that are just numbers
                if re.match(r'^\d+$', line):
                    continue
                
                cleaned_lines.append(line)
            # Even if not in job content yet, include substantial lines that might be job-related
            elif len(line) > 30 and any(keyword in line_lower for keyword in [
                'role', 'position', 'job', 'opportunity', 'employment', 'career',
                'responsible', 'duties', 'skills', 'experience', 'qualification'
            ]):
                in_job_content = True
                cleaned_lines.append(line)
        
        # Join lines and clean up extra whitespace while preserving structure
        full_description = '\n'.join(cleaned_lines)
        
        # Remove excessive consecutive newlines but preserve paragraph structure
        import re
        full_description = re.sub(r'\n{4,}', '\n\n\n', full_description)
        
        return full_description.strip()
    
    def _click_and_extract_from_panel(self, page, job_element):
        """
        Click a job card on the left and extract details from the right-hand panel
        without navigating to a separate page. Returns a job_data dict.
        """
        job_data = {
            'title': '',
            'company_name': 'Adecco',
            'company_logo': '',
            'location': '',
            'categories': '',
            'employment_type': '',
            'salary_text': '',
            'work_type': '',
            'job_url': '',
            'description': ''
        }

        try:
            # Click the card and wait for right panel to be ready
            job_element.scroll_into_view_if_needed()
            job_element.click(timeout=5000)
            # Wait for either URL jobId or the Apply button to appear/update
            try:
                page.wait_for_selector('button:has-text("APPLY FOR JOB")', timeout=6000)
            except Exception:
                pass

            # Capture the current URL as an external_url if it includes jobId
            current_url = page.url
            if 'jobId=' in current_url:
                job_data['job_url'] = current_url

            # Find the right panel via the Apply button ancestor for scoped parsing
            panel = None
            try:
                btn = page.query_selector('button:has-text("APPLY FOR JOB")')
                if btn:
                    panel = btn.evaluate_handle('(el)=>el.closest("section, article, div")')
            except Exception:
                panel = None

            # If we have the panel node, use it; otherwise, fallback to body
            container = None
            try:
                if panel:
                    container = panel.as_element()
            except Exception:
                container = None

            # Extract title
            title_text = ''
            for sel in ['h1', 'h2', '[class*="title"] h1', '[class*="title"] h2']:
                try:
                    node = (container or page).query_selector(sel)
                    if node:
                        t = (node.inner_text() or '').strip()
                        if 3 < len(t) < 160 and not re.search(r'timesheets|saved jobs', t, re.I):
                            title_text = t
                            break
                except Exception:
                    continue
            job_data['title'] = title_text

            # Extract the panel text for parsing of other fields
            try:
                panel_text = (container or page).inner_text()
            except Exception:
                panel_text = ''

            # Skip salary extraction from panel - will get it from individual job page only
            job_data['salary_text'] = ''

            # Location - Enhanced pattern to capture more variations
            # First try: Full state names and common city patterns
            loc_match = re.search(r'([A-Za-z][A-Za-z \-\']+),\s*(Queensland|New South Wales|Victoria|Western Australia|South Australia|Tasmania|Northern Territory|Australian Capital Territory)', panel_text, re.I)
            if loc_match:
                city = loc_match.group(1).strip()
                state = loc_match.group(2).strip()
                job_data['location'] = f"{city}, {state}"
            else:
                # Try abbreviated states
                loc_match = re.search(r'([A-Za-z][A-Za-z \-\']+),\s*(ACT|NSW|QLD|VIC|WA|SA|TAS|NT)\b', panel_text, re.I)
                if loc_match:
                    city = loc_match.group(1).strip()
                    state_abbr = loc_match.group(2).strip().upper()
                    # Convert abbreviation to full name
                    state_mapping = {
                        'NSW': 'New South Wales',
                        'QLD': 'Queensland', 
                        'VIC': 'Victoria',
                        'WA': 'Western Australia',
                        'SA': 'South Australia',
                        'TAS': 'Tasmania',
                        'NT': 'Northern Territory',
                        'ACT': 'Australian Capital Territory'
                    }
                    state = state_mapping.get(state_abbr, state_abbr)
                    job_data['location'] = f"{city}, {state}"

            # Employment type
            et_match = re.search(r'(Permanent|Temporary|Contract)', panel_text, re.I)
            if et_match:
                job_data['employment_type'] = et_match.group(1)

            # Work type
            wt_match = re.search(r'(Casual|Full\s*Time|Part\s*Time|Shift)', panel_text, re.I)
            if wt_match:
                job_data['work_type'] = wt_match.group(1)

            # Description: Enhanced extraction for better content capture
            description_texts = []
            
            # Method 1: Try to find job description in dedicated containers
            desc_selectors = [
                '.job-description',
                '[class*="description"]',
                '.content',
                '[class*="content"]',
                'div:has-text("Our client"):parent',
                'div:has-text("seeking"):parent'
            ]
            
            for selector in desc_selectors:
                try:
                    desc_node = (container or page).query_selector(selector)
                    if desc_node:
                        desc_text = (desc_node.inner_text() or '').strip()
                        if len(desc_text) >= 100:
                            description_texts.append(desc_text)
                            break
                except Exception:
                    continue
            
            # Method 2: Extract meaningful paragraphs if no dedicated container
            if not description_texts:
                try:
                    p_nodes = (container or page).query_selector_all('p')
                    for pn in p_nodes:
                        t = (pn.inner_text() or '').strip()
                        # Look for substantial job description content
                        if (len(t) >= 40 and 
                            not re.search(r'apply for job|save job|copy link|linkedin|facebook|about us', t, re.I) and
                            any(keyword in t.lower() for keyword in ['seeking', 'client', 'role', 'responsible', 'experience', 'skills', 'requirements'])):
                            description_texts.append(t)
                except Exception:
                    pass
            
            # Method 3: Fallback to filtered significant lines
            if not description_texts:
                desc_lines = []
                unwanted = ['apply for job', 'copy link', 'save job', 'linkedin', 'facebook', 'about us', 'adecco', 'navigation', 'menu']
                for l in lines:
                    if any(u in l.lower() for u in unwanted):
                        continue
                    # Look for lines that likely contain job description content
                    if (len(l) >= 50 and 
                        any(keyword in l.lower() for keyword in ['seeking', 'client', 'role', 'responsible', 'experience', 'skills', 'requirements', 'duties', 'qualifications'])):
                        desc_lines.append(l)
                if desc_lines:
                    description_texts = desc_lines
            
            job_data['description'] = '\n\n'.join(description_texts).strip()

            return job_data
        except Exception as e:
            logger.debug(f"Panel extraction failed: {e}")
            return job_data
    
    def _detect_total_pages(self, page):
        """
        Detect total number of pages from pagination.
        
        Args:
            page: Playwright page object
            
        Returns:
            int: Total number of pages, or None if not detected
        """
        try:
            logger.info("Starting pagination detection...")
            
            # Look for pagination elements
            page_links = page.query_selector_all('a[href*="pg="]')
            max_page = 0
            
            for link in page_links:
                href = link.get_attribute('href')
                if href:
                    page_match = re.search(r'pg=(\d+)', href)
                    if page_match:
                        page_num = int(page_match.group(1))
                        max_page = max(max_page, page_num)
            
            # Also check for "Next" button to see if there are more pages
            next_button = page.query_selector('a:has-text("Next"), button:has-text("Next")')
            if next_button and max_page > 0:
                max_page += 1  # Add one more page if Next button exists
            
            if max_page > 0:
                logger.info(f"Detected {max_page} total pages")
                return max_page
            
            # Fallback: look for job count to estimate pages
            page_text = page.inner_text('body')
            job_count_match = re.search(r'(\d+)\s+Jobs?\s+Found', page_text)
            if job_count_match:
                total_jobs = int(job_count_match.group(1))
                # Assume ~20 jobs per page
                estimated_pages = (total_jobs + 19) // 20
                logger.info(f"Estimated {estimated_pages} pages from {total_jobs} jobs")
                return estimated_pages
            
        except Exception as e:
            logger.error(f"Error detecting total pages: {e}")
        
        # Conservative fallback
        logger.warning("Could not detect total pages, using conservative fallback of 5 pages")
        return 5
    
    def _get_right_panel_title(self, page) -> str:
        """Read the current title from the right details panel if present."""
        title_candidates = ['h1', '.job-title', '[class*="title"] h1', '[class*="title"]']
        for sel in title_candidates:
            try:
                node = page.query_selector(sel)
                if node:
                    text = (node.inner_text() or '').strip()
                    if 3 < len(text) < 160:
                        return text
            except Exception:
                continue
        return ''

    def _wait_for_panel_change(self, page, prev_title: str, prev_url: str, timeout_sec: float = 6.0) -> None:
        """After clicking a card, wait until either the URL (jobId) or the title changes."""
        start = time.time()
        while time.time() - start < timeout_sec:
            try:
                if page.url != prev_url and 'jobId=' in page.url:
                    return
                current_title = self._get_right_panel_title(page)
                if current_title and current_title != prev_title:
                    return
            except Exception:
                pass
            time.sleep(0.2)

    def _find_left_column_job_cards(self, page):
        """Return a list of job card elements using the correct selectors discovered from debugging."""
        # Use the selectors that actually work - found from website debugging
        primary_selectors = [
            '.card',           # Primary selector: exactly 10 job cards per page
            'article',         # Alternative: exactly 10 article elements per page  
        ]
        
        for selector in primary_selectors:
            try:
                elements = page.query_selector_all(selector)
                if len(elements) == 10:  # Perfect match - 10 jobs per page
                    logger.info(f"Found {len(elements)} job cards using selector: {selector}")
                    return elements
                elif 8 <= len(elements) <= 12:  # Close enough
                    logger.info(f"Found {len(elements)} job cards using selector: {selector} (close to 10)")
                    return elements
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        # Fallback to original method if needed
        logger.warning("Using fallback job card detection method")
        candidates_selectors = [
            'div:has-text("Permanent")',
            'div:has-text("Temporary")',
            'div:has-text("Casual")',
            'div:has-text("Full Time")',
            'div:has-text("Part Time")',
            'div:has-text(", ")'
        ]
        elements = []
        for sel in candidates_selectors:
            try:
                elements = page.query_selector_all(sel)
                if elements:
                    break
            except Exception:
                continue

        filtered = []
        # Use viewport width to split left vs right roughly in half
        try:
            viewport = page.viewport_size
            split_x = (viewport.get('width', 1200) // 2) if viewport else 600
        except Exception:
            split_x = 600

        for el in elements or []:
            try:
                box = el.bounding_box()
                txt = (el.inner_text() or '').strip()
                # Heuristics: only left column cards (x small), reasonable size, has multiple lines
                if box and box.get('x', 1000) < split_x and box.get('height', 0) > 120 and '\n' in txt:
                    filtered.append(el)
            except Exception:
                continue

        return filtered

    # -------------------------
    # Validation / cleaning
    # -------------------------
    def _is_valid_title(self, title: str) -> bool:
        if not title:
            return False
        t = title.strip()
        if len(t) < 3 or len(t) > 160:  # Allow shorter titles like "UX Designer"
            return False
        
        # Enhanced banned terms to catch fake job titles
        banned = [
            'timesheets', 'job title', 'saved jobs', 'employers', 'candidates',
            'most recent', 'check your', 'navigation', 'menu', 'header', 'footer',
            'online centre', 'adecco works for everyone', 'cookie policy', 'privacy policy',
            'about us', 'contact us', 'terms and conditions', 'site navigation',
            'main navigation', 'breadcrumb', 'search results', 'filter by', 'sort by',
            'privacy preference center', 'preference center', 'privacy preferences'
        ]
        if any(b in t.lower() for b in banned):
            return False
        
        # Reject titles that are just company names or generic website text
        company_names = ['adecco', 'seek', 'linkedin', 'indeed', 'jobsearch']
        if t.lower() in company_names:
            return False
        
        # Reject titles that look like website UI elements
        ui_patterns = [
            r'^(search|filter|sort|view|show|hide|toggle|click|select)$',
            r'^(page \d+|next|previous|back|home|login|logout|register)$',
            r'^(yes|no|ok|cancel|submit|apply now|save job)$'
        ]
        for pattern in ui_patterns:
            if re.match(pattern, t.lower()):
                return False
        
        # Must contain at least one alphabetic character
        if not re.search(r'[A-Za-z]', t):
            return False
        
        # Expanded job-like keywords to include more varied roles
        job_words = [
            'manager', 'engineer', 'technician', 'operator', 'assistant', 'coordinator', 
            'supervisor', 'programmer', 'driver', 'welder', 'designer', 'machinist', 
            'analyst', 'officer', 'advisor', 'developer', 'specialist', 'consultant',
            'executive', 'director', 'lead', 'senior', 'junior', 'trainee', 'apprentice',
            'clerk', 'receptionist', 'administrator', 'secretary', 'accountant',
            'nurse', 'therapist', 'teacher', 'instructor', 'chef', 'waiter', 'barista',
            'sales', 'marketing', 'hr', 'human resources', 'finance', 'accounting',
            'beater', 'mechanic', 'fitter', 'electrician', 'plumber', 'carpenter',
            'associate', 'representative', 'agent', 'helper', 'worker', 'staff'
        ]
        
        # Valid if it has multiple words OR contains job-related keywords
        if len(t.split()) >= 2 or any(w in t.lower() for w in job_words):
            return True
            
        # Also accept single words that look like job titles (proper nouns or capitalized)
        if len(t.split()) == 1 and (t[0].isupper() or t.istitle()) and len(t) >= 3:
            # But not if it's a common website word
            website_words = ['home', 'about', 'contact', 'help', 'support', 'blog', 'news']
            if t.lower() not in website_words:
                return True
            
        return False

    def _is_valid_location(self, loc: str) -> bool:
        if not loc:
            return False
        l = loc.strip()
        if '$' in l or 'hour' in l.lower() or 'salary' in l.lower():
            return False
        states = ['NSW', 'QLD', 'VIC', 'WA', 'SA', 'TAS', 'NT', 'ACT',
                  'New South Wales', 'Queensland', 'Victoria', 'Western Australia', 'South Australia', 'Tasmania', 'Northern Territory']
        if any(s.lower() in l.lower() for s in states):
            return True
        return False

    def _is_valid_description(self, desc: str) -> bool:
        """Validate that description contains actual job content, not website elements."""
        if not desc:
            return False
        
        # More lenient length requirement - allow shorter descriptions
        if len(desc.strip()) < 20:
            return False
        
        desc_lower = desc.lower()
        
        # Only reject obvious privacy/cookie policy content (be more specific)
        privacy_indicators = [
            'these cookies may be set through our site by our advertising partners',
            'build a profile of your interests and show you relevant adverts',
            'privacy preference center',
            'cookie policy - terms and conditions',
            'gdpr compliance policy'
        ]
        
        # Only reject if it's clearly privacy policy content (require longer matches)
        for indicator in privacy_indicators:
            if indicator in desc_lower:
                return False
        
        # Check for obvious navigation/website content (be more specific)
        website_indicators = [
            'site navigation menu',
            'main navigation breadcrumb',
            'search results filter by sort by',
            'page footer header menu',
            'website navigation bar'
        ]
        
        # Only reject if it's clearly website navigation (require longer matches)
        for indicator in website_indicators:
            if indicator in desc_lower:
                return False
        
        # Accept if it contains ANY job-related content (be more permissive)
        job_content_indicators = [
            'role', 'position', 'responsible', 'duties', 'experience', 'skills',
            'qualifications', 'requirements', 'join', 'team', 'client', 'seeking',
            'opportunity', 'candidate', 'apply', 'we are looking', 'you will',
            'partnering', 'recruit', 'industry', 'adecco is', 'our client is',
            'job', 'work', 'employment', 'career', 'hiring', 'vacancy',
            'trade', 'service', 'automotive', 'retail', 'manufacturing'
        ]
        
        if any(indicator in desc_lower for indicator in job_content_indicators):
            return True
        
        # If description is short but has professional content, accept it
        if len(desc.strip()) < 100:
            # For short descriptions, be more lenient
            short_desc_indicators = [
                'looking for', 'we are', 'seeking', 'required', 'needed',
                'position available', 'opportunity', 'employment'
            ]
            if any(indicator in desc_lower for indicator in short_desc_indicators):
                return True
        
        # If none of the above, still accept if it's not obviously invalid
        # (This makes the validation more permissive)
        return True

    def _clean_location_text(self, text: str) -> str:
        if not text:
            return ''
        # Remove salary-related words and collapse whitespace
        cleaned = re.sub(r'(?i)\b(hour|hr|salary|per\s*hour)\b', '', text)
        cleaned = re.sub(r'[\n\r\t]+', ' ', cleaned).strip()
        # Reduce repeated spaces
        cleaned = re.sub(r'\s{2,}', ' ', cleaned)
        return cleaned

    def _merge_prefer_panel(self, base: dict, fallback: dict) -> dict:
        """Merge two job_data dicts preferring panel-extracted values and validating fields."""
        out = dict(base)
        for k, v in (fallback or {}).items():
            if not v:
                continue
            if k == 'location':
                # don't allow salary lines to override location; validate
                if not out.get('location') and self._is_valid_location(v):
                    out['location'] = self._clean_location_text(v)
            elif k == 'title':
                if not out.get('title') and self._is_valid_title(v):
                    out['title'] = v.strip()
            else:
                if not out.get(k):
                    out[k] = v
        return out

    def _save_job_to_database_sync(self, job_data):
        """
        Save job data to database in a separate thread to avoid async context issues.
        
        Args:
            job_data (dict): Job information dictionary
            
        Returns:
            bool: Success status
        """
        def save_job():
            try:
                # Close any existing connections
                connections.close_all()
                
                with transaction.atomic():
                    # Get or create company
                    company = self._get_or_create_company(
                        job_data['company_name'],
                        job_data.get('company_logo', '')
                    )
                    
                    # Get or create location
                    location = self._get_or_create_location(job_data['location'])
                    
                    # Parse salary information
                    salary_info = self._parse_salary_info(job_data.get('salary_text', ''))
                    
                    # Categorize job
                    category = self._categorize_job(
                        job_data['title'], 
                        job_data['description'],
                        job_data.get('categories', '')
                    )
                    
                    # Normalize job type
                    job_type = 'full_time'  # Default
                    if job_data.get('employment_type'):
                        emp_type = job_data['employment_type'].lower()
                        if 'temporary' in emp_type or 'temp' in emp_type:
                            job_type = 'temporary'
                        elif 'contract' in emp_type:
                            job_type = 'contract'
                        elif 'casual' in emp_type:
                            job_type = 'casual'
                        elif 'part' in emp_type:
                            job_type = 'part_time'
                    
                    # Create unique external URL
                    external_url = job_data.get('job_url', f"{self.jobs_url}#{uuid.uuid4()}")
                    
                    # Check if job already exists
                    existing_job = JobPosting.objects.filter(external_url=external_url).first()
                    if existing_job:
                        logger.info(f"Job already exists: {job_data['title']} at {company.name}")
                        return False
                    
                    # Create job posting
                    job_posting = JobPosting.objects.create(
                        title=job_data['title'],
                        description=job_data['description'],
                        company=company,
                        location=location,
                        posted_by=self.scraper_user,
                        job_category=category,
                        job_type=job_type,
                        salary_min=salary_info.get('min'),
                        salary_max=salary_info.get('max'),
                        salary_currency=salary_info.get('currency', 'AUD'),
                        salary_type=salary_info.get('type', 'yearly'),
                        salary_raw_text=salary_info.get('raw_text', ''),
                        external_source='adecco.com.au',
                        external_url=external_url,
                        posted_ago='',
                        date_posted=timezone.now(),
                        status='active',
                        additional_info={
                            'scraper_version': '1.0',
                            'scraped_from': 'adecco.com.au',
                            'adecco_categories': job_data.get('categories', ''),
                            'employment_type': job_data.get('employment_type', ''),
                            'work_type': job_data.get('work_type', ''),
                            'original_data': job_data
                        }
                    )
                    
                    logger.info(f"SUCCESS: Saved job: {job_data['title']} at {company.name} - {location.name}")
                    return True
                    
            except Exception as e:
                logger.error(f"ERROR: Error saving job to database: {e}")
                return False
        
        # Run in separate thread to avoid async context issues
        result = [False]
        
        def run_save():
            result[0] = save_job()
        
        thread = threading.Thread(target=run_save)
        thread.start()
        thread.join()
        
        return result[0]
    
    def scrape_jobs(self):
        """
        Main method to scrape jobs from adecco.com/en-au/jobs.
        
        Returns:
            list: List of scraped job data
        """
        logger.info(f"Starting Adecco Australia scraping session...")
        logger.info(f"Target: {self.max_jobs or 'No limit'} jobs")
        
        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            
            # Create context with stealth settings
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            # [cursor:reason] Be resilient to heavy third-party requests; avoid 'networkidle' stalls
            page.set_default_navigation_timeout(60000)
            
            try:
                # Go to jobs page with minimal retry to handle intermittent network/CDN stalls
                logger.info(f"Navigating to {self.jobs_url}")
                for attempt in range(1, 3):
                    try:
                        # [cursor:reason] Some sites never reach 'networkidle'; wait for DOM
                        page.goto(self.jobs_url, wait_until='domcontentloaded', timeout=60000)
                        break
                    except PlaywrightTimeoutError:
                        logger.warning(f"Initial navigation timeout (attempt {attempt}). Retrying once...")
                        if attempt == 2:
                            raise
                
                # Handle cookie consent if present
                try:
                    cookie_button = page.query_selector('button:has-text("Yes"), button:has-text("Accept")')
                    if cookie_button:
                        cookie_button.click()
                        time.sleep(1)
                        logger.info("Accepted cookies")
                except:
                    pass
                
                # Wait for job listings to load
                page.wait_for_selector('body', timeout=10000)
                time.sleep(random.uniform(3, 5))
                
                page_number = 1
                jobs_scraped = 0
                total_pages = None
                
                while (self.max_jobs is None) or (jobs_scraped < self.max_jobs):
                    logger.info(f"Scraping page {page_number}...")
                    
                    # Detect total pages on first page
                    if total_pages is None:
                        total_pages = self._detect_total_pages(page)
                        logger.info(f"Total pages detected: {total_pages}")
                    
                    # Find job elements - use the CORRECT selectors found from debugging
                    job_selectors = [
                        '.card',                                             # Primary: 10 job cards per page
                        'article',                                           # Alternative: 10 article elements per page
                        'div[class*="card"].mb2',                           # More specific card selector
                    ]
                    
                    # Prefer left-column card detection to avoid selecting the right panel
                    job_elements = self._find_left_column_job_cards(page)
                    if job_elements:
                        logger.info(f"Found {len(job_elements)} left-column job cards")
                    else:
                        # fallback to previous generic selectors
                        for selector in job_selectors:
                            try:
                                elements = page.query_selector_all(selector)
                                if elements:
                                    job_elements = elements
                                    logger.info(f"Found {len(job_elements)} job elements using selector: {selector}")
                                    break
                            except Exception as e:
                                logger.debug(f"Selector {selector} failed: {e}")
                                continue
                    
                    if not job_elements:
                        logger.warning("No job elements found on page")
                        break
                    
                    # Check if we've exceeded total pages
                    if total_pages and page_number > total_pages:
                        logger.info(f"Reached end of pages (page {page_number} > {total_pages}). Stopping.")
                        break
                    
                    # Extract data from each job element
                    job_data_list = []
                    for i, job_element in enumerate(job_elements):
                        if self.max_jobs is not None and jobs_scraped >= self.max_jobs:
                            break
                        
                        logger.info(f"Extracting job {i+1}/{len(job_elements)}")
                        # Wait for unique update after clicking to avoid same right-panel data for multiple cards
                        prev_title = self._get_right_panel_title(page)
                        prev_url = page.url
                        try:
                            job_element.scroll_into_view_if_needed()
                            job_element.click(timeout=5000)
                        except Exception:
                            pass
                        self._wait_for_panel_change(page, prev_title, prev_url, timeout_sec=6.0)

                        # Now extract from panel
                        panel_data = self._click_and_extract_from_panel(page, job_element)
                        # If title still missing, fallback to parsing the left card HTML
                        parsed = None
                        if not self._is_valid_title(panel_data.get('title', '')):
                            parsed = self._extract_job_data_from_listing(job_element)
                        # Merge, preferring panel values after validation
                        job_data = self._merge_prefer_panel(panel_data, parsed or {})
                        
                        if job_data and self._is_valid_title(job_data.get('title', '')):
                            job_data_list.append(job_data)
                        else:
                            logger.info(f"Skipped invalid job data on page {page_number}: {job_data.get('title', 'No title')} - {job_data}")
                        
                        # Small delay between extractions
                        time.sleep(random.uniform(0.1, 0.3))
                    
                    # Now get full descriptions and save jobs to database
                    for job_data in job_data_list:
                        if self.max_jobs is not None and jobs_scraped >= self.max_jobs:
                            break
                        
                        # Initialize details to avoid reference errors
                        details = {}
                        
                        # Get full job description if URL is available
                        if job_data.get('job_url'):
                            try:
                                # Save the salary we extracted from the job card
                                card_salary = job_data.get('salary_text', '')
                                
                                details = self._get_full_job_description(
                                    page, job_data['job_url'], job_data.get('title', '')
                                )
                                # Merge back dynamic fields to avoid static placeholders
                                for k in ['title', 'description', 'location', 'salary_text', 'employment_type', 'work_type']:
                                    if details.get(k):
                                        job_data[k] = details[k]
                                
                                # Restore the salary from job card if we had one (more reliable than detail page)
                                if card_salary:
                                    job_data['salary_text'] = card_salary
                                
                            except Exception as e:
                                logger.error(f"Failed to get description for {job_data['title']}: {e}")
                        
                        # Validate we have a real, dynamic title and reasonable content before saving
                        is_valid_title = self._is_valid_title(job_data.get('title', ''))
                        is_valid_desc = self._is_valid_description(job_data.get('description', ''))
                        
                        if is_valid_title:
                            # Clean/validate location - use extracted location if available
                            if not job_data.get('location') or not self._is_valid_location(job_data['location']):
                                job_data['location'] = details.get('location', '') or job_data.get('location', '')
                            
                            # Save if we have a valid title and either:
                            # 1. Valid description, OR
                            # 2. Job title contains legitimate job keywords, OR 
                            # 3. Job title is a category that typically contains multiple job types
                            job_title_lower = job_data.get('title', '').lower()
                            
                            # Legitimate job keywords
                            legitimate_keywords = [
                                'manager', 'engineer', 'technician', 'operator', 'assistant', 'beater', 
                                'electrician', 'plumber', 'sales', 'analyst', 'developer', 'designer',
                                'associate', 'specialist', 'coordinator', 'supervisor', 'looking for'
                            ]
                            
                            # Job category titles that are legitimate (even without detailed descriptions)
                            job_categories = [
                                'trades and services', 'automotive', 'aerospace', 'retail', 'wholesale',
                                'e-commerce', 'manufacturing', 'transport', 'logistics', 'healthcare',
                                'medical', 'construction', 'infrastructure', 'hospitality', 'events'
                            ]
                            
                            has_legitimate_keyword = any(keyword in job_title_lower for keyword in legitimate_keywords)
                            is_job_category = any(category in job_title_lower for category in job_categories)
                            
                            if is_valid_desc or has_legitimate_keyword or is_job_category:
                                if self._save_job_to_database_sync(job_data):
                                    jobs_scraped += 1
                                    self.scraped_jobs.append(job_data)
                                    logger.info(f"Saved job {jobs_scraped}/{self.max_jobs or '∞'}: {job_data['title']}")
                            else:
                                logger.info(f"Skipping job with invalid description: {job_data.get('title', '')}")
                        else:
                            logger.info(f"Skipping job with invalid title: {job_data.get('title', '')}")
                        
                        # Add delay between operations
                        time.sleep(random.uniform(1.0, 2.0))
                    
                    # Try to go to next page
                    if (self.max_jobs is None) or (jobs_scraped < self.max_jobs):
                        # Check if we've reached the detected total pages
                        if total_pages and page_number >= total_pages:
                            logger.info(f"Reached last page ({page_number} of {total_pages}). Stopping.")
                            break
                        
                        try:
                            # Check if page is still valid before proceeding
                            if page.is_closed():
                                logger.error("Page has been closed. Cannot navigate to next page.")
                                break
                            
                            # Try to find next page button - corrected based on actual HTML structure
                            next_selectors = [
                                'button[aria-label="Navigate next"]',                    # Primary: Navigate next arrow button
                                'button[title="Navigate next"]',                        # Alternative: Same button with title
                                '.paginator-icon[aria-label="Navigate next"]',          # More specific selector
                                'button:has(span.material-icons:has-text("navigate_next"))',  # Button containing navigate_next icon
                                'span.material-icons:has-text("navigate_next")',        # Direct icon selector
                                '.paginator button[aria-label*="Navigate next"]',       # Within paginator container
                                'button.paginator-page:last-of-type',                   # Last page number button as fallback
                                'a:has-text("Next")',                                   # Legacy text-based (likely won't work)
                                'button:has-text("Next")'                               # Legacy text-based (likely won't work)
                            ]
                            
                            next_button = None
                            for selector in next_selectors:
                                try:
                                    next_button = page.query_selector(selector)
                                    if next_button:
                                        logger.info(f"Found next button using selector: {selector}")
                                        break
                                except Exception as e:
                                    logger.debug(f"Error with selector {selector}: {e}")
                                    continue
                            
                            # Use DIRECT URL navigation instead of clicking (more reliable)
                            try:
                                next_page_url = f"{self.jobs_url}?pg={page_number + 1}"
                                logger.info(f"Navigating directly to page {page_number + 1}: {next_page_url}")
                                
                                # [cursor:reason] Avoid networkidle on sites with long-polling; extend timeout
                                page.goto(next_page_url, wait_until='domcontentloaded', timeout=60000)
                                time.sleep(3)  # Wait for content to fully load
                                
                                # Verify job cards loaded
                                test_cards = page.query_selector_all('.card')
                                if len(test_cards) >= 8:  # Should have ~10 cards
                                    page_number += 1
                                    logger.info(f"✅ Successfully navigated to page {page_number} - found {len(test_cards)} job cards")
                                else:
                                    logger.info(f"❌ Page {page_number + 1} has insufficient job cards ({len(test_cards)}). End of pagination.")
                                    break
                                
                            except Exception as nav_error:
                                logger.error(f"Direct navigation failed: {nav_error}")
                                break
                            else:
                                # If no next button found, try direct navigation anyway
                                try:
                                    next_page_url = f"{self.jobs_url}?pg={page_number + 1}"
                                    logger.info(f"No next button found. Trying direct navigation to: {next_page_url}")
                                    page.goto(next_page_url, wait_until='domcontentloaded', timeout=60000)
                                    time.sleep(3)
                                    
                                    test_cards = page.query_selector_all('.card')
                                    if len(test_cards) >= 8:
                                        page_number += 1
                                        logger.info(f"✅ Direct navigation successful - found {len(test_cards)} job cards")
                                    else:
                                        logger.info("No more job cards found. End of pagination.")
                                        break
                                except:
                                    logger.info("Direct navigation failed. End of pagination.")
                                    break
                                
                        except Exception as e:
                            logger.error(f"Error during page navigation: {e}")
                            break
                    else:
                        break
                
            except Exception as e:
                logger.error(f"Scraping error: {e}")
                # Check if it's a browser-related error
                if "closed" in str(e).lower() or "target" in str(e).lower():
                    logger.error("Browser context lost. Cannot continue scraping.")
                else:
                    logger.error(f"Unexpected error: {e}")
            
            finally:
                try:
                    browser.close()
                except Exception as close_error:
                    logger.warning(f"Error closing browser: {close_error}")
        
        logger.info(f"Scraping completed! Total jobs scraped: {len(self.scraped_jobs)}")
        return self.scraped_jobs
    
    def get_stats(self):
        """Get scraping statistics."""
        total_jobs = JobPosting.objects.filter(external_source='adecco.com.au').count()
        recent_jobs = JobPosting.objects.filter(
            external_source='adecco.com.au',
            scraped_at__gte=timezone.now() - timedelta(days=1)
        ).count()
        
        return {
            'total_jobs_in_db': total_jobs,
            'recent_jobs_24h': recent_jobs,
            'current_session': len(self.scraped_jobs)
        }


def main():
    """Main function to run the scraper."""
    # Get max jobs from command line argument
    max_jobs = None
    headless = True
    if len(sys.argv) > 1:
        # Parse positional max_jobs and optional flags
        for arg in sys.argv[1:]:
            if arg.isdigit():
                try:
                    max_jobs = int(arg)
                except ValueError:
                    logger.error("Invalid max_jobs argument. Using unlimited.")
            else:
                if arg in ("--show", "--headed", "--headful"):
                    headless = False
                if arg in ("--headless",):
                    headless = True
    
    # Create and run scraper
    scraper = AdeccoAustraliaJobScraper(max_jobs=max_jobs, headless=headless)
    
    try:
        # Scrape jobs
        scraped_jobs = scraper.scrape_jobs()
        
        # Print statistics
        stats = scraper.get_stats()
        logger.info("=" * 60)
        logger.info("ADECCO AUSTRALIA SCRAPING STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total jobs in database: {stats['total_jobs_in_db']}")
        logger.info(f"Jobs scraped in last 24h: {stats['recent_jobs_24h']}")
        logger.info(f"Jobs scraped this session: {stats['current_session']}")
        logger.info("=" * 60)
        
        # Print sample jobs
        if scraped_jobs:
            logger.info("Sample scraped jobs:")
            for i, job in enumerate(scraped_jobs[:3]):
                logger.info(f"{i+1}. {job['title']} at {job['company_name']} - {job['location']}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


def run(max_jobs=None, headless=True):
    """Automation entrypoint for Adecco Australia scraper."""
    try:
        scraper = AdeccoAustraliaJobScraper(max_jobs=max_jobs, headless=headless)
        scraped_jobs = scraper.scrape_jobs()
        stats = scraper.get_stats()
        return {
            'success': True,
            'jobs_scraped': len(scraped_jobs) if isinstance(scraped_jobs, list) else None,
            'stats': stats,
            'message': 'Adecco scraping completed'
        }
    except Exception as e:
        try:
            logger.error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }

