#!/usr/bin/env python3
"""
Professional Robert Half Australia Job Scraper using Playwright
===============================================================

Advanced Playwright-based scraper for Robert Half Australia (https://www.roberthalf.com/au/en/jobs/all/all) 
that integrates with your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Supports recruitment agency job listings

Features:
- 🎯 Smart job data extraction from Robert Half Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional recruitment job categorization

Usage:
    python roberthalf_australia_scraper.py [job_limit]
    
Examples:
    python roberthalf_australia_scraper.py 30    # Scrape 30 jobs
    python roberthalf_australia_scraper.py       # Scrape all available jobs (default: 50)
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

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('roberthalf_australia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RobertHalfAustraliaScraper:
    """
    Professional scraper for Robert Half Australia job listings
    """
    
    def __init__(self, max_jobs=50, headless=True, max_pages=None):
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.headless = headless
        self.base_url = "https://www.roberthalf.com"
        self.jobs_url = "https://www.roberthalf.com/au/en/jobs/all/all"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        self.jobs_per_page = 25  # Robert Half shows 25 jobs per page
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'new_jobs': 0,
            'duplicate_jobs': 0,
            'errors': 0,
            'companies_created': 0,
            'locations_created': 0,
            'pages_scraped': 0,
            'total_pages_found': 0,
            'total_jobs_available': 0
        }
        
        # Get or create default user for job postings
        self.default_user, _ = User.objects.get_or_create(
            username='roberthalf_australia_scraper',
            defaults={'email': 'scraper@roberthalf.com.au'}
        )
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        logger.info("Robert Half Australia Scraper initialized")
        logger.info(f"Job limit: {max_jobs}")

    def human_delay(self, min_delay=1, max_delay=3):
        """Add human-like delays between requests"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def get_pagination_info(self, page):
        """Extract pagination information from Robert Half pagination structure"""
        try:
            # Look for the pagination container
            pagination_container = page.query_selector('.rhcl-pagination')
            if not pagination_container:
                logger.info("No pagination found - single page")
                return 1, 1, 0
            
            # Extract total jobs count from "1-25 of 202 jobs" text
            total_jobs = 0
            total_text_element = pagination_container.query_selector('.rhcl-pagination--numbered-label rhcl-typography')
            if total_text_element:
                total_text = total_text_element.inner_text().strip()
                # Parse "1-25 of 202 jobs" format
                import re
                total_match = re.search(r'of (\d+) jobs?', total_text)
                if total_match:
                    total_jobs = int(total_match.group(1))
                    logger.info(f"Found total jobs: {total_jobs}")
            
            # Calculate total pages (25 jobs per page)
            total_pages = (total_jobs + self.jobs_per_page - 1) // self.jobs_per_page if total_jobs > 0 else 1
            
            # Find current page
            current_page = 1
            current_page_element = pagination_container.query_selector('.rhcl-pagination--numbered-pagination--pagination--tile-selected rhcl-typography')
            if current_page_element:
                try:
                    current_page = int(current_page_element.inner_text())
                except ValueError:
                    current_page = 1
            
            logger.info(f"Pagination info: Current page {current_page}, Total pages: {total_pages}, Total jobs: {total_jobs}")
            return current_page, total_pages, total_jobs
            
        except Exception as e:
            logger.warning(f"Error detecting pagination: {e}")
            return 1, 1, 0

    def build_page_url(self, page_number):
        """Build URL for a specific page"""
        if page_number == 1:
            return self.jobs_url
        else:
            return f"{self.jobs_url}?pagenumber={page_number}"

    def navigate_to_page(self, page, page_number):
        """Navigate to a specific page"""
        try:
            page_url = self.build_page_url(page_number)
            logger.info(f"Navigating to page {page_number}: {page_url}")
            page.goto(page_url)
            self.human_delay(3, 5)
            
            # Wait for content to load
            page.wait_for_selector('rhcl-job-card, body', timeout=15000)
            
            # Verify we're on the correct page
            pagination_container = page.query_selector('.rhcl-pagination')
            if pagination_container:
                current_page_element = pagination_container.query_selector('.rhcl-pagination--numbered-pagination--pagination--tile-selected rhcl-typography')
                if current_page_element:
                    current_page_text = current_page_element.inner_text().strip()
                    if current_page_text == str(page_number):
                        logger.info(f"Successfully navigated to page {page_number}")
                        return True
                    else:
                        logger.warning(f"Expected page {page_number}, but found page {current_page_text}")
            
            return True  # Assume success if we can't verify
            
        except Exception as e:
            logger.error(f"Error navigating to page {page_number}: {e}")
            return False

    def parse_salary_info(self, salary_text):
        """Extract salary information from text"""
        if not salary_text:
            return None, None, 'yearly', ''
            
        salary_text = salary_text.strip()
        original_text = salary_text
        
        # Common salary patterns from Robert Half
        patterns = [
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*AUD\s*/\s*(hour|day|week|month|year)',  # 65 - 75 AUD / hour
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*AUD\s*per\s*(hour|day|week|month|year)',  # 65 - 75 AUD per hour
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*AUD\s*/\s*(annum)',  # 80000 - 100000 AUD / annum
            r'(\d{1,3}(?:,\d{3})*)\s*AUD\s*/\s*(hour|day|week|month|year)',  # 75 AUD / hour
            r'(\d{1,3}(?:,\d{3})*)\s*AUD\s*per\s*(hour|day|week|month|year)',  # 75 AUD per hour
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)',  # 80000 - 100000
        ]
        
        salary_min = None
        salary_max = None
        salary_type = 'yearly'
        
        for pattern in patterns:
            match = re.search(pattern, salary_text, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) >= 3:  # Range with period
                        salary_min = Decimal(groups[0].replace(',', ''))
                        salary_max = Decimal(groups[1].replace(',', ''))
                        period = groups[2].lower()
                        if period in ['hour', 'hourly']:
                            salary_type = 'hourly'
                        elif period in ['day', 'daily']:
                            salary_type = 'daily'
                        elif period in ['week', 'weekly']:
                            salary_type = 'weekly'
                        elif period in ['month', 'monthly']:
                            salary_type = 'monthly'
                        elif period in ['annum', 'year', 'yearly']:
                            salary_type = 'yearly'
                        break
                    elif len(groups) == 2 and groups[1] in ['hour', 'day', 'week', 'month', 'year', 'annum']:  # Single amount with period
                        salary_min = Decimal(groups[0].replace(',', ''))
                        period = groups[1].lower()
                        if period in ['hour', 'hourly']:
                            salary_type = 'hourly'
                        elif period in ['day', 'daily']:
                            salary_type = 'daily'
                        elif period in ['week', 'weekly']:
                            salary_type = 'weekly'
                        elif period in ['month', 'monthly']:
                            salary_type = 'monthly'
                        elif period in ['annum', 'year', 'yearly']:
                            salary_type = 'yearly'
                        break
                    elif len(groups) == 2:  # Range without explicit period
                        salary_min = Decimal(groups[0].replace(',', ''))
                        salary_max = Decimal(groups[1].replace(',', ''))
                        # Determine type based on amount ranges
                        if salary_min < 500:  # Likely hourly
                            salary_type = 'hourly'
                        elif salary_min < 5000:  # Likely weekly/monthly
                            salary_type = 'weekly'
                        else:  # Likely yearly
                            salary_type = 'yearly'
                        break
                except (ValueError, AttributeError):
                    continue
        
        return salary_min, salary_max, salary_type, original_text

    def parse_location(self, location_text):
        """Parse location string into normalized location data"""
        if not location_text:
            return 'Australia'
            
        location_text = location_text.strip()
        
        # Clean up location text
        location_text = re.sub(r'\s+', ' ', location_text)
        
        # Handle common Robert Half location formats
        # Examples: "Parramatta, New South Wales", "Melbourne, Victoria", "Melbourne CBD, Victoria"
        
        # Australian state abbreviations and full names
        aus_states = {
            'VIC': 'Victoria', 'NSW': 'New South Wales', 'QLD': 'Queensland',
            'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory', 'NT': 'Northern Territory'
        }
        
        # Extract state if present
        for abbr, full_name in aus_states.items():
            if abbr in location_text or full_name in location_text:
                # Return formatted location
                if ',' in location_text:
                    parts = [p.strip() for p in location_text.split(',')]
                    city = parts[0]
                    return f"{city}, {full_name}"
                else:
                    return f"{location_text}, {full_name}"
        
        # Return as-is if no state found
        return location_text

    def get_or_create_company(self, company_name, company_url=None):
        """Get or create company with proper handling"""
        if not company_name or company_name.strip() == '':
            company_name = 'Robert Half'
            
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                website=company_url if company_url else 'https://www.roberthalf.com/au',
                description=f'Professional recruitment services company posting jobs via Robert Half Australia'
            )
            self.stats['companies_created'] += 1
            logger.info(f"Created new company: {company_name}")
            
        return company

    def get_or_create_location(self, location_text):
        """Get or create location with proper handling"""
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
            
        location_text = location_text.strip()
        
        # Truncate to fit database field (varchar 100)
        location_text = location_text[:100]
        
        # Try to find existing location (case-insensitive)
        location = Location.objects.filter(
            name__iexact=location_text
        ).first()
        
        if not location:
            # Parse location components
            city = location_text
            state = 'Unknown'
            country = 'Australia'
            
            # Extract state if present
            aus_states = {
                'VIC': 'Victoria', 'NSW': 'New South Wales', 'QLD': 'Queensland',
                'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
                'ACT': 'Australian Capital Territory', 'NT': 'Northern Territory'
            }
            
            for abbr, full_name in aus_states.items():
                if abbr in location_text or full_name in location_text:
                    state = full_name
                    if ',' in location_text:
                        city = location_text.split(',')[0].strip()
                    break
            
            location = Location.objects.create(
                name=location_text,
                city=city,
                state=state,
                country=country
            )
            self.stats['locations_created'] += 1
            logger.info(f"Created new location: {location_text}")
            
        return location

    def extract_job_data(self, job_element):
        """Extract job data from Robert Half custom job card element"""
        try:
            job_data = {}
            
            # Extract data from rhcl-job-card attributes
            # All the data is stored as attributes in the custom web component
            job_data['title'] = job_element.get_attribute('headline') or ''
            job_data['url'] = job_element.get_attribute('destination') or ''
            job_data['location'] = job_element.get_attribute('location') or 'Australia'
            job_data['job_type'] = job_element.get_attribute('type') or 'full_time'
            job_data['work_mode'] = job_element.get_attribute('worksite') or ''
            job_data['description'] = job_element.get_attribute('copy') or ''
            job_data['job_id'] = job_element.get_attribute('job-id') or ''
            job_data['date_posted'] = job_element.get_attribute('date') or ''
            
            # Extract salary information from attributes
            salary_min_attr = job_element.get_attribute('salary-min')
            salary_max_attr = job_element.get_attribute('salary-max')
            salary_currency_attr = job_element.get_attribute('salary-currency') or 'AUD'
            salary_period_attr = job_element.get_attribute('salary-period') or 'yearly'
            
            # Process salary data
            if salary_min_attr:
                try:
                    job_data['salary_min'] = Decimal(salary_min_attr)
                except:
                    job_data['salary_min'] = None
            else:
                job_data['salary_min'] = None
                
            if salary_max_attr:
                try:
                    job_data['salary_max'] = Decimal(salary_max_attr)
                except:
                    job_data['salary_max'] = None
            else:
                job_data['salary_max'] = None
            
            # Map salary period to our database format
            period_mapping = {
                'hour': 'hourly',
                'day': 'daily', 
                'week': 'weekly',
                'month': 'monthly',
                'year': 'yearly',
                'annum': 'yearly'
            }
            job_data['salary_type'] = period_mapping.get(salary_period_attr, 'yearly')
            
            # Build salary raw text for display
            if job_data['salary_min'] and job_data['salary_max']:
                job_data['salary_raw_text'] = f"{job_data['salary_min']} - {job_data['salary_max']} {salary_currency_attr} / {salary_period_attr}"
            elif job_data['salary_min']:
                job_data['salary_raw_text'] = f"{job_data['salary_min']} {salary_currency_attr} / {salary_period_attr}"
            else:
                job_data['salary_raw_text'] = ''
            
            # Clean up and process job type
            job_type_raw = job_data['job_type'].lower() if job_data['job_type'] else ''
            if job_type_raw == 'project':
                job_data['job_type'] = 'contract'  # Projects are typically contract work
            elif job_type_raw == 'permanent placement':
                job_data['job_type'] = 'permanent'
            elif job_type_raw == 'contract/temporary talent':
                job_data['job_type'] = 'contract'
            elif 'permanent' in job_type_raw:
                job_data['job_type'] = 'permanent'
            elif 'contract' in job_type_raw or 'temporary' in job_type_raw:
                job_data['job_type'] = 'contract'
            elif 'part-time' in job_type_raw:
                job_data['job_type'] = 'part_time'
            else:
                job_data['job_type'] = 'full_time'  # Default
            
            # Clean up work mode
            worksite_raw = job_data['work_mode'].lower() if job_data['work_mode'] else ''
            if worksite_raw == 'onsite':
                job_data['work_mode'] = 'On-site'
            elif worksite_raw == 'remote':
                job_data['work_mode'] = 'Remote'
            elif worksite_raw == 'hybrid':
                job_data['work_mode'] = 'Hybrid'
            else:
                job_data['work_mode'] = ''
            
            # Process location - already clean from attribute
            job_data['location'] = self.parse_location(job_data['location'])
            
            # Process posted date
            if job_data['date_posted']:
                try:
                    # Parse ISO date: 2025-07-22T06:55:54Z
                    date_obj = datetime.fromisoformat(job_data['date_posted'].replace('Z', '+00:00'))
                    days_ago = (datetime.now(date_obj.tzinfo) - date_obj).days
                    if days_ago == 0:
                        job_data['posted_ago'] = 'Today'
                    elif days_ago == 1:
                        job_data['posted_ago'] = '1 day ago'
                    else:
                        job_data['posted_ago'] = f'{days_ago} days ago'
                except:
                    job_data['posted_ago'] = 'Recently'
            else:
                job_data['posted_ago'] = ''
            
            # Validate essential data
            if not job_data['title'] or len(job_data['title']) < 2:
                logger.warning("Job title too short or empty")
                return None
            
            if not job_data['url']:
                logger.warning("No job URL found")
                return None
            
            # Decode HTML entities in title and description
            import html
            job_data['title'] = html.unescape(job_data['title'])
            job_data['description'] = html.unescape(job_data['description'])
            
            # Truncate only necessary fields for database constraints
            job_data['title'] = job_data['title'][:200]  # Database constraint
            job_data['posted_ago'] = job_data['posted_ago'][:50]  # Database constraint
            job_data['salary_raw_text'] = job_data['salary_raw_text'][:200]  # Display purposes
            # Note: description is kept without length restrictions
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None

    def get_job_details(self, job_url, page):
        """Get detailed job information from the job detail page"""
        try:
            page.goto(job_url)
            self.human_delay(2, 4)
            
            # Wait for content to load
            page.wait_for_selector('body', timeout=10000)
            
            job_details = {
                'description': 'Professional opportunity with Robert Half Australia.',
                'company': 'Robert Half',
                'experience_level': '',
                'additional_info': {}
            }
            
            # Extract full job description from Robert Half specific structure
            description = ''
            
            # Primary selector: Robert Half job description container
            description_selectors = [
                '[data-testid="job-details-description"]',  # Primary Robert Half selector
                'div[slot="description"]',                   # Alternative slot-based selector
                '.job-description',
                '.job-content',
                '.description',
                '.content',
                '.job-details'
            ]
            
            for selector in description_selectors:
                try:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        # Get the full HTML content and convert to clean text
                        desc_html = desc_element.inner_html()
                        
                        # Convert HTML to text while preserving structure
                        desc_text = desc_element.inner_text().strip()
                        
                        # If we got substantial content, use it
                        if len(desc_text) > 100:
                            description = desc_text
                            logger.info(f"Found description using selector: {selector} ({len(desc_text)} characters)")
                            break
                            
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Fallback: Extract from table structure (as seen in your HTML)
            if not description or len(description) < 200:
                try:
                    # Look for table-based content (common in Robert Half job descriptions)
                    table_element = page.query_selector('table td')
                    if table_element:
                        table_text = table_element.inner_text().strip()
                        if len(table_text) > 200:
                            description = table_text
                            logger.info(f"Found description in table structure ({len(table_text)} characters)")
                except Exception as e:
                    logger.debug(f"Error extracting from table: {e}")
            
            # Fallback: try to get main content
            if not description or len(description) < 200:
                try:
                    main_content = page.query_selector('main, .main, #main, .container')
                    if main_content:
                        # Get all substantial paragraphs and combine
                        paragraphs = main_content.query_selector_all('p, li, div')
                        content_parts = []
                        for element in paragraphs:
                            text = element.inner_text().strip()
                            # Include any substantial content
                            if len(text) > 30 and not any(skip in text.lower() for skip in ['cookie', 'privacy', 'footer']):
                                content_parts.append(text)
                        
                        if content_parts:
                            description = '\n\n'.join(content_parts)
                            logger.info(f"Extracted description from main content ({len(description)} characters)")
                except Exception as e:
                    logger.warning(f"Error extracting from main content: {e}")
            
            # Clean up the description
            if description:
                # Remove email tracking images and privacy notices
                import re
                # Remove email tracking references
                description = re.sub(r'By clicking.*?at this time\.', '', description, flags=re.DOTALL)
                # Remove extra whitespace
                description = re.sub(r'\n\s*\n\s*\n', '\n\n', description)
                description = description.strip()
                
                # Store the full description without any length restrictions
                job_details['description'] = description
                logger.info(f"Final description length: {len(description)} characters")
            
            # Extract company name (might be the actual client company)
            company_selectors = [
                '.company-name',
                '.client-name', 
                '.company',
                '.employer',
                'h1, h2, h3'
            ]
            
            for selector in company_selectors:
                try:
                    company_element = page.query_selector(selector)
                    if company_element:
                        company_text = company_element.inner_text().strip()
                        if company_text and 'robert half' not in company_text.lower() and len(company_text) > 3:
                            job_details['company'] = company_text
                            logger.info(f"Found client company: {company_text}")
                            break
                except Exception:
                    continue
            
            # Extract experience level from job description
            if description:
                desc_lower = description.lower()
                if any(term in desc_lower for term in ['senior', 'sr.', 'lead', 'principal']):
                    job_details['experience_level'] = 'Senior'
                elif any(term in desc_lower for term in ['junior', 'jr.', 'graduate', 'entry']):
                    job_details['experience_level'] = 'Junior'
                elif any(term in desc_lower for term in ['manager', 'director', 'head of', 'vp', 'executive']):
                    job_details['experience_level'] = 'Executive'
                elif any(term in desc_lower for term in ['mid-level', 'intermediate', '3-5 years', '2-4 years']):
                    job_details['experience_level'] = 'Mid-level'
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Could not get job details for {job_url}: {e}")
            return {
                'description': 'Professional opportunity with Robert Half Australia. Visit the job URL for full details.',
                'company': 'Robert Half',
                'experience_level': '',
                'additional_info': {}
            }

    def categorize_job(self, title, description):
        """Categorize job using the categorization service"""
        try:
            category = self.categorization_service.categorize_job(title, description)
            
            # Additional Robert Half specific categorizations
            title_lower = title.lower()
            desc_lower = description.lower()
            
            # Robert Half specializes in certain areas
            if any(term in title_lower for term in ['financial controller', 'finance', 'accounting', 'bookkeeper']):
                return 'finance'
            elif any(term in title_lower for term in ['devops', 'developer', 'programmer', 'software', 'it']):
                return 'technology'
            elif any(term in title_lower for term in ['executive assistant', 'admin', 'office', 'coordinator']):
                return 'office_support'
            elif any(term in title_lower for term in ['recruitment', 'hr', 'human resources']):
                return 'hr'
            elif any(term in title_lower for term in ['marketing', 'digital', 'communications']):
                return 'marketing'
            elif any(term in title_lower for term in ['legal', 'lawyer', 'solicitor']):
                return 'legal'
            elif any(term in title_lower for term in ['consultant', 'advisory', 'consulting']):
                return 'consulting'
            
            return category
            
        except Exception as e:
            logger.warning(f"Error categorizing job: {e}")
            return 'other'

    def save_job(self, job_data, page):
        """Save job to database with proper error handling"""
        try:
            with transaction.atomic():
                # Check for duplicates
                existing_job = JobPosting.objects.filter(external_url=job_data['url']).first()
                if existing_job:
                    logger.info(f"Duplicate job found: {job_data['title']}")
                    self.stats['duplicate_jobs'] += 1
                    return False
                
                # Get detailed job information
                job_details = self.get_job_details(job_data['url'], page)
                
                # Get or create company
                company = self.get_or_create_company(job_details['company'])
                
                # Get or create location
                location = self.get_or_create_location(job_data['location'])
                
                # Categorize job
                full_description = job_details['description']
                category = self.categorize_job(job_data['title'], full_description)
                
                # Create job posting (no length restrictions on description)
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],  # Keep title limit for database constraint
                    description=full_description,  # Full description without any length restrictions
                    company=company,
                    location=location,
                    posted_by=self.default_user,
                    job_category=category,
                    job_type=job_data.get('job_type', 'full_time'),
                    experience_level=job_details.get('experience_level', '')[:100],  # Keep reasonable limit
                    work_mode=job_data.get('work_mode', '')[:50],  # Keep reasonable limit
                    salary_min=job_data.get('salary_min'),
                    salary_max=job_data.get('salary_max'),
                    salary_type=job_data.get('salary_type', 'yearly'),
                    salary_currency='AUD',
                    salary_raw_text=job_data.get('salary_raw_text', '')[:200],  # Keep for display purposes
                    external_source='roberthalf.com.au',
                    external_url=job_data['url'],  # No length restriction on URL
                    posted_ago=job_data.get('posted_ago', '')[:50],  # Keep reasonable limit
                    status='active',
                    additional_info={
                        'scrape_timestamp': datetime.now().isoformat(),
                        'source_page': 'Robert Half Australia',
                        'recruitment_agency': True,
                        'job_id': job_data.get('job_id', ''),
                        'original_copy': job_data.get('description', '')  # Store original short description too
                    }
                )
                
                logger.info(f"Saved job: {job_data['title']} at {company.name}")
                self.stats['new_jobs'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    def scrape_jobs(self):
        """Main scraping method with pagination support"""
        logger.info("Starting Robert Half Australia job scraping...")
        if self.max_pages:
            logger.info(f"Max pages to scrape: {self.max_pages}")
        
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=self.user_agent,
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            try:
                # Navigate to first page to get pagination info
                logger.info(f"Navigating to: {self.jobs_url}")
                page.goto(self.jobs_url)
                self.human_delay(3, 5)
                
                # Wait for page to load
                page.wait_for_selector('body', timeout=15000)
                
                # Accept cookies if needed
                try:
                    cookie_button = page.query_selector('button:has-text("Accept"), button:has-text("Continue"), .cookie-accept')
                    if cookie_button:
                        cookie_button.click()
                        self.human_delay(1, 2)
                except Exception:
                    pass
                
                # Get pagination information
                current_page, total_pages, total_jobs = self.get_pagination_info(page)
                self.stats['total_pages_found'] = total_pages
                self.stats['total_jobs_available'] = total_jobs
                
                logger.info(f"Found {total_jobs} total jobs across {total_pages} pages")
                
                # Determine which pages to scrape
                pages_to_scrape = []
                if self.max_pages:
                    pages_to_scrape = list(range(1, min(self.max_pages + 1, total_pages + 1)))
                else:
                    # Calculate pages needed based on max_jobs
                    pages_needed = (self.max_jobs + self.jobs_per_page - 1) // self.jobs_per_page
                    pages_to_scrape = list(range(1, min(pages_needed + 1, total_pages + 1)))
                
                logger.info(f"Will scrape {len(pages_to_scrape)} pages: {pages_to_scrape}")
                
                all_extracted_jobs = []
                
                # Scrape each page
                for page_num in pages_to_scrape:
                    try:
                        logger.info(f"=" * 60)
                        logger.info(f"SCRAPING PAGE {page_num} of {total_pages}")
                        logger.info(f"=" * 60)
                        
                        # Navigate to the page (skip navigation for page 1 as we're already there)
                        if page_num > 1:
                            success = self.navigate_to_page(page, page_num)
                            if not success:
                                logger.error(f"Failed to navigate to page {page_num}, skipping")
                                continue
                        
                        # Look for Robert Half custom job card elements
                        job_selectors = [
                            'rhcl-job-card',  # Primary: Robert Half custom job card component
                            '[data-testid*="job-card"]',  # Secondary: job cards with testid
                            '[data-testid*="job"]',  # Fallback: any job elements
                        ]
                        
                        job_elements = []
                        for selector in job_selectors:
                            elements = page.query_selector_all(selector)
                            if elements:
                                job_elements = elements
                                logger.info(f"Found {len(elements)} jobs using selector: {selector}")
                                break
                        
                        if not job_elements:
                            logger.warning(f"No job listings found on page {page_num}")
                            continue
                        
                        # Extract job data from this page
                        page_jobs = []
                        logger.info(f"Extracting data from {len(job_elements)} jobs on page {page_num}...")
                        
                        for i, job_element in enumerate(job_elements, 1):
                            try:
                                logger.info(f"Extracting job {i}/{len(job_elements)} from page {page_num}")
                                job_data = self.extract_job_data(job_element)
                                if job_data:
                                    job_data['page_number'] = page_num  # Track which page this came from
                                    page_jobs.append(job_data)
                                    logger.info(f"Extracted: {job_data['title']}")
                                    logger.info(f"  Location: {job_data['location']}")
                                    logger.info(f"  Salary: {job_data.get('salary_raw_text', 'Not specified')}")
                                else:
                                    logger.warning(f"Could not extract data for job {i} on page {page_num}")
                                
                                # Small delay between extractions
                                self.human_delay(0.5, 1)
                                
                            except Exception as e:
                                logger.error(f"Error extracting job {i} on page {page_num}: {e}")
                                self.stats['errors'] += 1
                                continue
                        
                        logger.info(f"Successfully extracted {len(page_jobs)} jobs from page {page_num}")
                        all_extracted_jobs.extend(page_jobs)
                        self.stats['pages_scraped'] += 1
                        
                        # Check if we've reached the job limit
                        if self.max_jobs and len(all_extracted_jobs) >= self.max_jobs:
                            logger.info(f"Reached job limit of {self.max_jobs}, stopping pagination")
                            all_extracted_jobs = all_extracted_jobs[:self.max_jobs]
                            break
                        
                        # Add delay between pages
                        if page_num < pages_to_scrape[-1]:  # Don't delay after the last page
                            self.human_delay(2, 4)
                            
                    except Exception as e:
                        logger.error(f"Error scraping page {page_num}: {e}")
                        self.stats['errors'] += 1
                        continue
                
                logger.info(f"=" * 60)
                logger.info(f"PROCESSING ALL COLLECTED JOBS")
                logger.info(f"=" * 60)
                logger.info(f"Total jobs extracted from all pages: {len(all_extracted_jobs)}")
                
                # Now process and save each extracted job
                for i, job_data in enumerate(all_extracted_jobs, 1):
                    try:
                        page_num = job_data.get('page_number', 'Unknown')
                        logger.info(f"Processing job {i}/{len(all_extracted_jobs)} (from page {page_num}): {job_data['title']}")
                        self.stats['total_processed'] += 1
                        
                        success = self.save_job(job_data, page)
                        if success:
                            logger.info(f"Successfully saved: {job_data['title']}")
                        
                        # Add delay between job processing
                        self.human_delay(1, 2)
                        
                    except Exception as e:
                        logger.error(f"Error processing job {i}: {e}")
                        self.stats['errors'] += 1
                        continue
                
            except Exception as e:
                logger.error(f"Error during scraping: {e}")
                
            finally:
                browser.close()
        
        self.print_summary()

    def print_summary(self):
        """Print scraping summary"""
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total jobs available: {self.stats['total_jobs_available']}")
        logger.info(f"Total pages found: {self.stats['total_pages_found']}")
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Total jobs processed: {self.stats['total_processed']}")
        logger.info(f"New jobs saved: {self.stats['new_jobs']}")
        logger.info(f"Duplicate jobs skipped: {self.stats['duplicate_jobs']}")
        logger.info(f"Companies created: {self.stats['companies_created']}")
        logger.info(f"Locations created: {self.stats['locations_created']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main function with pagination support"""
    max_jobs = 50  # Default
    max_pages = None  # Default: auto-calculate based on max_jobs
    
    # Parse command line arguments
    # Usage: python script.py [max_jobs] [max_pages]
    # Examples:
    #   python script.py 50        - Scrape max 50 jobs (auto-calculate pages)
    #   python script.py 50 3      - Scrape max 50 jobs from first 3 pages  
    #   python script.py - 2       - Scrape all jobs from first 2 pages
    
    if len(sys.argv) > 1:
        try:
            if sys.argv[1] != '-':
                max_jobs = int(sys.argv[1])
                logger.info(f"Job limit set to: {max_jobs}")
        except ValueError:
            logger.error("Invalid job limit. Please provide a number or '-'.")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            max_pages = int(sys.argv[2])
            logger.info(f"Page limit set to: {max_pages}")
        except ValueError:
            logger.error("Invalid page limit. Please provide a number.")
            sys.exit(1)
    
    # Create and run scraper
    scraper = RobertHalfAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
    scraper.scrape_jobs()


if __name__ == "__main__":
    main()
