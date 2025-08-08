#!/usr/bin/env python
"""
Professional Territory WorkerConnect Job Scraper using Playwright

This script scrapes jobs from https://jobs.theterritory.com.au/ using Playwright
for modern web scraping with human-like behavior to avoid detection.

Features:
- Playwright-based scraping with real browser automation
- Human-like behavior with random delays and interactions
- Comprehensive data extraction from all job fields
- Pagination support to scrape all available pages
- Django ORM integration with existing models
- Real-time scraping from live website (no mock data)

Usage:
    python scrap_theterritory.py [max_jobs]

Example:
    python scrap_theterritory.py 50
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

# Import our Django models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Back to normal logging
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('territory_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TerritoryJobScraper:
    """
    Professional Territory WorkerConnect scraper using Playwright.
    """
    
    def __init__(self, headless=False, job_limit=50):
        """Initialize the Territory scraper."""
        self.headless = headless
        self.base_url = "https://jobs.theterritory.com.au"
        self.job_limit = job_limit
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='territory_scraper_system',
                defaults={
                    'email': 'system@territoryscraper.com',
                    'first_name': 'Territory',
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
    
    def human_delay(self, min_seconds=2.5, max_seconds=6.0):
        """Add human-like delay between actions as specified in requirements."""
        delay = random.uniform(min_seconds, max_seconds)
        logger.debug(f"Human delay: {delay:.2f} seconds...")
        time.sleep(delay)
    
    def scroll_page_slowly(self, page):
        """Scroll the page slowly like a human would."""
        try:
            # Get page height
            page_height = page.evaluate("document.body.scrollHeight")
            current_position = 0
            scroll_step = random.randint(300, 600)
            
            while current_position < page_height:
                # Scroll down by a random amount
                current_position += scroll_step
                page.evaluate(f"window.scrollTo(0, {current_position})")
                
                # Add random delay between scrolls
                time.sleep(random.uniform(0.8, 2.0))
                
                # Randomly vary scroll step
                scroll_step = random.randint(300, 600)
                
                # Check if new content loaded (page height changed)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height > page_height:
                    page_height = new_height
                    
            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            self.human_delay(1, 2)
            
        except Exception as e:
            logger.warning(f"Error during scrolling: {str(e)}")
    
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
        
        # Northern Territory specific locations
        nt_cities = {
            'Darwin': 'Darwin',
            'Alice Springs': 'Alice Springs', 
            'Katherine': 'Katherine',
            'Tennant Creek': 'Tennant Creek',
            'Palmerston': 'Palmerston',
            'Nhulunbuy': 'Nhulunbuy',
            'Casuarina': 'Casuarina'
        }
        
        city = ""
        state = "Northern Territory"
        country = "Australia"
        
        # Check if location contains a known NT city
        for nt_city in nt_cities.keys():
            if nt_city.lower() in location_string.lower():
                city = nt_city
                break
        
        # If no specific city found, use the whole location as city
        if not city:
            city = location_string
        
        location_name = f"{city}, NT" if city else location_string
        
        return location_name, city, state, country
    
    def parse_salary(self, salary_text):
        """Parse salary information into structured data."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for salary extraction
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|annum|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|annum|month|week|day|hour)',
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80-100k"
            r'(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80k"
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
                    if groups[2] in ['year', 'annum']:
                        salary_type = 'yearly'
                    else:
                        salary_type = groups[2]
                    break
                elif len(groups) == 2 and 'k' in salary_text.lower():  # Range in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_max = Decimal(groups[1].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
                elif len(groups) == 2:  # Single amount with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    if groups[1] in ['year', 'annum']:
                        salary_type = 'yearly'
                    else:
                        salary_type = groups[1]
                    break
                elif len(groups) == 1 and 'k' in salary_text.lower():  # Single amount in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
        
        return salary_min, salary_max, currency, salary_type, salary_text
    
    def extract_job_data_from_listing(self, job_element, page):
        """Extract basic job data from Territory WorkerConnect listing page job card."""
        try:
            job_data = {}
            
            # Territory-specific selectors for job title (simplified based on actual structure)
            title_selectors = [
                # Job titles are in clickable links (most common pattern)
                'a[href*="/industry/"]',  # Primary job title links
                'a[href*="/job/"]',       # Alternative job links
                # Heading elements containing job titles
                'h3', 'h2', 'h4',
                # Links within headings
                'h3 a', 'h2 a', 'h4 a',
                # Any link that's not a button
                'a:not([class*="btn"]):not([class*="button"])',
            ]
            
            job_data['title'] = self.extract_text_by_selectors(job_element, title_selectors)
            
            # Extract company name (Territory shows company names prominently)
            company_selectors = [
                # Company name is often after job title in Territory structure
                'h4', 'h5',  # Often company names are in h4/h5
                '.company, .employer, .organization',
                '[class*="company"], [class*="employer"]',
                # Text that comes after job title
                'h3 + div, h2 + div',
                # Look for patterns like "Company Name - Location"
                'div:has-text(" - ")',
            ]
            
            job_data['company'] = self.extract_text_by_selectors(job_element, company_selectors)
            
            # Territory often shows "Company - Location, NT" format
            if job_data['company'] and ' - ' in job_data['company']:
                # Split and take first part as company
                job_data['company'] = job_data['company'].split(' - ')[0].strip()
            
            if not job_data['company']:
                job_data['company'] = "Northern Territory Government"
            
            # Extract location (NT locations are very specific)
            location_selectors = [
                # Location often in the company line: "Company - Location, NT"
                'h4:has-text("NT"), h5:has-text("NT")',
                # Standard location selectors
                '.location, .place, .region',
                '[class*="location"], [class*="region"]',
                '.address, .suburb',
                # Look for NT in text
                ':has-text("NT")',
                # Look for Darwin, Alice Springs, etc.
                ':has-text("Darwin"), :has-text("Alice Springs"), :has-text("Katherine")'
            ]
            
            job_data['location'] = self.extract_text_by_selectors(job_element, location_selectors)
            
            # Extract location from company text if needed
            if not job_data['location'] and job_data['company'] and ' - ' in job_data['company']:
                parts = job_data['company'].split(' - ')
                if len(parts) > 1:
                    job_data['location'] = parts[1].strip()
                    job_data['company'] = parts[0].strip()
            
            # Extract job URL (View Job button or job title link)
            url_selectors = [
                # Job title links (most reliable for Territory)
                'a[href*="/industry/"]',  # Primary pattern
                'a[href*="/job/"]',       # Alternative pattern
                # "View Job" buttons
                'a:has-text("View Job")',
                # Links in headings
                'h3 a', 'h2 a', 'h4 a',
                # Any href with job-related paths
                'a[href*="job"]',
            ]
            
            job_data['url'] = self.extract_url_by_selectors(job_element, url_selectors)
            
            # Extract posted date (Territory shows dates like "7 August")
            date_selectors = [
                # Look for date patterns
                ':has-text("August"), :has-text("July"), :has-text("September")',
                ':has-text("ago"), :has-text("days")',
                '.date, .posted, .published',
                '[class*="date"], [class*="posted"]',
                '.job-date, .vacancy-date'
            ]
            
            job_data['posted_date'] = self.extract_text_by_selectors(job_element, date_selectors)
            
            # Extract job summary/description (often in paragraph below title)
            desc_selectors = [
                # Description text is often in paragraphs
                'p', 'div p',
                '.description, .summary, .content',
                '[class*="description"], [class*="summary"]',
                '.job-description, .vacancy-description',
                # Text content below title
                'h3 + p, h2 + p, h3 + div p'
            ]
            
            job_data['description'] = self.extract_text_by_selectors(job_element, desc_selectors)
            
            # Extract salary (Territory often shows salary in job text)
            salary_selectors = [
                # Look for salary patterns in text
                ':has-text("$"), :has-text("hour"), :has-text("salary")',
                '.salary, .pay, .compensation',
                '[class*="salary"], [class*="pay"]',
                '.remuneration, .package'
            ]
            
            job_data['salary'] = self.extract_text_by_selectors(job_element, salary_selectors)
            
            # Extract job type (Full Time, Part Time, etc.)
            type_selectors = [
                ':has-text("Full Time"), :has-text("Part Time"), :has-text("Contract")',
                '.job-type, .employment-type, .work-type',
                '[class*="type"], [class*="employment"]'
            ]
            
            job_data['job_type'] = self.extract_text_by_selectors(job_element, type_selectors)
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data from listing: {str(e)}")
            return None
    
    def extract_text_by_selectors(self, element, selectors):
        """Try multiple selectors to extract text content."""
        for selector in selectors:
            try:
                target = element.query_selector(selector)
                if target:
                    text = target.inner_text().strip()
                    if text and len(text) > 1:
                        return text
            except:
                continue
        return ""
    
    def extract_url_by_selectors(self, element, selectors):
        """Try multiple selectors to extract URL."""
        for selector in selectors:
            try:
                target = element.query_selector(selector)
                if target:
                    href = target.get_attribute('href')
                    if href:
                        if href.startswith('/'):
                            return f"{self.base_url}{href}"
                        elif href.startswith('http'):
                            return href
                        else:
                            return f"{self.base_url}/{href}"
            except:
                continue
        return ""
    
    def get_job_detail_data(self, job_url, page):
        """Navigate to job detail page and extract comprehensive information."""
        try:
            logger.info(f"🔗 Visiting job detail page: {job_url}")
            
            # Navigate to job detail page
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(3, 5)
            
            # Wait for page content to load using page.wait_for_selector
            try:
                page.wait_for_selector('body', timeout=10000)
            except:
                pass  # Continue even if selector wait fails
            
            # Scroll page to ensure all content is loaded
            self.scroll_page_slowly(page)
            
            job_detail_data = {}
            
            # Extract detailed description (be more specific for Territory)
            description_selectors = [
                # Look for specific job description sections
                '.job-description', '.description', '.job-detail',
                '[class*="description"]:not([class*="create"]):not([class*="alert"])',
                # Territory specific content areas
                '.content:not(.filter):not(.alert)',
                '#job-detail', '.job-content',
                # Fallback to main content but limit length
                'main p', '.main-content p'
            ]
            
            detailed_desc = self.extract_text_by_selectors(page, description_selectors)
            # Limit description length to avoid capturing entire page
            if detailed_desc and len(detailed_desc) > 100:
                # Truncate if too long (likely captured too much)
                if len(detailed_desc) > 2000:
                    detailed_desc = detailed_desc[:2000] + "..."
                job_detail_data['detailed_description'] = detailed_desc
            
            # Extract requirements
            req_selectors = [
                '.requirements', '.criteria', '.qualifications',
                '[class*="requirement"]', '[class*="criteria"]', '[class*="qualification"]'
            ]
            
            job_detail_data['requirements'] = self.extract_text_by_selectors(page, req_selectors)
            
            # Extract benefits
            benefit_selectors = [
                '.benefits', '.perks', '.package',
                '[class*="benefit"]', '[class*="perk"]', '[class*="package"]'
            ]
            
            job_detail_data['benefits'] = self.extract_text_by_selectors(page, benefit_selectors)
            
            # Extract additional job details
            job_detail_data['closing_date'] = self.extract_closing_date(page)
            job_detail_data['classification'] = self.extract_classification(page)
            job_detail_data['reference_number'] = self.extract_reference_number(page)
            
            return job_detail_data
            
        except Exception as e:
            logger.warning(f"Error getting job detail data: {str(e)}")
            return {}
    
    def extract_closing_date(self, page):
        """Extract job application closing date."""
        selectors = [
            '.closing-date', '.closing', '.deadline',
            '[class*="closing"]', '[class*="deadline"]',
            '.application-date', '.expire'
        ]
        return self.extract_text_by_selectors(page, selectors)
    
    def extract_classification(self, page):
        """Extract job classification (relevant for government jobs)."""
        selectors = [
            '.classification', '.level', '.grade',
            '[class*="classification"]', '[class*="level"]', '[class*="grade"]'
        ]
        return self.extract_text_by_selectors(page, selectors)
    
    def extract_reference_number(self, page):
        """Extract job reference number."""
        selectors = [
            '.reference', '.ref-number', '.job-id',
            '[class*="reference"]', '[class*="ref"]', '[class*="id"]'
        ]
        return self.extract_text_by_selectors(page, selectors)
    
    def save_job_to_database_sync(self, job_data):
        """Synchronous database save function to be called from thread."""
        try:
            # Close any existing connections to ensure fresh connection
            connections.close_all()
            
            with transaction.atomic():
                # Enhanced duplicate detection: Check both URL and title+company
                job_url = job_data['url']
                job_title = job_data['title']
                company_name = job_data['company']
                
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
                company_name = job_data.get('company', 'Northern Territory Government')
                company_slug = slugify(company_name)
                
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Jobs from Territory WorkerConnect',
                        'website': self.base_url,
                        'company_size': 'large'  # Most Territory jobs are government/large orgs
                    }
                )
                
                # Parse salary
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary', '')
                )
                
                # Parse date
                date_posted = self.parse_date(job_data.get('posted_date', ''))
                
                # Determine job type based on available data
                job_type = "full_time"  # Default
                work_mode = ""
                experience_level = ""
                
                job_type_text = job_data.get('job_type', '').lower()
                if 'part-time' in job_type_text or 'part time' in job_type_text:
                    job_type = "part_time"
                elif 'contract' in job_type_text:
                    job_type = "contract"
                elif 'temporary' in job_type_text:
                    job_type = "temporary"
                elif 'casual' in job_type_text:
                    job_type = "part_time"
                
                # Extract work mode and experience level from description
                all_text = f"{job_data.get('title', '')} {job_data.get('description', '')} {job_data.get('detailed_description', '')}".lower()
                
                if any(term in all_text for term in ['remote', 'work from home', 'telecommute']):
                    work_mode = "Remote"
                elif any(term in all_text for term in ['hybrid', 'flexible']):
                    work_mode = "Hybrid"
                
                if any(term in all_text for term in ['senior', 'lead', 'principal', 'manager']):
                    experience_level = "Senior"
                elif any(term in all_text for term in ['junior', 'graduate', 'entry level', 'trainee']):
                    experience_level = "Entry Level"
                elif any(term in all_text for term in ['mid-level', 'intermediate', 'experienced']):
                    experience_level = "Mid-Level"
                
                # Create tags from job data
                tags_list = []
                
                # Add NT-specific tags
                tags_list.append('Northern Territory')
                tags_list.append('Territory WorkerConnect')
                
                # Add classification if available
                if job_data.get('classification'):
                    tags_list.append(job_data['classification'])
                
                # Add skills/keywords from job content
                skills_keywords = [
                    'leadership', 'management', 'communication', 'analysis', 'technical',
                    'customer service', 'administration', 'education', 'healthcare',
                    'engineering', 'construction', 'tourism', 'agriculture'
                ]
                
                for skill in skills_keywords:
                    if skill in all_text:
                        tags_list.append(skill.title())
                
                tags_string = ', '.join(list(set(tags_list))[:10])  # Limit to 10 unique tags
                
                # Automatic job categorization
                job_category = JobCategorizationService.categorize_job(
                    title=job_data.get('title', ''),
                    description=job_data.get('description', '') + ' ' + job_data.get('detailed_description', '')
                )
                
                # Create unique slug
                base_slug = slugify(job_data.get('title', 'territory-job'))
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Prepare final description
                final_description = job_data.get('detailed_description', '') or job_data.get('description', '')
                if not final_description:
                    final_description = f"Job opportunity for {job_data.get('title', 'Position')} with {company_name} in Northern Territory."
                
                # Add additional info to description if available
                additional_info = []
                if job_data.get('requirements'):
                    additional_info.append(f"Requirements: {job_data['requirements']}")
                if job_data.get('benefits'):
                    additional_info.append(f"Benefits: {job_data['benefits']}")
                if job_data.get('closing_date'):
                    additional_info.append(f"Closing Date: {job_data['closing_date']}")
                
                if additional_info:
                    final_description += "\n\n" + "\n\n".join(additional_info)
                
                # Create the JobPosting object
                job_posting = JobPosting.objects.create(
                    title=job_data.get('title', ''),
                    slug=unique_slug,
                    description=final_description,
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
                    external_source='jobs.theterritory.com.au',
                    external_url=job_data.get('url', ''),
                    status='active',
                    posted_ago=job_data.get('posted_date', ''),
                    date_posted=date_posted,
                    tags=tags_string,
                    additional_info=job_data  # Store all extracted data
                )
                
                logger.info(f"✅ Saved job: {job_posting.title} at {job_posting.company.name}")
                logger.info(f"   📋 Category: {job_posting.job_category}")
                logger.info(f"   📍 Location: {job_posting.location.name if job_posting.location else 'Not specified'}")
                logger.info(f"   💰 Salary: {job_posting.salary_display}")
                logger.info(f"   🔗 URL: {job_posting.external_url}")
                
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
    
    def find_job_elements(self, page):
        """Find job listing elements on the current page using Territory-specific selectors."""
        # Territory WorkerConnect specific selectors (optimized for actual job cards)
        territory_job_selectors = [
            # Most reliable: containers that have BOTH a job title link AND a View Job button
            'div:has(a[href*="/industry/"]):has(a:text("View Job"))',
            # Containers with job title links and company info
            'div:has(a[href*="/industry/"]):has(h4)',  # h4 often contains company name
            # Fallback: just job title links (less reliable but needed)
            'div:has(a[href*="/industry/"])',
            # Alternative job URLs
            'div:has(a[href*="/job/"]):has(a:text("View Job"))',
        ]
        
        # Try Territory-specific selectors first
        for selector in territory_job_selectors:
            try:
                logger.debug(f"Trying Territory selector: {selector}")
                job_elements = page.query_selector_all(selector)
                if job_elements and len(job_elements) >= 3:  # Territory shows multiple jobs per page
                    logger.info(f"✅ Found {len(job_elements)} job elements using Territory selector: {selector}")
                    return job_elements
            except Exception as e:
                logger.debug(f"Territory selector {selector} failed: {str(e)}")
                continue
        
        # Fallback to more generic selectors
        generic_selectors = [
            # Based on the HTML structure visible in the screenshots
            'article, section',
            '[class*="job"], [class*="listing"]', 
            'div[class*="result"], div[class*="item"]',
            # Look for elements containing both title and company
            'div:has(h2):has(h3), div:has(h3):has(h4), div:has(h1):has(h2)',
        ]
        
        for selector in generic_selectors:
            try:
                job_elements = page.query_selector_all(selector)
                if job_elements and len(job_elements) > 1:
                    logger.info(f"✅ Found {len(job_elements)} job elements using generic selector: {selector}")
                    return job_elements
            except Exception as e:
                logger.debug(f"Generic selector {selector} failed: {str(e)}")
                continue
        
        logger.warning("No job elements found with any selector")
        return []
    
    def scrape_page(self, page):
        """Scrape all job listings from the current page."""
        try:
            # Wait for page to load completely using page.wait_for_selector
            try:
                page.wait_for_selector('body', timeout=30000)
            except:
                pass  # Continue even if selector wait fails
            
            self.human_delay(2, 4)
            
            # Scroll page to ensure all content is loaded
            self.scroll_page_slowly(page)
            
            # Find job elements
            job_elements = self.find_job_elements(page)
            
            if not job_elements:
                logger.warning("No job listings found on current page")
                return 0
            
            logger.info(f"📋 Found {len(job_elements)} job listings on current page")
            
            jobs_processed = 0
            
            # Process each job element
            for i, job_element in enumerate(job_elements):
                try:
                    # Check if we've reached the job limit
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                        return -1  # Special return value to indicate limit reached
                    
                    logger.info(f"🔄 Processing job {i+1}/{len(job_elements)}")
                    
                    # Scroll job into view
                    try:
                        job_element.scroll_into_view_if_needed()
                    except:
                        pass  # Sometimes this fails, but it's not critical
                    
                    self.human_delay(1, 2)
                    
                    # Extract basic job data from listing
                    job_data = self.extract_job_data_from_listing(job_element, page)
                    
                    if not job_data or not job_data.get('title') or not job_data.get('url'):
                        logger.warning(f"⚠️ Skipping job {i+1} - missing essential data")
                        continue
                    
                    logger.info(f"📋 Extracted: {job_data['title']} at {job_data['company']}")
                    
                    # Get detailed job information by visiting the job detail page
                    try:
                        detail_data = self.get_job_detail_data(job_data['url'], page)
                        
                        # Merge detail data with basic data
                        job_data.update(detail_data)
                        
                    except Exception as e:
                        logger.warning(f"Could not get job details: {str(e)[:100]}...")
                        # Continue with basic data if detail page fails
                    
                    # Save job to database
                    if self.save_job_to_database(job_data):
                        jobs_processed += 1
                        logger.info(f"✅ Job {self.scraped_count} saved successfully!")
                    else:
                        logger.warning(f"⚠️ Failed to save job (duplicate or error)")
                    
                    # Add human delay between job processing
                    self.human_delay(2, 4)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing job {i+1}: {str(e)}")
                    self.error_count += 1
                    continue
            
            logger.info(f"✅ Page complete: {jobs_processed} jobs processed")
            return jobs_processed
            
        except Exception as e:
            logger.error(f"Error scraping page: {str(e)}")
            return 0
    
    def has_next_page(self, page):
        """Check if there's a next page available."""
        try:
            # Territory-specific pagination selectors
            territory_next_selectors = [
                # Territory likely uses page-number-X in URL structure
                'a[href*="page-number-2"], a[href*="page-number-3"]',
                'a[href*="page-number"]:not([href*="page-number-1"])',
                # Standard pagination
                'a[aria-label="Next"]', 'a[title="Next"]',
                'a:has-text("Next")', 'a:has-text(">")',
                '.next', '.pagination-next', '.pager-next',
                '[class*="next"]', '[class*="pager"]',
                'a[href*="page"]', 'a[href*="Page"]'
            ]
            
            for selector in territory_next_selectors:
                try:
                    next_element = page.query_selector(selector)
                    if next_element and next_element.is_enabled() and next_element.is_visible():
                        # Check if it's not disabled
                        disabled = next_element.get_attribute('disabled')
                        aria_disabled = next_element.get_attribute('aria-disabled')
                        
                        if not disabled and aria_disabled != 'true':
                            logger.debug(f"Found active next page button: {selector}")
                            return True
                except:
                    continue
            
            # Also check if current URL indicates more pages available
            try:
                current_url = page.url
                if 'page-number-1' in current_url and 'page-size-20' in current_url:
                    # If we're on page 1 with 20 results per page, there might be more
                    # Check if we have 20 jobs on current page
                    job_elements = self.find_job_elements(page)
                    if len(job_elements) >= 20:
                        # Likely more pages exist
                        logger.debug("Current page has 20+ jobs, likely more pages exist")
                        return True
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for next page: {str(e)}")
            return False
    
    def go_to_next_page(self, page):
        """Navigate to the next page of results."""
        try:
            # First try clicking pagination buttons
            territory_next_selectors = [
                # Territory pagination
                'a[href*="page-number-2"], a[href*="page-number-3"]',
                'a[href*="page-number"]:not([href*="page-number-1"])',
                # Standard pagination
                'a[aria-label="Next"]', 'a[title="Next"]',
                'a:has-text("Next")', 'a:has-text(">")',
                '.next', '.pagination-next', '.pager-next',
                '[class*="next"]', '[class*="pager"]'
            ]
            
            for selector in territory_next_selectors:
                try:
                    next_element = page.query_selector(selector)
                    if next_element and next_element.is_enabled() and next_element.is_visible():
                        # Check if it's not disabled
                        disabled = next_element.get_attribute('disabled')
                        aria_disabled = next_element.get_attribute('aria-disabled')
                        
                        if not disabled and aria_disabled != 'true':
                            logger.info(f"🔗 Clicking next page button: {selector}")
                            
                            # Scroll to element and click
                            next_element.scroll_into_view_if_needed()
                            self.human_delay(1, 2)
                            
                            # Click the next button
                            next_element.click()
                            
                            # Wait for page to load
                            try:
                                page.wait_for_selector('body', timeout=30000)
                            except:
                                pass
                            
                            self.human_delay(3, 5)
                            
                            return True
                except Exception as e:
                    logger.debug(f"Next button selector {selector} failed: {str(e)}")
                    continue
            
            # If no pagination button found, try URL manipulation for Territory
            try:
                current_url = page.url
                if 'page-number-' in current_url:
                    # Extract current page number and increment
                    import re
                    match = re.search(r'page-number-(\d+)', current_url)
                    if match:
                        current_page = int(match.group(1))
                        next_page = current_page + 1
                        next_url = current_url.replace(f'page-number-{current_page}', f'page-number-{next_page}')
                        
                        logger.info(f"🔗 Navigating to next page via URL: page {next_page}")
                        page.goto(next_url, wait_until='domcontentloaded', timeout=30000)
                        self.human_delay(3, 5)
                        
                        return True
            except Exception as e:
                logger.debug(f"URL manipulation failed: {str(e)}")
            
            logger.warning("No working next page method found")
            return False
            
        except Exception as e:
            logger.error(f"Error navigating to next page: {str(e)}")
            return False
    
    def run(self):
        """Main method to run the complete scraping process."""
        logger.info("🚀 Starting Territory WorkerConnect Job Scraper with Playwright")
        logger.info(f"🎯 Target: {self.job_limit} jobs from {self.base_url}")
        logger.info(f"🤖 Browser mode: {'Headless' if self.headless else 'Visible'}")
        logger.info("=" * 70)
        
        with sync_playwright() as p:
            # Launch browser with human-like settings
            browser = p.chromium.launch(
                headless=self.headless,  # Visible browser as per requirements
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
                # Navigate to Territory jobs website
                logger.info(f"🌐 Navigating to {self.base_url}")
                
                # Try multiple entry points (based on actual Territory URL structure)
                entry_urls = [
                    # Direct job search URL (from your screenshot)
                    f"{self.base_url}/jobs-in-any-industry/last-7/radius-exact/sorted-by-date/page-number-1/page-size-20",
                    # Alternative job search URLs
                    f"{self.base_url}/jobs-in-any-industry",
                    f"{self.base_url}/",
                    f"{self.base_url}/jobs",
                    f"{self.base_url}/CommunityJobSearchDefault",
                    f"{self.base_url}/search"
                ]
                
                page_loaded = False
                
                for url in entry_urls:
                    try:
                        logger.info(f"🔍 Trying: {url}")
                        page.goto(url, wait_until='domcontentloaded', timeout=30000)
                        self.human_delay(5, 8)
                        
                        # Check if we got meaningful content
                        page_content = page.content()
                        if len(page_content) > 1000 and 'blocked' not in page_content.lower():
                            logger.info(f"✅ Successfully loaded: {url}")
                            logger.info(f"📄 Page title: {page.title()}")
                            page_loaded = True
                            break
                        else:
                            logger.warning(f"⚠️ Page seems blocked or has minimal content: {url}")
                            
                    except Exception as e:
                        logger.warning(f"❌ Failed to load {url}: {str(e)}")
                        continue
                
                if not page_loaded:
                    logger.error("❌ Could not load any Territory website pages")
                    return
                
                # Look for job search functionality
                self.human_delay(2, 4)
                
                page_number = 1
                total_jobs_found = 0
                
                # Main scraping loop with pagination
                while True:
                    logger.info(f"📄 Scraping page {page_number}...")
                    
                    # Scrape current page
                    jobs_on_page = self.scrape_page(page)
                    
                    # Check if we reached the job limit
                    if jobs_on_page == -1:
                        logger.info("✅ Job limit reached, stopping scraping.")
                        break
                    
                    total_jobs_found += jobs_on_page if jobs_on_page > 0 else 0
                    
                    if jobs_on_page == 0:
                        logger.warning("⚠️ No jobs found on current page, stopping...")
                        break
                    
                    # Check if we've reached our job limit
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"✅ Reached job limit of {self.job_limit}. Scraping complete!")
                        break
                    
                    # Check if there's a next page
                    if not self.has_next_page(page):
                        logger.info("✅ No more pages available, scraping complete!")
                        break
                    
                    # Navigate to next page
                    if not self.go_to_next_page(page):
                        logger.warning("⚠️ Failed to navigate to next page, stopping...")
                        break
                    
                    page_number += 1
                    
                    # Add a longer delay between pages
                    self.human_delay(5, 8)
                
                # Final statistics
                logger.info("=" * 70)
                logger.info("🎉 TERRITORY SCRAPING COMPLETED!")
                logger.info(f"📊 Total pages scraped: {page_number}")
                logger.info(f"📋 Total jobs found: {total_jobs_found}")
                logger.info(f"✅ Jobs saved to database: {self.scraped_count}")
                logger.info(f"🔄 Duplicate jobs skipped: {self.duplicate_count}")
                logger.info(f"❌ Errors encountered: {self.error_count}")
                
                # Get total job count using thread-safe approach
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(lambda: JobPosting.objects.filter(external_source='jobs.theterritory.com.au').count())
                        total_territory_jobs = future.result(timeout=10)
                        logger.info(f"🗂️ Total Territory jobs in database: {total_territory_jobs}")
                except:
                    logger.info("🗂️ Total Territory jobs in database: (count unavailable)")
                
                logger.info("=" * 70)
                
            except Exception as e:
                logger.error(f"❌ Fatal error during scraping: {str(e)}")
                raise
            finally:
                browser.close()


def scrape_theterritory_jobs(job_limit=50):
    """
    Main function to run the Territory WorkerConnect job scraper.
    
    Args:
        job_limit (int): Maximum number of jobs to scrape
    """
    logger.info(f"🔧 Starting Territory WorkerConnect Job Scraper")
    logger.info(f"🎯 Target: {job_limit} jobs maximum")
    
    # Create scraper instance
    scraper = TerritoryJobScraper(
        headless=False,  # Not headless as per requirements
        job_limit=job_limit
    )
    
    try:
        # Run the scraping process
        scraper.run()
        
    except KeyboardInterrupt:
        logger.info("🛑 Scraping interrupted by user")
    except Exception as e:
        logger.error(f"❌ Scraping failed: {str(e)}")
        raise


def main():
    """Main function to run the scraper with command line arguments."""
    import sys
    
    print("🔍 Territory WorkerConnect Job Scraper using Playwright")
    print("=" * 60)
    
    # Parse command line arguments
    job_limit = 50  # Default
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            print("❌ Invalid number of jobs. Using default: 50")
    
    print(f"🎯 Target: {job_limit} jobs from Territory WorkerConnect")
    print(f"🗄️ Database: JobPosting, Company, Location models")
    print(f"🤖 Engine: Playwright (visible browser)")
    print("=" * 60)
    
    # Run the scraper
    scrape_theterritory_jobs(job_limit)


if __name__ == "__main__":
    main()
