#!/usr/bin/env python
"""
Professional Voyages.com.au Job Scraper using Playwright

This script scrapes job postings from Voyages Indigenous Tourism Australia's careers page
and stores them in the professional database structure with JobPosting, Company, and Location models.

Features:
- Professional database structure with proper relationships
- Automatic job categorization using keyword matching
- Human-like behavior to avoid detection
- Complete data extraction and normalization
- Playwright for modern web scraping
- Handles all job listings from Voyages careers page

Usage:
    python voyages_australia_scraper.py [max_jobs]

Example:
    python voyages_australia_scraper.py 50
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
import json

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

# Handle both normal execution and exec() execution
try:
    # Normal execution
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    # When executed with exec(), __file__ is not defined
    project_root = os.getcwd()

sys.path.append(project_root)

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('voyages_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# User model
User = get_user_model()

class VoyagesScraper:
    """Professional Voyages.com.au job scraper with database integration."""
    
    def __init__(self, max_jobs=None, headless=True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.voyages.com.au"
        self.careers_url = "https://www.voyages.com.au/careers/positions-available"
        self.scraped_count = 0
        self.company = None
        self.scraper_user = None
        self._current_job_page_content = None
        
        # Job category mapping for Voyages positions
        self.category_keywords = {
            'hospitality': [
                'food', 'beverage', 'dining', 'restaurant', 'kitchen', 'chef', 'cook',
                'housekeeping', 'room', 'attendant', 'front office', 'guest services',
                'porter', 'spa', 'therapist', 'hotel', 'resort', 'hospitality'
            ],
            'retail': [
                'retail', 'shop', 'store', 'sales', 'merchandise', 'assistant'
            ],
            'technology': [
                'it', 'technology', 'computer', 'software', 'network', 'support',
                'technical', 'system', 'digital'
            ],
            'construction': [
                'maintenance', 'trades', 'electrician', 'plumber', 'painter', 'builder',
                'construction', 'repair', 'grounds', 'gardener', 'facilities'
            ],
            'finance': [
                'finance', 'accounting', 'financial', 'budget', 'revenue', 'audit',
                'payroll', 'bookkeeping'
            ],
            'hr': [
                'human resources', 'recruitment', 'training', 'hr', 'people',
                'employee', 'talent'
            ],
            'other': [
                'security', 'recreation', 'operations', 'management', 'supervisor',
                'coordinator', 'administration', 'business support', 'airport',
                'transport', 'driver', 'laundry', 'steward', 'officer'
            ]
        }
        
        # Location mapping for Voyages
        self.location_mapping = {
            'yulara': {'city': 'Yulara', 'state': 'Northern Territory'},
            'mossman gorge': {'city': 'Mossman Gorge', 'state': 'Queensland'},
            'sydney': {'city': 'Sydney', 'state': 'New South Wales'},
            'northern territory': {'city': '', 'state': 'Northern Territory'},
            'queensland': {'city': '', 'state': 'Queensland'},
            'new south wales': {'city': '', 'state': 'New South Wales'}
        }
        
    def setup_database_objects(self):
        """Create or get Company and User objects for scraping."""
        try:
            # Create or get Voyages company
            self.company, created = Company.objects.get_or_create(
                name="Voyages Indigenous Tourism Australia",
                defaults={
                    'description': "Voyages Indigenous Tourism Australia operates Ayers Rock Resort and Mossman Gorge Cultural Centre, providing unique tourism experiences in partnership with Traditional Owners.",
                    'website': "https://www.voyages.com.au",
                    'company_size': 'large',
                    'logo': "https://www.voyages.com.au/assets/images/logos/voyages-logo.png"
                }
            )
            
            if created:
                logger.info(f"Created new company: {self.company.name}")
            else:
                logger.info(f"Using existing company: {self.company.name}")
            
            # Create or get scraper user
            self.scraper_user, created = User.objects.get_or_create(
                username='voyages_scraper',
                defaults={
                    'email': 'scraper@voyages.local',
                    'first_name': 'Voyages',
                    'last_name': 'Scraper',
                    'is_active': True
                }
            )
            
            if created:
                logger.info(f"Created scraper user: {self.scraper_user.username}")
            else:
                logger.info(f"Using existing scraper user: {self.scraper_user.username}")
                
        except Exception as e:
            logger.error(f"Error setting up database objects: {str(e)}")
            raise
            
    def get_or_create_location(self, location_text):
        """Get or create location from text."""
        if not location_text:
            return None
            
        location_text = location_text.strip()
        logger.info(f"Processing location: '{location_text}'")
        
        # Clean location text
        location_clean = location_text
        
        # Handle common location patterns
        location_mappings = {
            'Yulara, Northern Territory': {'city': 'Yulara', 'state': 'Northern Territory'},
            'Mossman Gorge, Queensland': {'city': 'Mossman Gorge', 'state': 'Queensland'},
            'Sydney, New South Wales': {'city': 'Sydney', 'state': 'New South Wales'},
            'Northern Territory': {'city': '', 'state': 'Northern Territory'},
            'Queensland': {'city': '', 'state': 'Queensland'},
            'New South Wales': {'city': '', 'state': 'New South Wales'},
            'Yulara': {'city': 'Yulara', 'state': 'Northern Territory'},
            'Mossman': {'city': 'Mossman Gorge', 'state': 'Queensland'},
            'Sydney': {'city': 'Sydney', 'state': 'New South Wales'}
        }
        
        # Try exact match first
        for location_key, location_data in location_mappings.items():
            if location_key.lower() == location_text.lower():
                location_name = f"{location_data['city']}, {location_data['state']}" if location_data['city'] else location_data['state']
                
                location, created = Location.objects.get_or_create(
                    name=location_name,
                    defaults={
                        'city': location_data['city'],
                        'state': location_data['state'],
                        'country': 'Australia'
                    }
                )
                
                if created:
                    logger.info(f"Created new location: {location.name}")
                else:
                    logger.info(f"Using existing location: {location.name}")
                
                return location
        
        # Try partial match
        for location_key, location_data in location_mappings.items():
            if location_key.lower() in location_text.lower():
                location_name = f"{location_data['city']}, {location_data['state']}" if location_data['city'] else location_data['state']
                
                location, created = Location.objects.get_or_create(
                    name=location_name,
                    defaults={
                        'city': location_data['city'],
                        'state': location_data['state'],
                        'country': 'Australia'
                    }
                )
                
                if created:
                    logger.info(f"Created new location (partial match): {location.name}")
                else:
                    logger.info(f"Using existing location (partial match): {location.name}")
                
                return location
        
        # If no match found, try to parse the location
        city = ''
        state = ''
        
        if ',' in location_text:
            parts = [part.strip() for part in location_text.split(',')]
            if len(parts) == 2:
                city = parts[0]
                state = parts[1]
        else:
            # Check if it's a state
            if any(state_name in location_text for state_name in ['Northern Territory', 'Queensland', 'New South Wales']):
                state = location_text
            else:
                city = location_text
                # Try to guess state from city
                if 'yulara' in location_text.lower():
                    state = 'Northern Territory'
                elif 'mossman' in location_text.lower():
                    state = 'Queensland'
                elif 'sydney' in location_text.lower():
                    state = 'New South Wales'
        
        # Create location
        location_name = f"{city}, {state}" if city and state else (state if state else city)
        
        location, created = Location.objects.get_or_create(
            name=location_name,
            defaults={
                'city': city,
                'state': state,
                'country': 'Australia'
            }
        )
        
        if created:
            logger.info(f"Created parsed location: {location.name}")
        else:
            logger.info(f"Using existing parsed location: {location.name}")
            
        return location
        
    def categorize_job(self, title, description):
        """Categorize job based on title and description."""
        text = f"{title} {description}".lower()
        
        # Count keyword matches for each category
        category_scores = {}
        for category, keywords in self.category_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            if score > 0:
                category_scores[category] = score
        
        # Return category with highest score, default to 'other'
        if category_scores:
            return max(category_scores.items(), key=lambda x: x[1])[0]
        
        return 'other'
        
    def normalize_job_type(self, job_type_text):
        """Normalize job type from text."""
        if not job_type_text:
            return 'full_time'
            
        job_type_text = job_type_text.lower().strip()
        
        type_mapping = {
            'casual': 'casual',  # Fixed: Casual is its own type in Australia
            'part time': 'part_time', 
            'part-time': 'part_time',
            'permanent': 'full_time',
            'full time': 'full_time',
            'full-time': 'full_time',
            'contract': 'contract',
            'temporary': 'temporary',
            'temp': 'temporary',
            'internship': 'internship',
            'freelance': 'freelance'
        }
        
        # Check for exact matches first (most specific to least specific)
        for key, value in type_mapping.items():
            if key in job_type_text:
                return value
                
        return 'full_time'
        
    def human_like_delay(self, min_delay=1, max_delay=3):
        """Add human-like delay between actions."""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
        
    def extract_job_data_from_row(self, page, job_row):
        """Extract data from a job listing row on Voyages careers page."""
        try:
            job_data = {}
            
            # Get all text content from the row
            row_text = job_row.inner_text()
            logger.info(f"Processing job row: {row_text[:100]}...")
            
            # Split the row text to extract components
            lines = [line.strip() for line in row_text.split('\n') if line.strip()]
            
            if len(lines) < 2:
                logger.warning(f"Insufficient data in job row: {lines}")
                return None
            
            # First line should be the department
            department = lines[0] if lines[0] != 'View Job' else ''
            
            # Find the job title (usually after department)
            title = ''
            location_text = ''
            job_type_text = ''
            
            for i, line in enumerate(lines):
                if 'View Job' in line:
                    continue
                elif any(loc in line for loc in ['Northern Territory', 'Queensland', 'New South Wales', 'Yulara', 'Mossman', 'Sydney']):
                    location_text = line
                elif any(jtype in line for jtype in ['Permanent', 'Full Time', 'Part Time', 'Casual', 'Contract']):
                    job_type_text = line
                elif line != department and not location_text and not job_type_text:
                    # This is likely the job title
                    title = line
            
            # If we couldn't find title in a separate line, it might be combined with department
            if not title and department:
                # Look for patterns like "Department View Job" and extract from there
                if 'View Job' in department:
                    title_part = department.replace('View Job', '').strip()
                    # Try to split department and title
                    parts = title_part.split()
                    if len(parts) > 2:
                        # Assume last few words are the title
                        title = ' '.join(parts[-3:]) if len(parts) >= 3 else title_part
                        department = ' '.join(parts[:-3]) if len(parts) > 3 else ''
                else:
                    title = department
                    department = ''
            
            # Try to get job link
            job_url = self.careers_url
            try:
                link_element = job_row.query_selector('a')
                if link_element:
                    href = link_element.get_attribute('href')
                    if href:
                        job_url = urljoin(self.base_url, href) if not href.startswith('http') else href
            except:
                pass
            
            # Default description
            description = "We are seeking a passionate person who is looking to take an adventure of a lifetime all while growing your career with us."
            closing_date = None
            
            # If we have a job URL that's different from careers page, try to get more details
            if job_url != self.careers_url:
                try:
                    # Open job detail page
                    detail_page = page.context.new_page()
                    detail_page.goto(job_url, wait_until="networkidle", timeout=30000)
                    
                    # Get full page content to extract closing date
                    page_content = detail_page.inner_text('body')
                    closing_date = self.extract_closing_date(page_content)
                    
                    # Look for job description
                    desc_selectors = [
                        '.job-description',
                        '.description',
                        '[class*="description"]',
                        '.content',
                        'main p',
                        '.job-details p'
                    ]
                    
                    for selector in desc_selectors:
                        desc_element = detail_page.query_selector(selector)
                        if desc_element:
                            desc_text = desc_element.inner_text().strip()
                            if len(desc_text) > 50:  # Only use if substantial
                                description = desc_text
                                # Extract closing date from description if not found in page content
                                if not closing_date:
                                    closing_date = self.extract_closing_date(desc_text)
                                break
                    
                    detail_page.close()
                    self.human_like_delay(1, 2)
                    
                except Exception as e:
                    logger.warning(f"Could not fetch details from {job_url}: {str(e)}")
            
            # Build job data
            job_data = {
                'title': title or 'Unknown Position',
                'description': description,
                'department': department,
                'location_text': location_text,
                'job_type_text': job_type_text,
                'external_url': job_url,
                'external_id': f"voyages_{hash(job_url + title)}",  # Generate consistent ID
                'posted_ago': '',
                'salary_info': '',
                'closing_date': closing_date
            }
            
            logger.info(f"Extracted: {job_data['title']} | {job_data['department']} | {location_text}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {str(e)}")
            return None
            
    def save_job_to_database(self, job_data):
        """Save job data to database with proper relationships."""
        try:
            with transaction.atomic():
                # Get or create location
                location = self.get_or_create_location(job_data['location_text'])
                
                # Categorize job
                job_category = self.categorize_job(job_data['title'], job_data['description'])
                
                # Normalize job type
                job_type = self.normalize_job_type(job_data['job_type_text'])
                
                # Check if job already exists (by title and company)
                existing_job = JobPosting.objects.filter(
                    title=job_data['title'],
                    company=self.company
                ).first()
                
                if existing_job:
                    logger.info(f"Job already exists: {job_data['title']}")
                    return existing_job
                
                # Create new job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'],
                    description=job_data['description'],
                    company=self.company,
                    posted_by=self.scraper_user,
                    location=location,
                    job_category=job_category,
                    job_type=job_type,
                    experience_level='',
                    work_mode='On-site',  # Voyages jobs are typically on-site
                    external_source='voyages.com.au',
                    external_url=job_data['external_url'],
                    external_id=job_data['external_id'],
                    status='active',
                    posted_ago=job_data['posted_ago'],
                    date_posted=timezone.now(),  # Use current time since no posting date available
                    tags=job_data['department'],  # Use department as tags
                    additional_info={
                        'department': job_data['department'],
                        'job_type_raw': job_data['job_type_text'],
                        'location_raw': job_data['location_text'],
                        'closing_date': job_data.get('closing_date'),
                        'scraped_from': 'voyages_careers_page',
                        'scraper_version': '1.0'
                    }
                )
                
                logger.info(f"Saved job to database: {job_posting.title} (ID: {job_posting.id})")
                return job_posting
                
        except Exception as e:
            logger.error(f"Error saving job to database: {str(e)}")
            return None
            
    def scrape_jobs(self):
        """Main scraping method using Playwright."""
        logger.info("Starting Voyages job scraping...")
        
        # Setup database objects
        self.setup_database_objects()
        
        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            try:
                # Navigate to careers page
                logger.info(f"Navigating to: {self.careers_url}")
                page.goto(self.careers_url, wait_until="networkidle")
                
                # Wait for page to load completely
                self.human_like_delay(3, 5)
                
                # Log page title to confirm we're on the right page
                page_title = page.title()
                logger.info(f"Page title: {page_title}")
                
                # Wait for job listings to appear
                try:
                    page.wait_for_selector('text="View Job"', timeout=10000)
                    logger.info("Job listings loaded successfully")
                except:
                    logger.warning("'View Job' buttons not found, proceeding with available content")
                
                # Method 1: Try to find job containers by looking for "View Job" text
                job_elements = []
                try:
                    # Find all "View Job" buttons/links
                    view_job_elements = page.query_selector_all('text="View Job"')
                    logger.info(f"Found {len(view_job_elements)} 'View Job' elements")
                    
                    for view_job_element in view_job_elements:
                        # Get the parent container that holds the job information
                        job_container = view_job_element.locator('xpath=..')
                        while job_container:
                            container_text = job_container.inner_text()
                            # Check if this container has job information (department, title, location)
                            if any(keyword in container_text for keyword in ['Northern Territory', 'Queensland', 'New South Wales', 'Permanent', 'Full Time', 'Part Time']):
                                job_elements.append(job_container)
                                break
                            # Move up one level
                            try:
                                job_container = job_container.locator('xpath=..')
                            except:
                                break
                                
                except Exception as e:
                    logger.warning(f"Method 1 failed: {str(e)}")
                
                # Method 2: If Method 1 didn't work, try to find job rows by content patterns
                if not job_elements:
                    logger.info("Trying alternative method to find job listings...")
                    try:
                        # Look for elements containing job-related text patterns
                        all_elements = page.query_selector_all('div, section, article, li')
                        for element in all_elements:
                            try:
                                element_text = element.inner_text()
                                # Check if element contains job-like information
                                if ('View Job' in element_text and 
                                    any(loc in element_text for loc in ['Northern Territory', 'Queensland', 'New South Wales']) and
                                    len(element_text) < 500):  # Not too much text (avoid main containers)
                                    job_elements.append(element)
                            except:
                                continue
                                
                    except Exception as e:
                        logger.warning(f"Method 2 failed: {str(e)}")
                
                # Use targeted job extraction for Voyages website
                logger.info("Using targeted Voyages job extraction...")
                job_data_list = self.extract_voyages_jobs_precisely(page)
                
                for job_data in job_data_list:
                    if self.max_jobs and self.scraped_count >= self.max_jobs:
                        break
                        
                    saved_job = self.save_job_to_database(job_data)
                    if saved_job:
                        self.scraped_count += 1
                        
                    # Human-like delay between jobs
                    self.human_like_delay(0.5, 1)
                
                logger.info(f"Scraping completed. Total jobs processed: {self.scraped_count}")
                
            except Exception as e:
                logger.error(f"Error during scraping: {str(e)}")
                
            finally:
                browser.close()
                
        return self.scraped_count
        
    def detect_departments_and_locations(self, page_text):
        """Dynamically detect departments and locations from page content."""
        departments = set()
        locations = set()
        
        # Common department keywords to help identify departments
        dept_keywords = [
            'maintenance', 'transport', 'retail', 'food', 'beverage', 'housekeeping', 
            'laundry', 'administration', 'business', 'support', 'front', 'office', 
            'guest', 'services', 'kitchen', 'airport', 'operations', 'residential',
            'community', 'wellbeing', 'operational', 'management', 'experiences',
            'touring', 'indigenous', 'engagement', 'programs', 'trades'
        ]
        
        # Australian location patterns
        location_patterns = [
            r'([A-Z][a-z\s]+),\s*(Northern Territory|Queensland|New South Wales|Victoria|South Australia|Western Australia|Tasmania|ACT)',
            r'(Northern Territory|Queensland|New South Wales|Victoria|South Australia|Western Australia|Tasmania|ACT)',
            r'(Yulara|Mossman|Sydney|Melbourne|Brisbane|Perth|Adelaide|Darwin|Cairns|Alice Springs)',
        ]
        
        lines = [line.strip() for line in page_text.split('\n') if line.strip()]
        
        # Detect departments dynamically
        for line in lines:
            # Look for lines that contain department-like words and are followed by job titles
            if any(keyword in line.lower() for keyword in dept_keywords):
                # Check if this line looks like a department header
                if (len(line) < 100 and  # Not too long
                    not any(char.isdigit() for char in line) and  # No numbers
                    line.count(' ') <= 5 and  # Not too many words
                    not line.startswith('http') and  # Not a URL
                    '&' in line or len(line.split()) >= 2):  # Contains & or multiple words
                    departments.add(line.strip())
        
        # Detect locations using regex patterns
        import re
        for pattern in location_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    # For pattern with groups, join them
                    location = ', '.join([part for part in match if part]).strip()
                else:
                    location = match.strip()
                
                if location and len(location) > 2:
                    locations.add(location)
        
        # Also look for location patterns in individual lines
        for line in lines:
            # Look for Australian state/territory names
            if any(state in line for state in ['Northern Territory', 'Queensland', 'New South Wales', 'Victoria', 'South Australia', 'Western Australia', 'Tasmania', 'ACT']):
                if len(line) < 100 and not line.startswith('http'):
                    locations.add(line.strip())
        
        logger.info(f"Dynamically detected {len(departments)} departments: {departments}")
        logger.info(f"Dynamically detected {len(locations)} locations: {locations}")
        
        return list(departments), list(locations)
    
    def extract_departments_from_context(self, page_text):
        """Fallback method to extract departments from context."""
        departments = []
        lines = [line.strip() for line in page_text.split('\n') if line.strip()]
        
        # Look for lines that appear before "View Job" and contain department-like words
        for i, line in enumerate(lines):
            if 'View Job' in line and i > 0:
                # Look at previous lines for department info
                for j in range(max(0, i-5), i):
                    prev_line = lines[j]
                    if (len(prev_line) > 5 and len(prev_line) < 80 and 
                        not prev_line.startswith('http') and
                        not any(char.isdigit() for char in prev_line[:5])):
                        departments.append(prev_line)
        
        return list(set(departments))
    
    def extract_locations_from_context(self, page_text):
        """Fallback method to extract locations from context."""
        locations = []
        
        # Look for common Australian location patterns
        import re
        patterns = [
            r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*(?:Northern Territory|Queensland|New South Wales|Victoria|South Australia|Western Australia|Tasmania|ACT)\b',
            r'\b(?:Northern Territory|Queensland|New South Wales|Victoria|South Australia|Western Australia|Tasmania|ACT)\b'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_text)
            locations.extend(matches)
        
        return list(set(locations))
    
    def detect_job_types(self, page_text):
        """Dynamically detect job types from page content."""
        job_types = set()
        
        # Common job type patterns - Enhanced for better Casual detection
        type_patterns = [
            r'\b(?:Casual|Part[\-\s]?time|Permanent|Full Time|Contract|Temporary|Internship|Freelance)\b',
            r'\b(?:Full[\-\s]?Time|Part[\-\s]?Time)\b',
            r'\b(?:Permanent\s*/\s*Full\s*Time)\b',
            # Additional patterns for Australian job descriptions
            r'(?:Employment\s+Type[:\s]*)?(?:Casual|Part[\-\s]?time|Full[\-\s]?time)',
            r'(?:Position\s+Type[:\s]*)?(?:Casual|Part[\-\s]?time|Full[\-\s]?time)',
            r'(?:Job\s+Type[:\s]*)?(?:Casual|Part[\-\s]?time|Full[\-\s]?time)'
        ]
        
        import re
        for pattern in type_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                job_types.add(match.strip())
        
        # If no types found, use common defaults
        if not job_types:
            job_types = {'Permanent / Full Time', 'Part-time', 'Casual'}
        
        logger.info(f"Detected job types: {job_types}")
        return list(job_types)
    
    def parse_jobs_from_page_content(self, page):
        """Parse job data from page content using intelligent text analysis."""
        job_data_list = []
        
        try:
            # Get the full page content
            page_content = page.content()
            page_text = page.inner_text('body')
            
            logger.info("Parsing jobs from page content...")
            
            # Dynamically detect departments and locations
            departments, locations = self.detect_departments_and_locations(page_text)
            
            # If no departments detected, fall back to common patterns
            if not departments:
                logger.warning("No departments detected, using fallback detection...")
                departments = self.extract_departments_from_context(page_text)
            
            # If no locations detected, fall back to common Australian locations
            if not locations:
                logger.warning("No locations detected, using fallback detection...")
                locations = self.extract_locations_from_context(page_text)
            
            # Dynamic job type detection
            job_types = self.detect_job_types(page_text)
            
            # Split page text into lines for analysis
            lines = [line.strip() for line in page_text.split('\n') if line.strip()]
            
            # Find job blocks - look for patterns around "View Job"
            job_blocks = []
            
            # Method 1: Extract jobs by looking for "View Job" patterns
            for i, line in enumerate(lines):
                if 'View Job' in line:
                    # Look backwards and forwards to find the job information
                    start_idx = max(0, i - 8)  # Look up to 8 lines back
                    end_idx = min(len(lines), i + 3)  # Look up to 3 lines forward
                    
                    context_lines = lines[start_idx:end_idx]
                    job_block = [l for l in context_lines if l.strip()]
                    
                    if len(job_block) >= 2:  # Must have at least department and job info
                        job_blocks.append(job_block)
            
            # Method 2: If Method 1 doesn't find enough jobs, try department-based extraction
            if len(job_blocks) < 10:
                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    # Look for department names that are likely headers
                    dept_match = None
                    for dept in departments:
                        if dept in line and len(line) < 100:
                            dept_match = dept
                            break
                    
                    if dept_match:
                        # Found a potential job block starting with department
                        job_block = [line]
                        j = i + 1
                        
                        # Collect lines until we find "View Job" or another department
                        while j < len(lines) and j < i + 15:  # Look ahead max 15 lines
                            next_line = lines[j]
                            
                            if 'View Job' in next_line:
                                job_block.append(next_line)
                                job_blocks.append(job_block)
                                break
                            elif any(dept in next_line for dept in departments if dept != dept_match) and j > i + 2:
                                # Found start of next job
                                job_blocks.append(job_block)
                                break
                            else:
                                job_block.append(next_line)
                            j += 1
                        
                        i = j
                    else:
                        i += 1
            
            logger.info(f"Found {len(job_blocks)} potential job blocks")
            
            # Process each job block to extract real job information
            for block in job_blocks:
                if len(block) < 2:
                    continue
                    
                block_text = ' '.join(block)
                logger.info(f"Processing job block: {block_text[:150]}...")
                
                # Extract information from the block
                title = ''
                department = ''
                location_text = ''
                job_type_text = ''
                
                # Clean up the block - remove generic text
                clean_lines = []
                for line in block:
                    line_clean = line.strip()
                    # Skip generic lines
                    if (line_clean and 
                        'View Job' not in line_clean and 
                        'search for' not in line_clean.lower() and
                        'working at voyages' not in line_clean.lower() and
                        'we are seeking' not in line_clean.lower() and
                        len(line_clean) > 2):
                        clean_lines.append(line_clean)
                
                # Find department (should be one of our detected departments)
                for line in clean_lines:
                    for dept in departments:
                        if dept.lower() in line.lower() and len(line) < 80:
                            department = dept
                            break
                    if department:
                        break
                
                # Find location (should match Australian location patterns)
                for line in clean_lines:
                    for loc in locations:
                        if loc.lower() in line.lower():
                            location_text = loc
                            break
                    if location_text:
                        break
                
                # Find job type
                for line in clean_lines:
                    for jtype in job_types:
                        if jtype.lower() in line.lower():
                            job_type_text = jtype
                            break
                    if job_type_text:
                        break
                
                # Extract actual job title - look for job-specific words
                job_title_indicators = [
                    'supervisor', 'manager', 'assistant', 'officer', 'attendant', 
                    'coordinator', 'specialist', 'chef', 'steward', 'therapist',
                    'driver', 'painter', 'plumber', 'electrician', 'gardener',
                    'receptionist', 'clerk', 'analyst', 'technician', 'operator'
                ]
                
                for line in clean_lines:
                    line_lower = line.lower()
                    # Check if line contains job title indicators
                    if (any(indicator in line_lower for indicator in job_title_indicators) and
                        line != department and
                        line != location_text and
                        line != job_type_text and
                        len(line) < 80 and
                        len(line.split()) <= 6):  # Reasonable job title length
                        title = line
                        break
                
                # If no specific title found, look for any reasonable title
                if not title:
                    for line in clean_lines:
                        if (line != department and 
                            line != location_text and
                            line != job_type_text and
                            len(line) > 3 and len(line) < 60 and
                            len(line.split()) <= 5 and
                            not any(word in line.lower() for word in ['copyright', 'voyages', 'australia', 'tourism', 'bring your'])):
                            title = line
                            break
                
                # Clean up location text
                if location_text:
                    # Extract clean location
                    for loc in ['Yulara, Northern Territory', 'Mossman Gorge, Queensland', 'Sydney, New South Wales']:
                        if loc.lower() in location_text.lower():
                            location_text = loc
                            break
                    
                    # If still messy, try to extract clean location
                    if len(location_text) > 50:
                        for loc in locations:
                            if len(loc) < 50 and any(state in loc for state in ['Northern Territory', 'Queensland', 'New South Wales']):
                                location_text = loc
                                break
                
                # Clean up job type
                if job_type_text:
                    if 'permanent' in job_type_text.lower() and 'full' in job_type_text.lower():
                        job_type_text = 'Permanent / Full Time'
                    elif 'casual' in job_type_text.lower():
                        job_type_text = 'Casual'
                    elif 'part' in job_type_text.lower():
                        job_type_text = 'Part-time'
                
                # Skip if we don't have a real job title
                if not title or len(title) < 3:
                    continue
                
                # Skip generic titles
                if any(generic in title.lower() for generic in [
                    'search for', 'working at', 'copyright', 'position', 'view job',
                    'all locations', 'front office & guest services', 'food & beverage']):
                    continue
                
                # Create job data
                job_data = {
                    'title': title.strip(),
                    'description': "We are seeking a passionate person who is looking to take an adventure of a lifetime all while growing your career with us.",
                    'department': department.strip() if department else '',
                    'location_text': location_text.strip() if location_text else '',
                    'job_type_text': job_type_text.strip() if job_type_text else 'Full Time',
                    'external_url': self.careers_url,
                    'external_id': f"voyages_{hash(title + department + location_text)}",
                    'posted_ago': '',
                    'salary_info': '',
                    'closing_date': None
                }
                
                # Only add if we have a proper job title
                if title and len(title) > 3:
                    job_data_list.append(job_data)
                    logger.info(f"Parsed job: {title} | {department} | {location_text}")
            
            # If we didn't find jobs through block parsing, try a simpler approach
            if not job_data_list:
                logger.info("Block parsing failed, trying dynamic title detection...")
                job_data_list = self.extract_jobs_by_pattern_matching(lines, departments, locations, job_types)
            
            # Final fallback: Extract from "View Job" context
            if not job_data_list:
                logger.info("Pattern matching failed, using View Job context extraction...")
                job_data_list = self.extract_jobs_from_view_job_context(lines, departments, locations, job_types)
            
            logger.info(f"Total jobs parsed from content: {len(job_data_list)}")
            return job_data_list
            
        except Exception as e:
            logger.error(f"Error parsing jobs from content: {str(e)}")
            return []
    
    def extract_jobs_by_pattern_matching(self, lines, departments, locations, job_types):
        """Extract jobs using dynamic pattern matching."""
        job_data_list = []
        
        # Dynamically detect job title patterns from the content
        job_title_patterns = self.detect_job_title_patterns(lines)
        
        for line in lines:
            # Skip obviously non-job lines
            if (len(line) > 100 or line.startswith('http') or 'View Job' in line or
                any(char.isdigit() for char in line[:3]) or len(line) < 3):
                continue
            
            # Check if line matches job title patterns
            is_job_title = any(pattern.lower() in line.lower() for pattern in job_title_patterns)
            
            if is_job_title:
                # Try to determine location and department from surrounding context
                line_index = lines.index(line) if line in lines else -1
                if line_index >= 0:
                    context_lines = lines[max(0, line_index-5):line_index+6]
                    context_text = ' '.join(context_lines)
                    
                    location_text = ''
                    department = ''
                    job_type_text = ''
                    
                    # Find best matching location
                    for loc in locations:
                        if loc.lower() in context_text.lower():
                            location_text = loc
                            break
                    
                    # Find best matching department
                    for dept in departments:
                        if dept.lower() in context_text.lower():
                            department = dept
                            break
                    
                    # Find best matching job type
                    for jtype in job_types:
                        if jtype.lower() in context_text.lower():
                            job_type_text = jtype
                            break
                    
                    # Only create job if we have some context
                    if location_text or department:
                        job_data = {
                            'title': line.strip(),
                            'description': "We are seeking a passionate person who is looking to take an adventure of a lifetime all while growing your career with us.",
                            'department': department,
                            'location_text': location_text,
                            'job_type_text': job_type_text or 'Permanent / Full Time',
                            'external_url': self.careers_url,
                            'external_id': f"voyages_{hash(line.strip())}",
                            'posted_ago': '',
                            'salary_info': '',
                            'closing_date': None
                        }
                        
                        job_data_list.append(job_data)
                        logger.info(f"Pattern match: {line.strip()} | {department} | {location_text}")
        
        return job_data_list
    
    def detect_job_title_patterns(self, lines):
        """Dynamically detect job title patterns from the content."""
        patterns = set()
        
        # Common job title keywords
        title_keywords = [
            'supervisor', 'assistant', 'manager', 'officer', 'attendant', 
            'coordinator', 'specialist', 'chef', 'steward', 'therapist',
            'driver', 'painter', 'plumber', 'electrician', 'gardener',
            'receptionist', 'clerk', 'analyst', 'technician', 'operator',
            'representative', 'consultant', 'advisor', 'executive',
            'administrator', 'director', 'lead', 'senior', 'junior'
        ]
        
        # Look for lines that contain job title keywords
        for line in lines:
            if (len(line) < 100 and len(line) > 5 and 
                not line.startswith('http') and 'View Job' not in line):
                
                for keyword in title_keywords:
                    if keyword in line.lower():
                        # Extract potential patterns
                        words = line.split()
                        if len(words) <= 5:  # Reasonable job title length
                            patterns.add(keyword)
        
        # If no patterns found, use common fallbacks
        if not patterns:
            patterns = {'supervisor', 'assistant', 'manager', 'officer', 'attendant'}
        
        logger.info(f"Detected job title patterns: {patterns}")
        return list(patterns)
    
    def extract_jobs_from_view_job_context(self, lines, departments, locations, job_types):
        """Extract jobs by analyzing context around 'View Job' buttons."""
        job_data_list = []
        
        for i, line in enumerate(lines):
            if 'View Job' in line:
                # Look at surrounding context (5 lines before and after)
                start_idx = max(0, i - 5)
                end_idx = min(len(lines), i + 6)
                context_lines = lines[start_idx:end_idx]
                context_text = ' '.join(context_lines)
                
                # Try to extract job information from context
                title = ''
                department = ''
                location_text = ''
                job_type_text = ''
                
                # Find department in context
                for dept in departments:
                    if dept.lower() in context_text.lower():
                        department = dept
                        break
                
                # Find location in context  
                for loc in locations:
                    if loc.lower() in context_text.lower():
                        location_text = loc
                        break
                
                # Find job type in context
                for jtype in job_types:
                    if jtype.lower() in context_text.lower():
                        job_type_text = jtype
                        break
                
                # Try to find job title from context lines
                for context_line in context_lines:
                    if (context_line != line and len(context_line) < 100 and 
                        len(context_line) > 5 and not context_line.startswith('http') and
                        context_line != department and context_line != location_text):
                        
                        # Check if this looks like a job title
                        title_keywords = ['supervisor', 'assistant', 'manager', 'officer', 'attendant', 'chef']
                        if any(keyword in context_line.lower() for keyword in title_keywords):
                            title = context_line.strip()
                            break
                
                # If still no title, generate one from department
                if not title and department:
                    title = f"{department} Position"
                elif not title:
                    title = "Available Position"
                
                # Create job data if we have enough information
                if title and (department or location_text):
                    job_data = {
                        'title': title,
                        'description': "We are seeking a passionate person who is looking to take an adventure of a lifetime all while growing your career with us.",
                        'department': department,
                        'location_text': location_text,
                        'job_type_text': job_type_text or 'Permanent / Full Time',
                        'external_url': self.careers_url,
                        'external_id': f"voyages_{hash(context_text)}",
                        'posted_ago': '',
                        'salary_info': '',
                        'closing_date': None
                    }
                    
                    job_data_list.append(job_data)
                    logger.info(f"Context extraction: {title} | {department} | {location_text}")
        
        return job_data_list
    
    def extract_voyages_jobs_precisely(self, page):
        """Extract real job data from Voyages website with fully dynamic approach."""
        job_data_list = []
        
        try:
            logger.info("Starting fully dynamic job extraction...")
            
            # Step 1: Find all "View Job" links dynamically
            view_job_links = []
            
            # Try different selectors to find "View Job" links
            selectors_to_try = [
                'a[href*="positions-available"]',  # Direct position links
                'a[href*="retail-supervisor"]',   # Example pattern from your URL
                'a:has-text("View Job")',          # Links with "View Job" text
                'a[text*="View"]',                 # Links containing "View"
                '.job-link a',                     # Job link containers
                '.position a',                     # Position containers
                'a[href*="careers"]'               # Career related links
            ]
            
            for selector in selectors_to_try:
                try:
                    links = page.query_selector_all(selector)
                    for link in links:
                        href = link.get_attribute('href')
                        if href and self.is_valid_job_url(href):
                            view_job_links.append(link)
                    
                    if view_job_links:
                        logger.info(f"Found {len(view_job_links)} job links using selector: {selector}")
                        break
                except:
                    continue
            
            # Step 2: If no direct links found, look for job cards and extract data dynamically
            if not view_job_links:
                logger.info("No direct job links found, scanning for job containers...")
                view_job_links = self.find_job_containers_dynamically(page)
            
            # Step 3: Extract job data from each found link/container
            for i, link_element in enumerate(view_job_links):
                if self.max_jobs and i >= self.max_jobs:
                    break
                    
                try:
                    job_data = self.extract_job_data_dynamically(page, link_element)
                    if job_data:
                        job_data_list.append(job_data)
                        logger.info(f"Extracted job: {job_data.get('title', 'Unknown')} -> {job_data.get('external_url', 'No URL')}")
                    
                    # Human-like delay
                    self.human_like_delay(0.5, 1)
                    
                except Exception as e:
                    logger.warning(f"Error extracting job data from element {i}: {str(e)}")
                    continue
            
            logger.info(f"Total jobs extracted dynamically: {len(job_data_list)}")
            return job_data_list
            
        except Exception as e:
            logger.error(f"Error in dynamic job extraction: {str(e)}")
            return []
    
    def is_valid_job_url(self, url):
        """Check if URL is a valid job URL."""
        if not url:
            return False
            
        # Check for valid job URL patterns
        valid_patterns = [
            'positions-available',
            'retail-supervisor',
            'maintenance-supervisor',
            'food-beverage',
            'kitchen-steward',
            'front-office',
            'spa-therapist'
        ]
        
        # URL should contain job-related patterns and not be the main careers page
        return (any(pattern in url for pattern in valid_patterns) and
                url != self.careers_url and
                '/careers/' in url and
                '#' not in url)
    
    def find_job_containers_dynamically(self, page):
        """Dynamically find job containers when direct links aren't available."""
        job_containers = []
        
        try:
            # Look for containers that might hold job information
            container_selectors = [
                'div:has-text("View Job")',
                '.job',
                '.position', 
                '.vacancy',
                '.career',
                'div[class*="job"]',
                'div[class*="position"]',
                'div[class*="career"]',
                'li:has-text("View Job")',
                'tr:has-text("View Job")'
            ]
            
            for selector in container_selectors:
                try:
                    containers = page.query_selector_all(selector)
                    for container in containers:
                        container_text = container.inner_text()
                        # Check if container has job-like content
                        if (len(container_text) > 20 and len(container_text) < 500 and
                            ('View Job' in container_text or 
                             any(location in container_text for location in ['Northern Territory', 'Queensland', 'Yulara', 'Mossman']) or
                             any(job_type in container_text for job_type in ['Permanent', 'Full Time', 'Casual', 'Part Time']))):
                            job_containers.append(container)
                    
                    if job_containers:
                        logger.info(f"Found {len(job_containers)} job containers using selector: {selector}")
                        break
                except:
                    continue
            
            return job_containers
            
        except Exception as e:
            logger.error(f"Error finding job containers: {str(e)}")
            return []
    
    def extract_job_data_dynamically(self, page, element):
        """Dynamically extract job data from an element (link or container)."""
        try:
            # Clear previous job page content
            self._current_job_page_content = None
            # Extract URL
            job_url = self.careers_url  # Default fallback
            
            # Get element text and href
            element_text = element.inner_text()
            href = element.get_attribute('href')
            
            # Check if this is a link element with valid job URL
            if href and self.is_valid_job_url(href):
                job_url = urljoin(self.base_url, href) if not href.startswith('http') else href
            else:
                # Look for links within the container
                try:
                    link = element.query_selector('a')
                    if link:
                        href = link.get_attribute('href')
                        if href and self.is_valid_job_url(href):
                            job_url = urljoin(self.base_url, href) if not href.startswith('http') else href
                except:
                    pass
            
            # Try to get broader context
            context_text = element_text
            try:
                # Get parent element context using JavaScript
                parent_info = element.evaluate('''
                    (el) => {
                        const parent = el.parentElement;
                        return parent ? {
                            text: parent.innerText || '',
                            tagName: parent.tagName || ''
                        } : null;
                    }
                ''')
                
                if parent_info and parent_info.get('text'):
                    parent_text = parent_info['text']
                    if len(parent_text) > len(element_text) and len(parent_text) < 1000:
                        context_text = parent_text
            except Exception as e:
                logger.debug(f"Could not get parent context: {str(e)}")
                pass
            
            # Extract job title dynamically from context
            title = self.extract_title_from_context(context_text, element)
            if not title:
                # Try to extract title directly from the href URL
                if job_url != self.careers_url:
                    url_parts = job_url.split('/')
                    if url_parts:
                        title_part = url_parts[-1]  # Get last part of URL
                        # Clean up the title from URL (e.g., retail-supervisor-767091 -> Retail Supervisor)
                        title_clean = title_part.split('-')[:-1]  # Remove ID at end
                        if title_clean:
                            title = ' '.join(word.capitalize() for word in title_clean)
                
                if not title:
                    logger.warning(f"Could not extract title from context: {context_text[:100]}...")
                    return None
            
            # Get enhanced description and closing date (this may provide better context)
            description, closing_date = self.get_job_description(page, job_url, title)
            
            # If we visited the job page, try to get location from there too
            enhanced_context = context_text
            if hasattr(self, '_current_job_page_content') and self._current_job_page_content:
                # Combine original context with job page content for better location detection
                enhanced_context = f"{context_text}\n{self._current_job_page_content}"
                logger.info(f"Enhanced context with job page content for better location detection")
            
            # Extract location with improved accuracy using enhanced context
            location_text = self.extract_location_from_context(enhanced_context)
            
            # Extract department dynamically
            department = self.extract_department_from_context(enhanced_context, title)
            
            # Extract job type
            job_type_text = self.extract_job_type_from_context(enhanced_context, title)
            
            job_data = {
                'title': title,
                'description': description,
                'department': department,
                'location_text': location_text,
                'job_type_text': job_type_text,
                'external_url': job_url,
                'external_id': f"voyages_{hash(job_url + title)}",
                'posted_ago': '',
                'salary_info': '',
                'closing_date': closing_date
            }
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error in dynamic job data extraction: {str(e)}")
            return None
    
    def extract_title_from_context(self, context_text, element):
        """Dynamically extract job title from context."""
        lines = [line.strip() for line in context_text.split('\n') if line.strip()]
        
        # Common job title indicators
        job_indicators = [
            'supervisor', 'assistant', 'manager', 'officer', 'attendant',
            'coordinator', 'specialist', 'chef', 'steward', 'therapist',
            'driver', 'painter', 'plumber', 'electrician', 'gardener',
            'receptionist', 'clerk', 'analyst', 'technician', 'operator',
            'representative', 'consultant', 'advisor', 'executive'
        ]
        
        # Look for lines that contain job title indicators
        for line in lines:
            if (len(line) > 5 and len(line) < 80 and
                not line.startswith('http') and
                'View Job' not in line and
                not any(word in line.lower() for word in ['northern territory', 'queensland', 'yulara', 'mossman', 'permanent', 'full time', 'closing date'])):
                
                # Check if line contains job indicators
                if any(indicator in line.lower() for indicator in job_indicators):
                    return line.strip()
                
                # If no indicators but looks like a title (proper case, reasonable length)
                if (len(line.split()) <= 6 and 
                    line[0].isupper() and
                    not line.isupper() and  # Not all caps
                    not any(char.isdigit() for char in line)):  # No numbers
                    return line.strip()
        
        # Fallback: try to extract from element attributes or nearby elements
        try:
            # Check if element has title-like attributes
            title_attrs = ['title', 'data-title', 'aria-label']
            for attr in title_attrs:
                attr_value = element.get_attribute(attr)
                if attr_value and len(attr_value) > 5 and len(attr_value) < 80:
                    return attr_value.strip()
        except Exception as e:
            logger.debug(f"Could not extract title from attributes: {str(e)}")
            pass
        
        return None
    
    def extract_location_from_context(self, context_text):
        """Extract location with improved accuracy and better pattern matching."""
        if not context_text:
            return 'Yulara, Northern Territory'
            
        context_lower = context_text.lower()
        
        # Enhanced location patterns with more precise matching
        # Order matters - most specific first
        location_patterns = [
            # Exact matches with city and state
            ('yulara, northern territory', 'Yulara, Northern Territory'),
            ('mossman gorge, queensland', 'Mossman Gorge, Queensland'), 
            ('sydney, new south wales', 'Sydney, New South Wales'),
            
            # City names with context indicators
            ('yulara nt', 'Yulara, Northern Territory'),
            ('mossman gorge qld', 'Mossman Gorge, Queensland'),
            ('sydney nsw', 'Sydney, New South Wales'),
            
            # Look for location in structured format (common in job listings)
            ('location: yulara', 'Yulara, Northern Territory'),
            ('location: mossman', 'Mossman Gorge, Queensland'),
            ('location: sydney', 'Sydney, New South Wales'),
            
            # City names alone (but be more careful about context)
            ('yulara', 'Yulara, Northern Territory'),
            ('mossman gorge', 'Mossman Gorge, Queensland'),
            ('mossman', 'Mossman Gorge, Queensland'),
            ('sydney', 'Sydney, New South Wales'),
            
            # State/territory names only
            ('northern territory', 'Northern Territory'),
            ('queensland', 'Queensland'), 
            ('new south wales', 'New South Wales'),
            
            # Abbreviations
            ('nt', 'Northern Territory'),
            ('qld', 'Queensland'),
            ('nsw', 'New South Wales')
        ]
        
        # Split context into lines to check line by line for better accuracy
        lines = [line.strip() for line in context_text.split('\n') if line.strip()]
        
        # First pass: Look for location in dedicated location lines
        for line in lines:
            line_lower = line.lower()
            # Check if this line is specifically about location
            if any(indicator in line_lower for indicator in ['location:', 'where:', 'based in', 'position location']):
                for pattern, location in location_patterns:
                    if pattern in line_lower:
                        logger.info(f"Found location from dedicated line: '{line}' -> {location}")
                        return location
        
        # Second pass: Look for location patterns in context with more weight
        location_scores = {}
        
        for pattern, location in location_patterns:
            if pattern in context_lower:
                # Calculate a score based on specificity and context
                score = len(pattern)  # Longer patterns get higher scores
                
                # Bonus points for being in a separate line (more likely to be location)
                for line in lines:
                    if pattern in line.lower():
                        # More points if the line is short (likely a location line)
                        if len(line) < 50:
                            score += 10
                        # More points if line contains location indicators
                        if any(indicator in line.lower() for indicator in ['location', 'where', 'based', 'territory', 'state']):
                            score += 15
                        break
                
                # Penalty for very common words that might match incorrectly
                if pattern in ['nt', 'qld', 'nsw'] and len(context_text) > 200:
                    score -= 5
                
                if location in location_scores:
                    location_scores[location] = max(location_scores[location], score)
                else:
                    location_scores[location] = score
        
        # Return location with highest score
        if location_scores:
            best_location = max(location_scores.items(), key=lambda x: x[1])
            logger.info(f"Location extraction scores: {location_scores}, selected: {best_location[0]}")
            return best_location[0]
        
        # Third pass: Try to extract from URL if available in context
        if 'positions-available' in context_lower:
            # Try to infer location from common Voyages patterns
            if any(word in context_lower for word in ['resort', 'uluru', 'ayers rock']):
                return 'Yulara, Northern Territory'
            elif any(word in context_lower for word in ['rainforest', 'daintree', 'cairns']):
                return 'Mossman Gorge, Queensland'
        
        # Final fallback with logging
        logger.warning(f"Could not determine location from context: {context_text[:200]}...")
        return 'Yulara, Northern Territory'  # Most common Voyages location
    
    def extract_department_from_context(self, context_text, title):
        """Extract department with improved logic."""
        context_lower = context_text.lower()
        title_lower = title.lower() if title else ''
        
        # Department patterns with priority
        dept_patterns = [
            (['retail', 'shop', 'store', 'sales'], 'Retail'),
            (['food', 'beverage', 'dining', 'restaurant'], 'Food & Beverage'),
            (['kitchen', 'chef', 'cook', 'steward', 'commis'], 'Kitchen'),
            (['housekeeping', 'room attendant', 'laundry'], 'Housekeeping & Commercial Laundry'),
            (['front office', 'guest services', 'reception'], 'Front Office & Guest Services'),
            (['maintenance', 'trades', 'painter', 'plumber', 'electrician', 'grounds'], 'Trades, Maintenance & Transport'),
            (['transport', 'driver', 'airport', 'operations'], 'Transport & Airport Operations'),
            (['administration', 'business support', 'finance', 'office'], 'Administration & Business Support'),
            (['spa', 'therapist', 'wellness'], 'Spa & Wellness'),
            (['security', 'officer'], 'Security'),
            (['management', 'supervisor', 'manager'], 'Management')
        ]
        
        # Check context and title for department indicators
        for keywords, department in dept_patterns:
            if any(keyword in context_lower or keyword in title_lower for keyword in keywords):
                return department
        
        return 'Other'
    
    def extract_job_type_from_context(self, context_text, title):
        """Extract job type from context."""
        context_lower = context_text.lower()
        title_lower = title.lower() if title else ''
        
        if 'casual' in context_lower or 'casual' in title_lower:
            return 'Casual'
        elif 'part time' in context_lower or 'part-time' in context_lower:
            return 'Part-time'
        elif 'fifo' in title_lower or 'contract' in context_lower:
            return 'Contract'
        elif 'temporary' in context_lower or 'temp' in context_lower:
            return 'Temporary'
        else:
            return 'Permanent / Full Time'
    
    def extract_closing_date(self, text):
        """Extract closing date from job description text."""
        import re
        from datetime import datetime
        
        if not text:
            return None
            
        # Common closing date patterns in Australian job descriptions
        patterns = [
            r'Closing Date[:\s]*([A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?,? \d{4})',  # "Closing Date: August 24th, 2025"
            r'Applications close[:\s]*([A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?,? \d{4})',  # "Applications close: August 24th, 2025"
            r'Apply by[:\s]*([A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?,? \d{4})',  # "Apply by: August 24th, 2025"
            r'Deadline[:\s]*([A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?,? \d{4})',  # "Deadline: August 24th, 2025"
            r'Close on[:\s]*([A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?,? \d{4})',  # "Close on: August 24th, 2025"
            r'(\d{1,2}/\d{1,2}/\d{4})',  # "24/08/2025"
            r'(\d{1,2}-\d{1,2}-\d{4})',  # "24-08-2025"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    # Clean up the date string by removing ordinal suffixes
                    cleaned_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
                    logger.info(f"Found closing date: {date_str}")
                    return date_str  # Return the original formatted date
                except Exception as e:
                    logger.warning(f"Could not parse closing date '{date_str}': {str(e)}")
                    continue
        
        return None
    
    def get_job_description(self, page, job_url, title):
        """Get enhanced job description and location from job detail page."""
        default_description = f"Join our team as a {title}. We are seeking a passionate person who is looking to take an adventure of a lifetime all while growing your career with us at Voyages Indigenous Tourism Australia."
        
        # Only try to fetch if we have a real job URL
        if job_url == self.careers_url or '#' in job_url:
            return default_description, None
        
        try:
            # Open job detail page
            detail_page = page.context.new_page()
            detail_page.goto(job_url, wait_until="networkidle", timeout=30000)
            
            # Get the full page content for better location extraction
            page_content = detail_page.inner_text('body')
            
            # Look for job description content
            desc_selectors = [
                '[class*="description"]',
                '.content',
                'main',
                '.job-details', 
                'article',
                '.position-details'
            ]
            
            description = default_description
            closing_date = None
            
            for selector in desc_selectors:
                try:
                    desc_elements = detail_page.query_selector_all(selector)
                    for desc_element in desc_elements:
                        desc_text = desc_element.inner_text().strip()
                        if (len(desc_text) > 100 and 
                            any(keyword in desc_text.lower() for keyword in [
                                'responsibilities', 'duties', 'seeking', 'requirements',
                                'qualifications', 'experience', 'skills', 'about the role'
                            ])):
                            
                            # Extract closing date from the full text before cleaning
                            if not closing_date:
                                closing_date = self.extract_closing_date(desc_text)
                            
                            # Clean up the description but keep closing date info
                            lines = desc_text.split('\n')
                            clean_lines = []
                            for line in lines:
                                line = line.strip()
                                if (line and len(line) > 10 and 
                                    not line.startswith('Apply Now') and
                                    not line.startswith('Book') and
                                    not line.startswith('Skip to')):
                                    clean_lines.append(line)
                            
                            if clean_lines:
                                description = '\n'.join(clean_lines)  # All meaningful lines including closing date
                                break
                except:
                    continue
                
                if description != default_description:
                    break
            
            # Store page content for potential location extraction later
            self._current_job_page_content = page_content
            
            # If closing date not found in description, try to extract from full page content
            if not closing_date:
                closing_date = self.extract_closing_date(page_content)
            
            detail_page.close()
            return description, closing_date
            
        except Exception as e:
            logger.warning(f"Could not fetch description from {job_url}: {str(e)}")
        
        return default_description, None


def main():
    """Main function to run the scraper."""
    # Get max jobs from command line argument
    max_jobs = None
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid max_jobs argument. Please provide a number.")
            sys.exit(1)
    
    logger.info(f"Starting Voyages scraper (max_jobs: {max_jobs or 'unlimited'})")
    
    # Create and run scraper
    scraper = VoyagesScraper(max_jobs=max_jobs, headless=True)
    
    try:
        jobs_scraped = scraper.scrape_jobs()
        logger.info(f"Scraping completed successfully. Jobs scraped: {jobs_scraped}")
        
        # Close database connections
        connections.close_all()
        
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
