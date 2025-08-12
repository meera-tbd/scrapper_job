#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import logging
from decimal import Decimal
import concurrent.futures
import json

# Django setup (same as your professional scraper)
print("Setting up Django environment...")
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("Initializing Django...")
import django
django.setup()
print("Django setup completed")

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

print("Django imports completed")

User = get_user_model()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('scraper_workinaus.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Set debug level for more detailed logging
logger.setLevel(logging.DEBUG)

# Add a console handler for debug output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("Logging setup completed")
logger.debug("Debug logging enabled")


class WorkinAUSScraper:
    def __init__(self, headless=True, job_limit=30, enrich_detail=True, start_page=1, job_category="all", location="all"):
        logger.debug("Initializing WorkinAUS scraper...")
        
        self.headless = headless
        self.base_url = "https://workinaus.com.au"
        self.search_path = "/job/searched"
        self.page_param = "pageNo"
        self.job_limit = job_limit
        self.enrich_detail = enrich_detail
        self.start_page = start_page
        self.job_category = job_category
        self.location = location

        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0

        logger.debug(f"Scraper initialized with: headless={headless}, job_limit={job_limit}, enrich_detail={enrich_detail}, start_page={start_page}, job_category={job_category}, location={location}")
        
        logger.debug("Getting or creating system user...")
        self.system_user = self.get_or_create_system_user()
        if self.system_user:
            logger.debug("System user setup completed")
        else:
            logger.error("Failed to setup system user")
        
        logger.debug("WorkinAUS scraper initialization completed")

    # ---------- Utilities ----------
    def get_or_create_system_user(self):
        """Get or create system user for job postings."""
        try:
            logger.debug("Looking for existing system user...")
            user, created = User.objects.get_or_create(
                username='workinaus_scraper_system',
                defaults=dict(
                    email='system@workinaus-scraper.com',
                    first_name='WorkinAUS',
                    last_name='Scraper',
                    is_staff=True,
                    is_active=True,
                )
            )
            
            if created:
                logger.info("Created new system user: workinaus_scraper_system")
            else:
                logger.debug("Found existing system user: workinaus_scraper_system")
            
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {e}")
            return None

    def human_delay(self, a=1.0, b=3.0):
        """Add a human-like delay between actions."""
        delay = random.uniform(a, b)
        logger.debug(f"Adding human delay: {delay:.2f} seconds")
        time.sleep(delay)
        logger.debug("Human delay completed")

    def parse_date(self, date_string):
        """Parse date string to extract date information."""
        if not date_string:
            logger.debug("No date string provided")
            return None
        
        logger.debug(f"Parsing date: {date_string}")
        s = date_string.lower().strip()
        now = timezone.now()
        
        if 'today' in s:
            logger.debug("Found 'today', returning current date")
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        if 'yesterday' in s:
            logger.debug("Found 'yesterday', returning yesterday's date")
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        m = re.search(r'(\d+)\s*(hour|day|week|month)s?\s*ago', s)
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            logger.debug(f"Found '{n} {unit}(s) ago' pattern")
            delta = dict(hour=timedelta(hours=n),
                         day=timedelta(days=n),
                         week=timedelta(weeks=n),
                         month=timedelta(days=30*n)).get(unit, None)
            if delta:
                result = now - delta
                logger.debug(f"Calculated date: {result}")
                return result
        
        logger.debug("No specific date pattern found, returning current date")
        return now.replace(hour=9, minute=0, second=0, microsecond=0)

    def parse_location(self, location_string):
        """Parse location string to extract state information."""
        if not location_string:
            logger.debug("No location string provided")
            return None
        
        logger.debug(f"Parsing location: {location_string}")
        
        # Clean up the location string
        location_string = location_string.strip()
        
        # Common Australian location patterns
        australian_states = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria', 
            'QLD': 'Queensland',
            'WA': 'Western Australia',
            'SA': 'South Australia',
            'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory',
            'NT': 'Northern Territory'
        }
        
        # Try to find a state abbreviation
        for abbr, full_name in australian_states.items():
            if abbr in location_string.upper():
                logger.debug(f"Found state abbreviation '{abbr}', returning '{full_name}'")
                return full_name
        
        # If no state found, return the original string
        logger.debug(f"No state abbreviation found, returning original: {location_string}")
        return location_string

    def parse_salary(self, salary_text):
        """Parse salary text to extract min and max values - improved for WorkinAUS format."""
        if not salary_text:
            return None, None
        
        # Remove common words and clean up
        salary_text = salary_text.lower().replace('salary', '').replace('package', '').strip()
        
        # Look for salary ranges like "$77,000 - $80,000 Annual" or "$77k - $80k"
        range_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)\s*-\s*\$?(\d+(?:,\d+)?(?:k|000)?)', salary_text)
        if range_match:
            min_sal = self._normalize_salary(range_match.group(1))
            max_sal = self._normalize_salary(range_match.group(2))
            logger.debug(f"Parsed salary range: {min_sal} - {max_sal}")
            return min_sal, max_sal
        
        # Look for single salary like "$60,000" or "$60k"
        single_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)', salary_text)
        if single_match:
            salary = self._normalize_salary(single_match.group(1))
            logger.debug(f"Parsed single salary: {salary}")
            return salary, salary
        
        # Look for patterns like "$77,000 Annual" or "$77k Annual"
        annual_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)\s*(?:per\s+)?(?:year|annual|annum)', salary_text)
        if annual_match:
            salary = self._normalize_salary(annual_match.group(1))
            logger.debug(f"Parsed annual salary: {salary}")
            return salary, salary
        
        # Look for patterns like "$77,000 Hourly" or "$77k Hourly"
        hourly_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)\s*(?:per\s+)?(?:hour|hourly)', salary_text)
        if hourly_match:
            salary = self._normalize_salary(hourly_match.group(1))
            logger.debug(f"Parsed hourly salary: {salary}")
            return salary, salary
        
        logger.debug(f"Could not parse salary text: {salary_text}")
        return None, None

    def _normalize_salary(self, salary_str):
        """Convert salary string to numeric value - improved for WorkinAUS format."""
        try:
            salary_str = salary_str.lower().replace(',', '').strip()
            if 'k' in salary_str:
                return int(salary_str.replace('k', '')) * 1000
            return int(salary_str)
        except (ValueError, AttributeError):
            logger.debug(f"Could not normalize salary: {salary_str}")
            return None

    # ---------- Job Data Extraction ----------
    def _extract_card_fields(self, card):
        """Extract job data from a job card element - improved for WorkinAUS."""
        data = {}
        
        try:
            # Debug: Log the card HTML structure
            card_html = card.inner_html()[:300] + "..." if len(card.inner_html()) > 300 else card.inner_html()
            logger.debug(f"Processing card HTML: {card_html}")
            
            # First try the generic extraction methods
            title = self._extract_title(card)
            data['job_title'] = title
            logger.debug(f"Extracted title: {title}")
            
            url = self._extract_url(card)
            data['job_url'] = url
            logger.debug(f"Extracted URL: {url}")
            
            company = self._extract_company(card)
            data['company_name'] = company
            logger.debug(f"Extracted company: {company}")
            
            location = self._extract_location(card)
            data['location'] = location
            logger.debug(f"Extracted location: {location}")
            
            summary = self._extract_summary(card)
            data['summary'] = summary
            logger.debug(f"Extracted summary: {summary}")
            
            salary_min, salary_max = self._extract_salary(card)
            data['salary_min'] = salary_min
            data['salary_max'] = salary_max
            logger.debug(f"Extracted salary: {salary_min} - {salary_max}")
            
            salary_text = self._extract_salary_text(card)
            data['salary_text'] = salary_text
            logger.debug(f"Extracted salary text: {salary_text}")
            
            job_type = self._extract_job_type(card)
            data['job_type'] = job_type
            logger.debug(f"Extracted job type: {job_type}")
            
            posted_date = self._extract_posted_date(card)
            data['posted_date'] = posted_date
            logger.debug(f"Extracted posted date: {posted_date}")
            
            category = self._extract_category(card)
            data['category'] = category
            logger.debug(f"Extracted category: {category}")
            
            posted_ago = self._extract_posted_ago(card)
            data['posted_ago'] = posted_ago
            logger.debug(f"Extracted posted ago: {posted_ago}")
            
            # If generic extraction didn't work well, try WorkinAUS-specific extraction
            if not data.get('job_title') or not data.get('company_name') or not data.get('location'):
                logger.debug("Generic extraction incomplete, trying WorkinAUS-specific extraction...")
                workinaus_data = self._extract_workinaus_specific_data(card)
                
                # Merge the data, preferring WorkinAUS-specific data for missing fields
                for key, value in workinaus_data.items():
                    if not data.get(key) and value:
                        data[key] = value
                        logger.debug(f"Added missing {key}: {value}")
            
            # Final validation and cleanup
            logger.debug("Final extracted data before validation:")
            for key, value in data.items():
                logger.debug(f"  {key}: {value}")
            
            logger.info(f"Successfully extracted data for job: {data}")
            
        except Exception as e:
            logger.error(f"Error extracting card fields: {e}")
            data = {}
        
        return data

    def _extract_posted_ago(self, card):
        """Extract posted ago information from card."""
        date_selectors = [
            '[class*="posted"]', '.posted', '.date-posted',
            '[class*="date"]', '.date', '.time',
            'small', 'time', '[class*="time"]'
        ]
        
        for selector in date_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    date_text = element.inner_text().strip()
                    if date_text and any(word in date_text.lower() for word in ['ago', 'today', 'yesterday', 'hour', 'day', 'week', 'month']):
                        return date_text
            except:
                continue
        
        return ""

    def _extract_tags(self, job_data):
        """Extract tags from job data."""
        logger.debug(f"Extracting tags from job data: {job_data}")
        tags = []
        
        # Add job type as tag
        if job_data.get('job_type'):
            tags.append(job_data['job_type'])
            logger.debug(f"Added job type tag: {job_data['job_type']}")
        
        # Add category as tag
        if job_data.get('category'):
            tags.append(job_data['category'])
            logger.debug(f"Added category tag: {job_data['category']}")
        
        # Add location as tag
        if job_data.get('location'):
            tags.append(job_data['location'])
            logger.debug(f"Added location tag: {job_data['location']}")
        
        # Add company as tag
        if job_data.get('company_name'):
            tags.append(job_data['company_name'])
            logger.debug(f"Added company tag: {job_data['company_name']}")
        
        # Remove duplicates and join
        unique_tags = list(set(tags))
        final_tags = ', '.join(unique_tags)
        logger.debug(f"Final tags: {final_tags}")
        return final_tags

    def _extract_title(self, card):
        """Extract job title from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS job card structure, job titles are typically in h2 or h3 elements
        # or in elements with specific classes
        title_selectors = [
            'h2', 'h3', 'h4',
            '[class*="job-title"]', '[class*="position-title"]',
            '.job-title', '.position-title', '.title',
            'a[href*="/job/"]', 'a[href*="/position/"]',
            '[class*="title"]'
        ]
        
        for selector in title_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    title = element.inner_text().strip()
                    if title and len(title) > 3 and len(title) < 100:
                        # Clean up the title
                        title = re.sub(r'\s+', ' ', title)
                        if not any(word in title.lower() for word in ['apply', 'view', 'details', 'company', 'location', 'salary']):
                            logger.debug(f"Found job title: {title}")
                            return title
            except:
                continue
        
        # Fallback: look for the most prominent text that could be a job title
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                # Look for lines that could be job titles (reasonable length, no obvious non-title words)
                if (len(line) > 5 and len(line) < 80 and 
                    not any(word in line.lower() for word in ['apply', 'view', 'details', 'company', 'location', 'salary', 'full time', 'part time', 'adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra']) and
                    not re.match(r'^\$[\d,]+', line) and  # Not salary
                    not re.match(r'^\d{4}', line) and      # Not postcode
                    not any(word in line.upper() for word in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) and  # Not state
                    not line.startswith('$') and           # Not salary
                    not line.endswith('Annual') and        # Not salary
                    not line.endswith('Hourly')):
                    logger.debug(f"Found potential job title: {line}")
                    return line
        except:
            pass
        
        return ""

    def _extract_url(self, card):
        """Extract job URL from card - improved for WorkinAUS structure."""
        # Look for the Apply button first, as it typically contains the job URL
        apply_selectors = [
            'a:has-text("Apply")', 'button:has-text("Apply")',
            '.apply-btn', '.btn-apply', '[class*="apply"]',
            'a[href*="apply"]', 'a[href*="job"]',
            'a[href*="details"]'
        ]
        
        for selector in apply_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    href = element.get_attribute('href')
                    if href and href.strip():
                        # Clean and validate the URL
                        href = href.strip()
                        if href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            full_url = urljoin(self.base_url, '/' + href)
                        
                        # Validate it's a job URL
                        if (full_url.startswith('http') and 
                            len(full_url) > 20 and 
                            ('/job/' in full_url or '/position/' in full_url or '/vacancy/' in full_url or '/details' in full_url)):
                            logger.debug(f"Found valid job URL: {full_url}")
                            return full_url
            except:
                continue
        
        # Fallback: look for any job-related links
        url_selectors = [
            'a[href*="/job/"]', 'a[href*="/position/"]', 'a[href*="/vacancy/"]',
            'a[href*="details"]', 'a[href]'
        ]
        
        for selector in url_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    href = element.get_attribute('href')
                    if href and href.strip():
                        href = href.strip()
                        if href.startswith('/'):
                            full_url = urljoin(self.base_url, href)
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            full_url = urljoin(self.base_url, '/' + href)
                        
                        if (full_url.startswith('http') and 
                            len(full_url) > 20 and 
                            ('/job/' in full_url or '/position/' in full_url or '/vacancy/' in full_url)):
                            logger.debug(f"Found valid job URL: {full_url}")
                            return full_url
            except:
                continue
        
        logger.debug("No valid job URL found in card")
        return ""

    def _extract_company(self, card):
        """Extract company name from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, company names are typically in specific elements
        company_selectors = [
            '.company', '.company-name', '.employer', 
            '[class*="company"]', '[class*="employer"]',
            '.job-company', '.employer-name',
            '[class*="business"]', '.business-name'
        ]
        
        for selector in company_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    company = element.inner_text().strip()
                    if company and len(company) > 2 and len(company) < 100:
                        # Clean up company name
                        company = re.sub(r'\s+', ' ', company)
                        if not any(word in company.lower() for word in ['job', 'position', 'apply', 'view', 'full time', 'part time']):
                            logger.debug(f"Found company name: {company}")
                            return company
            except:
                continue
        
        # Fallback: look for company patterns in the card text
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                # Look for lines that might be company names
                if (len(line) > 3 and len(line) < 100 and
                    (any(suffix in line for suffix in ['Pty Ltd', 'Ltd', 'Inc', 'LLC', 'Corp', 'Company', 'Pty', 'Limited', 'Clinic', 'Studio', 'Repairs', 'Care', 'Centre']) or
                     any(word in line.lower() for word in ['clinic', 'studio', 'repairs', 'care', 'centre', 'group', 'services', 'mechanical', 'automotive', 'garage', 'workshop'])) and
                    not any(word in line.lower() for word in ['job', 'position', 'apply', 'view', 'full time', 'part time', 'adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra']) and
                    not re.match(r'^\$[\d,]+', line) and  # Not salary
                    not re.match(r'^\d{4}', line)):        # Not postcode
                    logger.debug(f"Found potential company name: {line}")
                    return line
        except:
            pass
        
        # Additional fallback: look for any text that might be a company name
        try:
            all_elements = card.query_selector_all('div, span, p, strong, b')
            for element in all_elements:
                try:
                    text = element.inner_text().strip()
                    if (text and 3 < len(text) < 100 and 
                        (any(suffix in text for suffix in ['Pty Ltd', 'Ltd', 'Inc', 'LLC', 'Corp', 'Company', 'Pty', 'Limited', 'Clinic', 'Studio', 'Repairs', 'Care', 'Centre']) or
                         any(word in text.lower() for word in ['clinic', 'studio', 'repairs', 'care', 'centre', 'group', 'services', 'mechanical', 'automotive', 'garage', 'workshop'])) and
                        not any(word in text.lower() for word in ['job', 'position', 'apply', 'view', 'full time', 'part time', 'adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra']) and
                        not re.match(r'^\$[\d,]+', text) and
                        not re.match(r'^\d{4}', text)):
                        logger.debug(f"Found potential company name: {text}")
                        return text
                except:
                    continue
        except:
            pass
        
        return "Unknown Company"

    def _extract_location(self, card):
        """Extract location from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, locations are typically in specific elements
        location_selectors = [
            '[class*="location"]', '.job-location', '.location',
            '.meta', '.job-meta', '[class*="meta"]',
            '[class*="address"]', '.address'
        ]
        
        for selector in location_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    location = element.inner_text().strip()
                    if location and len(location) > 2 and len(location) < 100:
                        # Clean up location
                        location = re.sub(r'\s+', ' ', location)
                        if not any(word in location.lower() for word in ['apply', 'view', 'details', 'company', 'salary']):
                            logger.debug(f"Found location: {location}")
                            return location
            except:
                continue
        
        # Fallback: look for location patterns in the card text
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                # Look for location patterns like "City, Postcode, State"
                if (',' in line and 
                    (any(word in line.lower() for word in ['adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra']) or
                     any(word in line.upper() for word in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) or
                     re.search(r'\d{4}', line))):  # Postcode pattern
                    if 5 < len(line) < 100:
                        logger.debug(f"Found location pattern: {line}")
                        return line
        except:
            pass
        
        # Additional fallback: look for any text that might be a location
        try:
            all_elements = card.query_selector_all('div, span, p, strong, b')
            for element in all_elements:
                try:
                    text = element.inner_text().strip()
                    if (text and 5 < len(text) < 100 and 
                        (',' in text or 
                         any(word in text.upper() for word in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) or
                         re.search(r'\d{4}', text)) and
                        not any(word in text.lower() for word in ['apply', 'view', 'details', 'company', 'salary', 'full time', 'part time']) and
                        not re.match(r'^\$[\d,]+', text)):
                        logger.debug(f"Found potential location: {text}")
                        return text
                except:
                    continue
        except:
            pass
        
        return ""

    def _extract_summary(self, card):
        """Extract job summary from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, summaries are typically in specific elements
        summary_selectors = [
            '[class*="description"]', '.job-snippet', '.job-summary',
            '.summary', 'p', 'div[class*="desc"]',
            '[class*="snippet"]', '.snippet'
        ]
        
        for selector in summary_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    summary = element.inner_text().strip()
                    if 20 < len(summary) < 500:
                        # Clean up summary
                        summary = re.sub(r'\s+', ' ', summary)
                        if not any(word in summary.lower() for word in ['apply', 'view', 'details', 'company', 'location', 'salary']):
                            logger.debug(f"Found summary: {summary}")
                            return summary
            except:
                continue
        
        # Fallback: look for meaningful text that might be a job description
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                # Look for lines that might be job descriptions
                if (len(line) > 30 and len(line) < 300 and
                    not any(word in line.lower() for word in ['apply', 'view', 'details', 'company', 'location', 'salary', 'full time', 'part time']) and
                    not re.match(r'^\$[\d,]+', line) and  # Not salary
                    not re.match(r'^\d{4}', line) and      # Not postcode
                    not any(word in line.lower() for word in ['adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra']) and  # Not location
                    not any(word in line.upper() for word in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) and  # Not state
                    ',' in line and  # Likely descriptive text
                    any(word in line.lower() for word in ['employer', 'team', 'company', 'business', 'service', 'quality', 'customer', 'staff', 'work', 'role', 'position', 'seeking', 'looking', 'join', 'proactive', 'recognises', 'rewards', 'conscientious', 'personnel'])):
                    logger.debug(f"Found potential job description: {line}")
                    return line
        except:
            pass
        
        return ""

    def _extract_salary(self, card):
        """Extract salary information from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, salaries are typically in specific elements
        salary_selectors = [
            '[class*="salary"]', '.salary', '.pay',
            '[class*="compensation"]', '.compensation',
            '[class*="rate"]', '.rate'
        ]
        
        for selector in salary_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    salary_text = element.inner_text()
                    if salary_text and '$' in salary_text:
                        return self.parse_salary(salary_text)
            except:
                continue
        
        # Fallback: look for salary patterns in the card text
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                if '$' in line and ('Annual' in line or 'Hourly' in line or '-' in line):
                    return self.parse_salary(line)
        except:
            pass
        
        return None, None

    def _extract_salary_text(self, card):
        """Extract raw salary text from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, salaries are typically in specific elements
        salary_selectors = [
            '[class*="salary"]', '.salary', '.pay',
            '[class*="compensation"]', '.compensation',
            '[class*="rate"]', '.rate'
        ]
        
        for selector in salary_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    salary_text = element.inner_text().strip()
                    if salary_text and '$' in salary_text:
                        return salary_text
            except:
                continue
        
        # Fallback: look for salary patterns in the card text
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                if '$' in line and ('Annual' in line or 'Hourly' in line or '-' in line):
                    return line
        except:
            pass
        
        return ""

    def _extract_job_type(self, card):
        """Extract job type from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, job types are typically in specific elements
        job_type_selectors = [
            '[class*="type"]', '.job-type', '.type',
            '.employment-type', '.work-type', '.job-status',
            '[class*="employment"]', '.employment'
        ]
        
        for selector in job_type_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    job_type = element.inner_text().strip()
                    if job_type and any(word in job_type.lower() for word in ['full time', 'part time', 'casual', 'contract', 'temporary', 'permanent']):
                        logger.debug(f"Found job type: {job_type}")
                        return job_type
            except:
                continue
        
        # Fallback: look for job type patterns in the card text
        try:
            all_text = card.inner_text()
            lines = all_text.split('\n')
            for line in lines:
                line = line.strip()
                # Look for job type patterns
                if any(word in line.lower() for word in ['full time', 'part time', 'casual', 'contract', 'temporary', 'permanent', 'full-time', 'part-time']):
                    if len(line) < 50:  # Job type is usually short
                        logger.debug(f"Found job type: {line}")
                        return line
        except:
            pass
        
        return ""

    def _extract_posted_date(self, card):
        """Extract posted date from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, dates are typically in specific elements
        date_selectors = [
            '[class*="date"]', '.posted-date', '.date',
            '.time', '[class*="time"]', '[class*="posted"]',
            '.posted', '.date-posted'
        ]
        
        for selector in date_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    date_text = element.inner_text().strip()
                    if date_text:
                        parsed_date = self.parse_date(date_text)
                        if parsed_date:
                            logger.debug(f"Found posted date: {date_text}")
                            return parsed_date
            except:
                continue
        
        return None

    def _extract_category(self, card):
        """Extract job category from card - improved for WorkinAUS structure."""
        # Based on the WorkinAUS structure, categories are typically in specific elements
        category_selectors = [
            '[class*="category"]', '.category', '.job-category',
            '.sector', '.industry', '[class*="sector"]',
            '[class*="industry"]'
        ]
        
        for selector in category_selectors:
            try:
                element = card.query_selector(selector)
                if element:
                    category = element.inner_text().strip()
                    if category and len(category) > 2:
                        logger.debug(f"Found category: {category}")
                        return category
            except:
                continue
        
        return ""

    def _extract_workinaus_specific_data(self, card):
        """Extract data using WorkinAUS-specific patterns and structure."""
        data = {}
        
        try:
            logger.debug("Starting WorkinAUS-specific extraction...")
            
            # Look for the specific WorkinAUS job card structure
            # Based on the image, jobs have a specific layout
            
            # Try to find job title in h2/h3 elements or prominent text
            title_element = card.query_selector('h2, h3, h4, [class*="title"], [class*="job-title"]')
            if title_element:
                title = title_element.inner_text().strip()
                if title and len(title) > 3 and len(title) < 100:
                    data['job_title'] = title
                    logger.debug(f"Found WorkinAUS title: {title}")
            else:
                logger.debug("No title element found in WorkinAUS extraction")
            
            # Look for company name - often appears near the top of the card
            company_element = card.query_selector('[class*="company"], [class*="employer"], [class*="business"]')
            if company_element:
                company = company_element.inner_text().strip()
                if company and len(company) > 2 and len(company) < 100:
                    data['company_name'] = company
                    logger.debug(f"Found WorkinAUS company: {company}")
            else:
                logger.debug("No company element found in WorkinAUS extraction")
            
            # Look for location - often appears in meta information
            location_element = card.query_selector('[class*="location"], [class*="address"], [class*="meta"]')
            if location_element:
                location = location_element.inner_text().strip()
                if location and len(location) > 2 and len(location) < 100:
                    data['location'] = location
                    logger.debug(f"Found WorkinAUS location: {location}")
            else:
                logger.debug("No location element found in WorkinAUS extraction")
            
            # Look for job type - often appears as a badge or label
            job_type_element = card.query_selector('[class*="type"], [class*="employment"], [class*="status"]')
            if job_type_element:
                job_type = job_type_element.inner_text().strip()
                if job_type and any(word in job_type.lower() for word in ['full time', 'part time', 'casual', 'contract']):
                    data['job_type'] = job_type
                    logger.debug(f"Found WorkinAUS job type: {job_type}")
            else:
                logger.debug("No job type element found in WorkinAUS extraction")
            
            # Look for salary - often appears prominently
            salary_element = card.query_selector('[class*="salary"], [class*="pay"], [class*="rate"]')
            if salary_element:
                salary_text = salary_element.inner_text().strip()
                if salary_text and '$' in salary_text:
                    data['salary_text'] = salary_text
                    salary_min, salary_max = self.parse_salary(salary_text)
                    data['salary_min'] = salary_min
                    data['salary_max'] = salary_max
                    logger.debug(f"Found WorkinAUS salary: {salary_text}")
            else:
                logger.debug("No salary element found in WorkinAUS extraction")
            
            # Look for job description/summary
            summary_element = card.query_selector('[class*="description"], [class*="summary"], [class*="snippet"], p')
            if summary_element:
                summary = summary_element.inner_text().strip()
                if summary and len(summary) > 20 and len(summary) < 500:
                    data['summary'] = summary
                    logger.debug(f"Found WorkinAUS summary: {summary}")
            else:
                logger.debug("No summary element found in WorkinAUS extraction")
            
            # Look for category/sector
            category_element = card.query_selector('[class*="category"], [class*="sector"], [class*="industry"]')
            if category_element:
                category = category_element.inner_text().strip()
                if category and len(category) > 2:
                    data['category'] = category
                    logger.debug(f"Found WorkinAUS category: {category}")
            else:
                logger.debug("No category element found in WorkinAUS extraction")
            
            # Look for Apply button to get URL
            apply_element = card.query_selector('a:has-text("Apply"), button:has-text("Apply"), [class*="apply"]')
            if apply_element:
                href = apply_element.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        full_url = urljoin(self.base_url, href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin(self.base_url, '/' + href)
                    
                    if full_url.startswith('http') and len(full_url) > 20:
                        data['job_url'] = full_url
                        logger.debug(f"Found WorkinAUS URL: {full_url}")
            else:
                logger.debug("No apply element found in WorkinAUS extraction")
            
            logger.debug(f"WorkinAUS-specific extraction completed: {data}")
            
        except Exception as e:
            logger.error(f"Error in WorkinAUS-specific extraction: {e}")
        
        return data

    # ---------- Enhanced Scraping Methods ----------
    def _search_url_for_page(self, page_no):
        """Generate search URL for a specific page."""
        params = {self.page_param: page_no}
        if self.job_category != "all":
            params['category'] = self.job_category
        if self.location != "all":
            params['location'] = self.location
        
        query_string = urlencode(params)
        url = f"{self.base_url}{self.search_path}?{query_string}"
        logger.debug(f"Generated search URL: {url}")
        return url

    def scrape_page(self, page):
        """Enhanced page scraping with multiple strategies - improved for WorkinAUS."""
        logger.info(f"Scraping page with URL: {page.url}")
        
        # Wait for page to load
        try:
            page.wait_for_load_state('networkidle', timeout=30000)
        except:
            logger.warning("Page load timeout, continuing anyway")
        
        # Debug: Log page title and content
        try:
            page_title = page.title()
            logger.info(f"Page title: {page_title}")
            
            # Check if we're on the right page
            if "job" not in page_title.lower() and "career" not in page_title.lower():
                logger.warning(f"Page title doesn't seem job-related: {page_title}")
        except:
            pass
        
        # Debug: Log the page content structure
        try:
            page_text = page.inner_text()
            logger.debug(f"Page contains {len(page_text)} characters of text")
            
            # Look for job-related indicators
            if 'apply' in page_text.lower():
                logger.debug("Page contains 'apply' text - likely job listings page")
            if 'full time' in page_text.lower() or 'part time' in page_text.lower():
                logger.debug("Page contains job type indicators")
            if '$' in page_text:
                logger.debug("Page contains salary information")
        except:
            pass
        
        # Analyze page structure for debugging
        self._analyze_page_structure(page)
        
        # Strategy 1: Look for job listings in common containers
        job_data = self._strategy_1_find_jobs(page)
        if job_data:
            logger.info(f"Strategy 1 found {len(job_data)} jobs")
            return self._process_job_data(page, job_data)
        
        # Strategy 2: Look for job cards in the page content
        job_data = self._strategy_2_find_jobs(page)
        if job_data:
            logger.info(f"Strategy 2 found {len(job_data)} jobs")
            return self._process_job_data(page, job_data)
        
        # Strategy 3: Look for any clickable elements that might be jobs
        job_data = self._strategy_3_find_jobs(page)
        if job_data:
            logger.info(f"Strategy 3 found {len(job_data)} jobs")
            return self._process_job_data(page, job_data)
        
        # Strategy 4: Look specifically for WorkinAUS job card structure
        job_data = self._strategy_4_find_workinaus_jobs(page)
        if job_data:
            logger.info(f"Strategy 4 (WorkinAUS-specific) found {len(job_data)} jobs")
            return self._process_job_data(page, job_data)
        
        # Strategy 5: Check if we need to interact with filters first
        if self._check_if_filters_needed(page):
            logger.info("Filters detected, attempting to show jobs...")
            return self._handle_filters_and_show_jobs(page)
        
        logger.warning("No jobs found with any strategy")
        return 0

    def _strategy_1_find_jobs(self, page):
        """Strategy 1: Look for job listings in common containers - improved for WorkinAUS."""
        logger.debug("Trying Strategy 1: Common containers...")
        
        # Based on the WorkinAUS structure, jobs are typically in specific containers
        selectors = [
            '[class*="jobs-listing"]',
            '[class*="job-list"]',
            '[class*="search-results"]',
            '[class*="results"]',
            'main',
            'article',
            '.content',
            '.main-content',
            '[class*="job-card"]',
            '[class*="job-item"]'
        ]
        
        for selector in selectors:
            try:
                logger.debug(f"Trying selector: {selector}")
                container = page.query_selector(selector)
                if container:
                    logger.debug(f"Found container with selector: {selector}")
                    # Look for job items within this container
                    job_items = container.query_selector_all('[class*="job"], [class*="item"], [class*="card"], [class*="listing"], [class*="job-card"], [class*="job-item"]')
                    if job_items:
                        logger.info(f"Strategy 1 found {len(job_items)} job items with selector: {selector}")
                        return job_items
                    else:
                        logger.debug(f"Container found but no job items within it using selector: {selector}")
                else:
                    logger.debug(f"No container found with selector: {selector}")
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        logger.debug("Strategy 1: No jobs found")
        return []

    def _strategy_2_find_jobs(self, page):
        """Strategy 2: Look for job cards in the page content - improved for WorkinAUS."""
        logger.debug("Trying Strategy 2: Job cards in content...")
        
        # Based on the WorkinAUS structure, look for specific job card patterns
        selectors = [
            'div[class*="job"]',
            'div[class*="position"]',
            'div[class*="vacancy"]',
            'div[class*="listing"]',
            'div[class*="item"]',
            'div[class*="card"]',
            'section[class*="job"]',
            'article[class*="job"]',
            '[class*="job-card"]',
            '[class*="job-item"]',
            '[class*="position-card"]'
        ]
        
        for selector in selectors:
            try:
                logger.debug(f"Trying selector: {selector}")
                elements = page.query_selector_all(selector)
                if elements and len(elements) > 0:
                    logger.debug(f"Found {len(elements)} elements with selector: {selector}")
                    # Filter out elements that don't look like job cards
                    job_cards = []
                    for element in elements:
                        try:
                            text = element.inner_text()
                            # Check if this element contains job-related content
                            if (text and 
                                any(word in text.lower() for word in ['apply', 'full time', 'part time', 'salary', 'location']) and
                                len(text) > 50):  # Job cards typically have substantial content
                                job_cards.append(element)
                        except:
                            continue
                    
                    if job_cards:
                        logger.info(f"Strategy 2 found {len(job_cards)} job cards with selector: {selector}")
                        return job_cards
                    else:
                        logger.debug(f"Elements found but none look like job cards with selector: {selector}")
                else:
                    logger.debug(f"No elements found with selector: {selector}")
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        logger.debug("Strategy 2: No jobs found")
        return []

    def _strategy_3_find_jobs(self, page):
        """Strategy 3: Look for any clickable elements that might be jobs - improved for WorkinAUS."""
        logger.debug("Trying Strategy 3: Clickable job elements...")
        
        try:
            # Look for any links that might be job-related
            all_links = page.query_selector_all('a[href]')
            logger.debug(f"Found {len(all_links)} total links on page")
            
            job_links = []
            
            for link in all_links:
                try:
                    href = link.get_attribute('href')
                    text = link.inner_text().strip()
                    
                    # Check if this looks like a job link
                    if (href and 
                        ('/job/' in href or '/position/' in href or '/vacancy/' in href or 'details' in href) and
                        text and len(text) > 3 and len(text) < 100 and
                        not any(word in text.lower() for word in ['apply', 'view', 'details', 'company', 'location', 'salary'])):
                        job_links.append(link)
                except:
                    continue
            
            if job_links:
                logger.info(f"Strategy 3 found {len(job_links)} potential job links")
                return job_links
            else:
                logger.debug("Strategy 3: No job links found")
        except Exception as e:
            logger.debug(f"Error in Strategy 3: {e}")
        
        return []

    def _strategy_4_find_workinaus_jobs(self, page):
        """Strategy 4: Look specifically for WorkinAUS job card structure."""
        try:
            logger.info("Trying WorkinAUS-specific job finding strategy...")
            
            # Look for the specific WorkinAUS job card structure
            # Based on the image, jobs appear to be in specific containers
            
            # Try to find the main job listings container
            main_container = page.query_selector('[class*="jobs"], [class*="listings"], [class*="results"], main, .content')
            if main_container:
                logger.debug("Found main container, looking for job cards...")
                
                # Look for individual job cards
                job_cards = main_container.query_selector_all('div, article, section')
                if job_cards:
                    logger.debug(f"Found {len(job_cards)} potential job card elements")
                    
                    # Filter for actual job cards
                    actual_job_cards = []
                    for card in job_cards:
                        try:
                            text = card.inner_text()
                            if text and len(text) > 100:  # Job cards have substantial content
                                # Check if this looks like a job card
                                if any(word in text.lower() for word in ['apply', 'full time', 'part time', 'salary', 'location', 'company']):
                                    actual_job_cards.append(card)
                        except:
                            continue
                    
                    if actual_job_cards:
                        logger.info(f"Strategy 4 (WorkinAUS-specific) found {len(actual_job_cards)} job cards")
                        return actual_job_cards
                    else:
                        logger.debug("Found potential job card elements but none passed the filter")
                else:
                    logger.debug("No potential job card elements found in main container")
            else:
                logger.debug("No main container found")
            
            # Alternative: Look for any elements that contain job-related content
            logger.debug("Trying alternative approach: content analysis...")
            all_elements = page.query_selector_all('div, article, section')
            logger.debug(f"Found {len(all_elements)} total div/article/section elements")
            
            job_elements = []
            
            for element in all_elements:
                try:
                    text = element.inner_text()
                    if text and len(text) > 100:
                        # Check for multiple job indicators
                        indicators = 0
                        if 'apply' in text.lower():
                            indicators += 1
                        if any(word in text.lower() for word in ['full time', 'part time', 'casual', 'contract']):
                            indicators += 1
                        if '$' in text:
                            indicators += 1
                        if any(word in text.lower() for word in ['adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra']):
                            indicators += 1
                        if any(word in text.upper() for word in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']):
                            indicators += 1
                        
                        # If we have multiple indicators, this is likely a job card
                        if indicators >= 2:
                            job_elements.append(element)
                except:
                    continue
            
            if job_elements:
                logger.info(f"Strategy 4 (WorkinAUS-specific) found {len(job_elements)} job elements by content analysis")
                return job_elements
            else:
                logger.debug("Strategy 4: No job elements found by content analysis")
            
        except Exception as e:
            logger.error(f"Error in WorkinAUS-specific strategy: {e}")
        
        logger.debug("Strategy 4: No jobs found")
        return []

    def _check_if_filters_needed(self, page):
        """Check if the page shows filters but no jobs."""
        try:
            logger.debug("Checking if filters are needed...")
            # Look for filter elements
            filter_selectors = [
                '[class*="filter"]',
                '[class*="search"]',
                'select',
                'input[type="search"]',
                '.filters',
                '.search-filters'
            ]
            
            for selector in filter_selectors:
                if page.query_selector(selector):
                    logger.debug(f"Found filter element: {selector}")
                    return True
            
            # Check if page content mentions filters or search
            page_text = page.inner_text().lower()
            if any(word in page_text for word in ['filter', 'search', 'category', 'location']):
                logger.debug("Page content mentions filters or search")
                return True
                
            logger.debug("No filters detected")
        except Exception as e:
            logger.debug(f"Error checking filters: {e}")
        
        return False

    def _handle_filters_and_show_jobs(self, page):
        """Handle filters and try to show jobs."""
        try:
            logger.info("Attempting to interact with filters to show jobs...")
            
            # Try to find and click a search button
            search_button = page.query_selector('button[type="submit"], input[type="submit"], .search-btn, .btn-search')
            if search_button:
                logger.info("Found search button, clicking...")
                search_button.click()
                page.wait_for_load_state('networkidle', timeout=15000)
                self.human_delay(2, 4)
                
                # Now try to find jobs again
                logger.debug("Trying to find jobs after filter interaction...")
                return self._try_all_strategies_again(page)
            
            # Try to select a category if available
            category_select = page.query_selector('select[name*="category"], select[name*="sector"]')
            if category_select:
                logger.info("Found category select, trying to select first option...")
                try:
                    options = category_select.query_selector_all('option')
                    if len(options) > 1:  # Skip the first option if it's "Select category"
                        options[1].click()
                        self.human_delay(1, 2)
                        logger.debug("Trying to find jobs after category selection...")
                        return self._try_all_strategies_again(page)
                except Exception as e:
                    logger.debug(f"Error selecting category: {e}")
            
            logger.debug("No filter interaction possible")
        except Exception as e:
            logger.error(f"Error handling filters: {e}")
        
        return 0

    def _try_all_strategies_again(self, page):
        """Try all job finding strategies again after filter interaction."""
        logger.debug("Trying all strategies again after filter interaction...")
        
        # Try all strategies again
        strategies = [
            ('Strategy 1', self._strategy_1_find_jobs),
            ('Strategy 2', self._strategy_2_find_jobs),
            ('Strategy 3', self._strategy_3_find_jobs),
            ('Strategy 4', self._strategy_4_find_workinaus_jobs)
        ]
        
        for strategy_name, strategy_func in strategies:
            try:
                logger.debug(f"Trying {strategy_name}...")
                job_data = strategy_func(page)
                if job_data:
                    logger.info(f"Found jobs after filter interaction using {strategy_name}")
                    return self._process_job_data(page, job_data)
                else:
                    logger.debug(f"{strategy_name} found no jobs")
            except Exception as e:
                logger.debug(f"Error with {strategy_name}: {e}")
        
        logger.debug("No strategies found jobs after filter interaction")
        return 0

    def _process_job_data(self, page, job_elements):
        """Process found job elements and extract data - improved for WorkinAUS."""
        processed = 0
        
        logger.info(f"Processing {len(job_elements)} job elements...")
        
        for i, element in enumerate(job_elements):
            if self.job_limit and self.scraped_count >= self.job_limit:
                logger.info("Job limit reached.")
                return -1
            
            try:
                logger.info(f"Processing job element {i+1}/{len(job_elements)}")
                
                # Scroll element into view
                element.scroll_into_view_if_needed()
                self.human_delay(0.5, 1.0)
                
                # Log some basic info about the element
                try:
                    element_text = element.inner_text()[:200] + "..." if len(element.inner_text()) > 200 else element.inner_text()
                    logger.debug(f"Element {i+1} text preview: {element_text}")
                except:
                    logger.debug(f"Could not get text for element {i+1}")
                
                # Extract job data
                job_data = self._extract_card_fields(element)
                
                # Log what we extracted for debugging
                logger.info(f"Extracted data for job {i+1}: {job_data}")
                
                # Validate job data
                if not self._is_valid_job_data(job_data):
                    logger.debug(f"Skipping invalid job data for element {i+1}: {job_data}")
                    continue
                
                logger.info(f"Processing valid job {i+1}: {job_data.get('job_title', 'Unknown')}")
                
                # Enrich with detail page if requested
                if self.enrich_detail and job_data.get('job_url'):
                    logger.debug(f"Enriching job {i+1} from detail page...")
                    enriched_data = self.enrich_from_detail(page.context, job_data['job_url'], job_data)
                    if enriched_data:
                        job_data = enriched_data
                        logger.info(f"Enriched job data for element {i+1}: {job_data}")
                
                # Save to database
                if self.save_job_to_database(job_data):
                    processed += 1
                    self.scraped_count += 1
                    logger.info(f"Successfully saved job {i+1}: {job_data.get('job_title')} at {job_data.get('company_name')}")
                else:
                    logger.error(f"Failed to save job {i+1}: {job_data.get('job_title')}")
                
            except Exception as e:
                self.error_count += 1
                logger.error(f"Error processing job element {i+1}: {e}")
                continue
        
        logger.info(f"Finished processing {len(job_elements)} elements, successfully processed {processed}")
        return processed

    def _is_valid_job_data(self, job_data):
        """Check if job data is valid enough to save - improved validation."""
        logger.debug(f"Validating job data: {job_data}")
        
        # Must have at least a title
        if not job_data.get('job_title') or len(job_data['job_title'].strip()) < 3:
            logger.debug(f"Invalid job data - no title or title too short: {job_data}")
            return False
        
        # Title should not be obviously wrong
        title = job_data['job_title'].lower()
        if any(word in title for word in ['unknown', 'error', 'loading', 'searching', 'filter']):
            logger.debug(f"Invalid job data - suspicious title: {job_data}")
            return False
        
        # Must have a company name that's not "Unknown Company"
        if not job_data.get('company_name') or job_data['company_name'] == "Unknown Company" or len(job_data['company_name'].strip()) < 2:
            logger.debug(f"Invalid job data - no company name: {job_data}")
            return False
        
        # Company name should not be obviously wrong
        company = job_data['company_name'].lower()
        if any(word in company for word in ['unknown', 'error', 'loading', 'searching', 'filter', 'job', 'position']):
            logger.debug(f"Invalid job data - suspicious company name: {job_data}")
            return False
        
        # Must have a location
        if not job_data.get('location') or len(job_data['location'].strip()) < 3:
            logger.debug(f"Invalid job data - no location: {job_data}")
            return False
        
        # Location should not be obviously wrong
        location = job_data['location'].lower()
        if any(word in location for word in ['unknown', 'error', 'loading', 'searching', 'filter']):
            logger.debug(f"Invalid job data - suspicious location: {job_data}")
            return False
        
        # URL should be valid if present (and not empty)
        if job_data.get('job_url'):
            if not job_data['job_url'].startswith('http') or len(job_data['job_url'].strip()) < 10:
                logger.debug(f"Invalid job data - invalid or empty URL: {job_data}")
                return False
        
        # Job type should be reasonable
        if job_data.get('job_type'):
            job_type = job_data['job_type'].lower()
            if any(word in job_type for word in ['unknown', 'error', 'loading', 'searching', 'filter']):
                logger.debug(f"Invalid job data - suspicious job type: {job_data}")
                return False
        
        # Summary should be reasonable if present
        if job_data.get('summary'):
            summary = job_data['summary'].lower()
            if any(word in summary for word in ['unknown', 'error', 'loading', 'searching', 'filter']):
                logger.debug(f"Invalid job data - suspicious summary: {job_data}")
                return False
        
        logger.debug(f"Job data validation passed: {job_data}")
        return True

    def enrich_from_detail(self, context, job_url, seed):
        """Open detail page to get full description and additional data."""
        logger.debug(f"Enriching job from detail page: {job_url}")
        data = seed.copy()
        try:
            page = context.new_page()
            logger.debug("Opening new page for enrichment...")
            page.goto(job_url, wait_until='domcontentloaded', timeout=60000)
            self.human_delay(2, 3)

            # Try to extract JSON-LD data first
            logger.debug("Looking for JSON-LD structured data...")
            jd = self._extract_jsonld_from_detail(page)
            if jd:
                logger.debug("Found JSON-LD data, extracting information...")
                # title
                data['job_title'] = jd.get('title') or data['job_title']
                # company
                org = jd.get('hiringOrganization') or {}
                if isinstance(org, dict):
                    data['company_name'] = org.get('name') or data['company_name']
                # description
                desc = jd.get('description') or ""
                if desc:
                    # strip tags
                    desc = re.sub('<[^<]+?>', ' ', desc)
                    desc = re.sub(r'\s+', ' ', desc).strip()
                    data['summary'] = desc[:800]
                # salary
                comp = jd.get('baseSalary') or {}
                if isinstance(comp, dict):
                    sal_txt = ""
                    try:
                        val = comp.get('value') or {}
                        min_v = val.get('minValue'); max_v = val.get('maxValue')
                        unit = (val.get('unitText') or '').lower()
                        if min_v and max_v:
                            sal_txt = f"${min_v}-{max_v} per {unit or 'year'}"
                        elif min_v:
                            sal_txt = f"${min_v} per {unit or 'year'}"
                        elif max_v:
                            sal_txt = f"${max_v} per {unit or 'year'}"
                    except Exception:
                        pass
                    if sal_txt:
                        data['salary_text'] = sal_txt
                # date posted
                dp = jd.get('datePosted')
                if dp:
                    try:
                        data['posted_date'] = dp
                    except Exception:
                        pass
                # location
                loc = jd.get('jobLocation') or {}
                if isinstance(loc, dict):
                    addr = loc.get('address') or {}
                    city = addr.get('addressLocality') or ""
                    region = addr.get('addressRegion') or ""
                    loc_str = f"{city}, {region}".strip(', ')
                    if loc_str:
                        data['location'] = loc_str
                
                logger.debug("JSON-LD enrichment completed")
            else:
                logger.debug("No JSON-LD data found")

            # Fallback: get a longer text chunk
            if not data.get('summary'):
                logger.debug("No summary from JSON-LD, trying fallback extraction...")
                try:
                    main = page.query_selector('article, [class*="job"] [class*="description"], [class*="content"], main, .container')
                    if main:
                        t = (main.inner_text() or '').strip()
                        if len(t) > 60:
                            data['summary'] = re.sub(r'\s+', ' ', t)[:1000]
                            logger.debug("Fallback summary extraction completed")
                except:
                    logger.debug("Fallback summary extraction failed")

            page.close()
            logger.debug("Enrichment page closed")
        except Exception as e:
            logger.debug(f"Detail enrichment error for {job_url}: {e}")
        
        logger.debug(f"Enrichment completed, final data: {data}")
        return data

    def _extract_jsonld_from_detail(self, page):
        """Extract JSON-LD structured data from detail page."""
        try:
            logger.debug("Looking for JSON-LD script tags...")
            nodes = page.query_selector_all('script[type="application/ld+json"]')
            logger.debug(f"Found {len(nodes)} JSON-LD script tags")
            
            for i, n in enumerate(nodes or []):
                try:
                    txt = n.inner_text()
                    if not txt:
                        logger.debug(f"Script tag {i+1} has no content")
                        continue
                    
                    logger.debug(f"Processing JSON-LD script tag {i+1}...")
                    data = json.loads(txt)
                    # could be list or single dict
                    items = data if isinstance(data, list) else [data]
                    logger.debug(f"JSON-LD contains {len(items)} items")
                    
                    for j, it in enumerate(items):
                        if isinstance(it, dict) and it.get('@type') in ('JobPosting', 'Job'):
                            logger.debug(f"Found job posting data in item {j+1}")
                            return it
                        else:
                            logger.debug(f"Item {j+1} is not a job posting (type: {it.get('@type') if isinstance(it, dict) else 'unknown'})")
                            
                except json.JSONDecodeError as e:
                    logger.debug(f"Script tag {i+1} contains invalid JSON: {e}")
                except Exception as e:
                    logger.debug(f"Error processing script tag {i+1}: {e}")
            
            logger.debug("No valid job posting JSON-LD data found")
        except Exception as e:
            logger.debug(f"Error in JSON-LD extraction: {e}")
        
        return None

    # ---------- Database Operations ----------
    def save_job_to_database_sync(self, job_data):
        """Save job data to database synchronously."""
        try:
            logger.debug(f"Attempting to save job to database: {job_data}")
            
            with transaction.atomic():
                # Check for duplicates
                if job_data.get('job_url'):
                    existing = JobPosting.objects.filter(external_url=job_data['job_url']).first()
                    if existing:
                        self.duplicate_count += 1
                        logger.info(f"Duplicate job found: {job_data.get('job_title', 'Unknown')}")
                        return existing
                
                # Get or create company
                company = None
                if job_data.get('company_name'):
                    logger.debug(f"Getting or creating company: {job_data['company_name']}")
                    company, _ = Company.objects.get_or_create(
                        name=job_data['company_name'],
                        defaults={
                            'description': f'Company from WorkinAUS scraper',
                            'website': '',
                            'company_size': 'medium'
                        }
                    )
                    logger.debug(f"Company: {company}")
                
                # Get or create location
                location = None
                if job_data.get('location'):
                    logger.debug(f"Getting or creating location: {job_data['location']}")
                    location_name = self.parse_location(job_data['location'])
                    if location_name:
                        location, _ = Location.objects.get_or_create(
                            name=location_name,
                            defaults={
                                'country': 'Australia',
                                'state': location_name if any(state in location_name for state in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) else ''
                            }
                        )
                        logger.debug(f"Location: {location}")
                
                # Create job posting with correct field names
                logger.debug("Creating JobPosting object...")
                job_posting = JobPosting.objects.create(
                    title=job_data.get('job_title', 'Unknown Position'),
                    company=company,
                    location=location,
                    description=job_data.get('summary', ''),
                    external_url=job_data.get('job_url', ''),
                    salary_min=job_data.get('salary_min'),
                    salary_max=job_data.get('salary_max'),
                    salary_currency='AUD',  # Default to Australian Dollar
                    salary_type='yearly',   # Default to yearly
                    salary_raw_text=job_data.get('salary_text', ''),
                    job_type=self._map_job_type(job_data.get('job_type', '')),
                    job_category=self._map_job_category(job_data.get('category', '')),
                    date_posted=job_data.get('posted_date') or timezone.now(),
                    posted_by=self.system_user,
                    external_source='workinaus.com.au',
                    status='active',
                    posted_ago=job_data.get('posted_ago', ''),
                    tags=self._extract_tags(job_data),
                    additional_info=job_data
                )
                
                logger.info(f"Successfully saved job: {job_posting.title}")
                return job_posting
                
        except Exception as e:
            logger.error(f"Error saving job to database: {e}")
            return None

    def _map_job_type(self, job_type_text):
        """Map job type text to model choices."""
        if not job_type_text:
            logger.debug("No job type text provided, using default: full_time")
            return 'full_time'
        
        logger.debug(f"Mapping job type: {job_type_text}")
        job_type_lower = job_type_text.lower()
        
        if any(word in job_type_lower for word in ['full-time', 'full time', 'permanent']):
            logger.debug(f"Mapped '{job_type_text}' to 'full_time'")
            return 'full_time'
        elif any(word in job_type_lower for word in ['part-time', 'part time', 'casual']):
            logger.debug(f"Mapped '{job_type_text}' to 'part_time'")
            return 'part_time'
        elif any(word in job_type_lower for word in ['contract', 'temporary', 'temp']):
            logger.debug(f"Mapped '{job_type_text}' to 'contract'")
            return 'contract'
        elif any(word in job_type_lower for word in ['internship', 'graduate']):
            logger.debug(f"Mapped '{job_type_text}' to 'internship'")
            return 'internship'
        elif any(word in job_type_lower for word in ['freelance']):
            logger.debug(f"Mapped '{job_type_text}' to 'freelance'")
            return 'freelance'
        else:
            logger.debug(f"Could not map '{job_type_text}', using default: 'full_time'")
            return 'full_time'

    def _map_job_category(self, category_text):
        """Map category text to model choices."""
        if not category_text:
            logger.debug("No category text provided, using default: 'other'")
            return 'other'
        
        logger.debug(f"Mapping job category: {category_text}")
        category_lower = category_text.lower()
        
        # Map common categories
        category_mapping = {
            'technology': ['tech', 'software', 'developer', 'programmer', 'it', 'information technology'],
            'finance': ['finance', 'banking', 'accounting', 'financial'],
            'healthcare': ['health', 'medical', 'nursing', 'doctor', 'hospital'],
            'marketing': ['marketing', 'advertising', 'brand'],
            'sales': ['sales', 'retail'],
            'hr': ['human resources', 'hr', 'recruitment'],
            'education': ['education', 'teaching', 'teacher', 'school'],
            'retail': ['retail', 'shop', 'store'],
            'hospitality': ['hospitality', 'hotel', 'restaurant', 'chef', 'cook'],
            'construction': ['construction', 'building', 'trades'],
            'manufacturing': ['manufacturing', 'factory', 'production'],
            'consulting': ['consulting', 'consultant'],
            'legal': ['legal', 'law', 'lawyer'],
        }
        
        for model_category, keywords in category_mapping.items():
            if any(keyword in category_lower for keyword in keywords):
                logger.debug(f"Mapped '{category_text}' to '{model_category}'")
                return model_category
        
        logger.debug(f"Could not map '{category_text}', using default: 'other'")
        return 'other'

    def save_job_to_database(self, job_data):
        """Save job data to database with thread safety."""
        try:
            logger.debug("Starting threaded database save...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.save_job_to_database_sync, job_data)
                result = future.result(timeout=30)
                logger.debug(f"Database save completed with result: {result}")
                return result
        except Exception as e:
            logger.error(f"Error in threaded save: {e}")
            return None

    # ---------- Main Scraping Logic ----------
    def run(self):
        """Main scraping method."""
        logger.info("Starting WorkinAUS scraper")
        logger.info(f"Limit: {self.job_limit} | Detail enrichment: {self.enrich_detail}")
        logger.info(f"Category: {self.job_category} | Location: {self.location}")

        with sync_playwright() as p:
            logger.info("Launching browser...")
            try:
                browser = p.chromium.launch(headless=self.headless)
                logger.debug("Chromium browser launched successfully")
            except Exception as e:
                logger.error(f"Failed to launch browser: {e}")
                raise
            
            try:
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                logger.debug("Browser context created successfully")
            except Exception as e:
                logger.error(f"Failed to create browser context: {e}")
                browser.close()
                raise
            
            try:
                page = context.new_page()
                logger.debug("New page created successfully")
            except Exception as e:
                logger.error(f"Failed to create new page: {e}")
                context.close()
                browser.close()
                raise
            
            logger.info("Browser setup completed")

            try:
                page_no = self.start_page
                total_cards = 0

                while True:
                    url = self._search_url_for_page(page_no)
                    logger.info(f"Navigating to page {page_no}: {url}")
                    
                    try:
                        logger.debug(f"Starting navigation to: {url}")
                        page.goto(url, wait_until='domcontentloaded', timeout=60000)
                        logger.debug("Page navigation completed, waiting for load...")
                        self.human_delay(2.0, 3.5)
                        logger.info(f"Successfully loaded page {page_no}")
                    except Exception as e:
                        logger.error(f"Error navigating to page {page_no}: {e}")
                        logger.debug(f"Page navigation failed for URL: {url}")
                        break

                    logger.info(f"Starting to scrape page {page_no}...")
                    logger.debug(f"Page URL: {page.url}")
                    logger.debug(f"Page title: {page.title()}")
                    
                    processed = self.scrape_page(page)
                    
                    if processed == -1:
                        logger.info("Job limit reached.")
                        break
                    if processed == 0:
                        logger.info(f"No jobs found on page {page_no}. Stopping.")
                        break

                    total_cards += processed
                    logger.info(f"Processed page {page_no}: {processed} listings (total processed: {total_cards})")

                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info("Reached job limit; stopping.")
                        break

                    page_no += 1
                    logger.debug(f"Moving to next page: {page_no}")
                    self.human_delay(4.0, 7.0)

                logger.info("=" * 50)
                logger.info("WORKINAUS SCRAPING COMPLETED")
                logger.info(f"Pages visited: {page_no - self.start_page + (1 if processed else 0)}")
                logger.info(f"Jobs saved: {self.scraped_count}")
                logger.info(f"Duplicates skipped: {self.duplicate_count}")
                logger.info(f"Errors: {self.error_count}")

                try:
                    logger.debug("Getting total job count from database...")
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                        fut = ex.submit(lambda: JobPosting.objects.count())
                        total_in_db = fut.result(timeout=10)
                        logger.info(f"Total JobPosting in DB: {total_in_db}")
                except Exception as e:
                    logger.info("Total JobPosting in DB: (unavailable)")
                    logger.debug(f"Error getting total count: {e}")
                
                logger.info("=" * 50)

            except Exception as e:
                logger.error(f"Fatal error: {e}")
                raise
            finally:
                logger.info("Closing browser...")
                browser.close()
                logger.info("Browser closed")

    def _analyze_page_structure(self, page):
        """Analyze the page structure to help debug scraping issues."""
        try:
            logger.info("Analyzing page structure...")
            
            # Check for common elements
            elements = {
                'divs': len(page.query_selector_all('div')),
                'articles': len(page.query_selector_all('article')),
                'sections': len(page.query_selector_all('section')),
                'links': len(page.query_selector_all('a')),
                'buttons': len(page.query_selector_all('button')),
                'forms': len(page.query_selector_all('form'))
            }
            
            logger.info(f"Page structure: {elements}")
            
            # Look for specific WorkinAUS elements
            workinaus_elements = {
                'jobs_container': page.query_selector('[class*="jobs"]'),
                'listings_container': page.query_selector('[class*="listings"]'),
                'results_container': page.query_selector('[class*="results"]'),
                'search_container': page.query_selector('[class*="search"]'),
                'filter_container': page.query_selector('[class*="filter"]')
            }
            
            for name, element in workinaus_elements.items():
                if element:
                    logger.info(f"Found {name}: {element.tag_name} with classes: {element.get_attribute('class')}")
                else:
                    logger.debug(f"No {name} found")
            
            # Look for any text that might indicate job content
            page_text = page.inner_text()
            job_indicators = {
                'apply': page_text.lower().count('apply'),
                'full time': page_text.lower().count('full time'),
                'part time': page_text.lower().count('part time'),
                'salary': page_text.count('$'),
                'locations': sum(1 for word in ['adelaide', 'melbourne', 'sydney', 'brisbane', 'perth', 'darwin', 'hobart', 'canberra'] if word in page_text.lower())
            }
            
            logger.info(f"Job indicators found: {job_indicators}")
            
            # Look for potential job card containers
            potential_containers = page.query_selector_all('[class*="card"], [class*="item"], [class*="listing"], [class*="job"]')
            logger.info(f"Found {len(potential_containers)} potential job card containers")
            
            # Sample a few containers to understand their structure
            for i, container in enumerate(potential_containers[:3]):
                try:
                    container_text = container.inner_text()[:100] + "..." if len(container.inner_text()) > 100 else container.inner_text()
                    container_classes = container.get_attribute('class') or 'no-class'
                    logger.debug(f"Container {i+1} classes: {container_classes}, text preview: {container_text}")
                except:
                    logger.debug(f"Could not analyze container {i+1}")
            
        except Exception as e:
            logger.error(f"Error analyzing page structure: {e}")


def main():
    """Main function to run the scraper with command line arguments."""
    print("🔍 Enhanced WorkinAUS Job Scraper")
    print("="*50)
    
    # Parse command line arguments
    job_limit = 30  # Default
    job_category = "all"
    location = "all"
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"Job limit from command line: {job_limit}")
        except ValueError:
            print("Invalid job limit. Using default: 30")
    
    if len(sys.argv) > 2:
        job_category = sys.argv[2].lower()
        print(f"Job category from command line: {job_category}")
    
    if len(sys.argv) > 3:
        location = sys.argv[3].lower()
        print(f"Location from command line: {location}")
    
    print(f"Target: {job_limit} jobs")
    print(f"Category: {job_category}")
    print(f"Location: {location}")
    print("Database: Professional structure with JobPosting, Company, Location")
    print("Enhanced: Multiple scraping strategies, better error handling, filter detection")
    print("Debug: Comprehensive logging for troubleshooting")
    print("="*50)

    print("Initializing scraper...")
    scraper = WorkinAUSScraper(
        headless=True,
        job_limit=job_limit,
        enrich_detail=True,
        start_page=1,
        job_category=job_category,
        location=location
    )
    
    print("Starting scraper...")
    try:
        scraper.run()
        print("Scraper completed successfully!")
    except KeyboardInterrupt:
        print("Interrupted by user")
        logger.info("Interrupted by user")
    except Exception as e:
        print(f"Run failed: {e}")
        logger.error(f"Run failed: {e}")
        raise


if __name__ == "__main__":
    main()