#!/usr/bin/env python3
"""
Professional JobServe Australia Job Scraper
===========================================

Enhanced scraper for JobServe.com focused on Australian jobs with:
- Advanced anti-detection measures
- Human-like browsing behavior
- Robust job listing detection
- Australia-specific location parsing
- Professional database integration
- Enhanced error handling and recovery

Usage:
    python jobserve_australia_scraper_advanced.py [job_limit]
    
Examples:
    python jobserve_australia_scraper_advanced.py 100   # Scrape 100 jobs
    python jobserve_australia_scraper_advanced.py       # Scrape all jobs (no limit)
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
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from django.utils.text import slugify
from playwright.sync_api import sync_playwright
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

class JobServeAustraliaScraper:
    """Advanced JobServe Australia job scraper with enhanced anti-detection."""
    
    def __init__(self, job_category="all", job_limit=None):
        """Initialize the scraper with enhanced settings."""
        self.base_url = "https://www.jobserve.com"
        self.australia_search_url = "https://www.jobserve.com/au/en/Job-Search/"
        self.job_category = job_category
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.duplicate_count = 0
        self.error_count = 0
        self.pages_scraped = 0
        
        # Setup logging with UTF-8 encoding
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('jobserve_australia_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        # Enhanced user agents for better anti-detection
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        # Australian states and territories mapping
        self.australian_states = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria', 
            'QLD': 'Queensland',
            'WA': 'Western Australia',
            'SA': 'South Australia',
            'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory',
            'NT': 'Northern Territory'
        }
    
    def human_delay(self, min_seconds=2, max_seconds=5):
        """Enhanced human-like delay with random variations."""
        base_delay = random.uniform(min_seconds, max_seconds)
        # Add micro-delays to simulate human hesitation
        if random.random() < 0.3:  # 30% chance of slight hesitation
            base_delay += random.uniform(0.5, 1.5)
        time.sleep(base_delay)
    
    def scroll_page_naturally(self, page):
        """Natural scrolling behavior to mimic human browsing."""
        try:
            # Get page height
            page_height = page.evaluate("document.body.scrollHeight")
            viewport_height = page.evaluate("window.innerHeight")
            
            # Scroll down in natural chunks
            current_position = 0
            scroll_steps = 4 + random.randint(0, 3)  # 4-7 scroll steps
            
            for i in range(scroll_steps):
                # Calculate next scroll position with some randomness
                step_size = (page_height / scroll_steps) + random.randint(-100, 100)
                current_position += step_size
                
                # Don't scroll past the page
                if current_position > page_height - viewport_height:
                    current_position = page_height - viewport_height
                
                page.evaluate(f"window.scrollTo(0, {current_position})")
                self.human_delay(0.8, 2.0)  # Pause to "read" content
                
                if current_position >= page_height - viewport_height:
                    break
            
            # Sometimes scroll back up a bit (human behavior)
            if random.random() < 0.4:  # 40% chance
                scroll_back = random.randint(200, 500)
                current_position = max(0, current_position - scroll_back)
                page.evaluate(f"window.scrollTo(0, {current_position})")
                self.human_delay(1, 2)
            
        except Exception as e:
            self.logger.warning(f"Scroll failed: {e}")
    
    def extract_job_details_from_page(self, job_url, page):
        """Extract detailed job information using multiple robust strategies."""
        try:
            self.logger.info(f"Extracting details from job page: {job_url}")
            
            # Navigate to the job detail page
            page.goto(job_url, wait_until='domcontentloaded', timeout=15000)
            self.human_delay(2, 4)
            
            detailed_data = {}
            
            # Get the full page content for text-based extraction
            try:
                page_content = page.content()
                page_text = page.inner_text('body')
                self.logger.debug(f"Page loaded successfully, content length: {len(page_content)}")
            except Exception as e:
                self.logger.warning(f"Could not get page content: {e}")
                page_content = ""
                page_text = ""
            
            # Extract location from job detail page using the actual JobServe structure
            try:
                # Look for "Location" label and get the next cell's value
                location_element = page.query_selector('xpath=//tr[td[contains(text(), "Location")]]/td[2]')
                if location_element:
                    location_text = location_element.inner_text().strip()
                    if location_text and len(location_text) < 200:
                        detailed_data['location_text'] = location_text
                        self.logger.debug(f"Found location via XPath: {location_text}")
            except Exception as e:
                self.logger.debug(f"XPath location extraction failed: {e}")
                
            # Fallback location extraction methods
            if not detailed_data.get('location_text'):
                location_selectors = [
                    'td:contains("Location") + td',  # Location label + adjacent cell
                    '.location-detail', '.job-location-detail'  # Detail page location classes
                ]
                
                for selector in location_selectors:
                    try:
                        location_element = page.query_selector(selector)
                        if location_element:
                            location_text = location_element.inner_text().strip()
                            if location_text and len(location_text) < 200:
                                detailed_data['location_text'] = location_text
                                break
                    except:
                        continue
            
            # Extract company/employment agency from job detail page using JobServe structure
            try:
                # Look for "Employment Agency" label and get the next cell's value
                company_element = page.query_selector('xpath=//tr[td[contains(text(), "Employment Agency")]]/td[2]')
                if company_element:
                    company_text = company_element.inner_text().strip()
                    if company_text and len(company_text) < 150:
                        detailed_data['company_name'] = company_text
                        self.logger.debug(f"Found company via Employment Agency: {company_text}")
            except Exception as e:
                self.logger.debug(f"Employment Agency extraction failed: {e}")
                
            # Try "Posted by" as alternative
            if not detailed_data.get('company_name'):
                try:
                    posted_by_element = page.query_selector('xpath=//tr[td[contains(text(), "Posted by")]]/td[2]')
                    if posted_by_element:
                        company_text = posted_by_element.inner_text().strip()
                        if company_text and len(company_text) < 150:
                            detailed_data['company_name'] = company_text
                            self.logger.debug(f"Found company via Posted by: {company_text}")
                except Exception as e:
                    self.logger.debug(f"Posted by extraction failed: {e}")
                    
            # Fallback company extraction methods
            if not detailed_data.get('company_name'):
                company_selectors = [
                    'td:contains("Contact") + td',  # Contact label + adjacent cell
                    '.company-detail', '.employer-detail'  # Detail page company classes
                ]
                
                for selector in company_selectors:
                    try:
                        company_element = page.query_selector(selector)
                        if company_element:
                            company_text = company_element.inner_text().strip()
                            if company_text and len(company_text) < 150:
                                detailed_data['company_name'] = company_text
                                break
                    except:
                        continue
            
            # Extract salary/rate from job detail page using JobServe structure
            try:
                # Look for "Rate" label and get the next cell's value
                rate_element = page.query_selector('xpath=//tr[td[contains(text(), "Rate")]]/td[2]')
                if rate_element:
                    salary_text = rate_element.inner_text().strip()
                    if salary_text:
                        detailed_data['salary_text'] = salary_text
                        self.logger.debug(f"Found salary via Rate: {salary_text}")
            except Exception as e:
                self.logger.debug(f"Rate extraction failed: {e}")
                
            # Try "Salary" as alternative
            if not detailed_data.get('salary_text'):
                try:
                    salary_element = page.query_selector('xpath=//tr[td[contains(text(), "Salary")]]/td[2]')
                    if salary_element:
                        salary_text = salary_element.inner_text().strip()
                        if salary_text:
                            detailed_data['salary_text'] = salary_text
                            self.logger.debug(f"Found salary via Salary: {salary_text}")
                except Exception as e:
                    self.logger.debug(f"Salary extraction failed: {e}")
                    
            # Fallback salary extraction methods
            if not detailed_data.get('salary_text'):
                salary_selectors = [
                    '.salary-detail', '.rate-detail'  # Detail page salary classes
                ]
                
                for selector in salary_selectors:
                    try:
                        salary_element = page.query_selector(selector)
                        if salary_element:
                            salary_text = salary_element.inner_text().strip()
                            if salary_text:
                                detailed_data['salary_text'] = salary_text
                                break
                    except:
                        continue
            
            # ALTERNATIVE APPROACH: Text-based extraction using regex patterns
            self.logger.info("🔄 Trying text-based extraction as fallback...")
            
            if page_text and not all([detailed_data.get('location_text'), detailed_data.get('company_name'), detailed_data.get('salary_text')]):
                
                # Extract Location using dynamic text patterns (NO STATIC DATA)
                if not detailed_data.get('location_text'):
                    location_patterns = [
                        r'Location[:\s]*([^\n\r]+(?:,\s*[A-Z]{2,3})?(?:,\s*(?:USA|Australia|UK|United States|United Kingdom))?)',
                        r'([A-Za-z\s]+,\s*[A-Z]{2,3}(?:,\s*(?:USA|Australia|UK|United States|United Kingdom))?)',
                        r'([A-Za-z\s]+,\s*[A-Za-z\s&]+,\s*(?:USA|Australia|UK|United States|United Kingdom))',
                        r'([A-Za-z\s]+\s+[A-Za-z]+[^,\n]*(?:,\s*[^,\n]+)*)',  # Generic city pattern
                    ]
                    
                    for pattern in location_patterns:
                        match = re.search(pattern, page_text, re.IGNORECASE)
                        if match:
                            location = match.group(1).strip()
                            if len(location) > 3 and len(location) < 100 and ',' in location:
                                detailed_data['location_text'] = location
                                self.logger.info(f"✅ Found location via text pattern: {location}")
                                break
                
                # Extract Company/Employment Agency using dynamic text patterns (NO STATIC DATA)
                if not detailed_data.get('company_name'):
                    company_patterns = [
                        r'Employment Agency[:\s]*([^\n\r]+)',
                        r'Posted by[:\s]*([^\n\r]+)',
                        r'Contact[:\s]*([^\n\r]+)',
                        r'([A-Za-z\s&]+ (?:Inc|LLC|Ltd|Corp|Company|Group|Agency|Services|Vets|Clinic))',
                        r'([A-Za-z\s&]+ (?:Foods|Medical|Health|Care|Solutions|Systems))',
                        r'([A-Z][a-z]+ [A-Z][a-z]+ [A-Za-z]+)',  # Generic "Name Name Name" pattern
                    ]
                    
                    for pattern in company_patterns:
                        match = re.search(pattern, page_text, re.IGNORECASE)
                        if match:
                            company = match.group(1).strip()
                            
                            # Clean up company name
                            company = re.sub(r'^(by|from|at|posted by|contact)\s+', '', company, flags=re.IGNORECASE)
                            
                            if len(company) > 2 and len(company) < 100:
                                detailed_data['company_name'] = company
                                self.logger.info(f"✅ Found company via text pattern: {company}")
                                break
                
                # Extract Salary/Rate using text patterns
                if not detailed_data.get('salary_text'):
                    salary_patterns = [
                        r'Rate\s*([^\n\r]+)',
                        r'Salary\s*([^\n\r]+)',
                        r'(GBP\s*\d+(?:K|,\d+)?(?:\s*-\s*\d+(?:K|,\d+)?)?)',  # GBP patterns
                        r'(\$\d+(?:\.\d+)?(?:\s*-\s*\$?\d+(?:\.\d+)?)?(?:\s*-\s*\d+)?/hr)',  # $25.83 - 50/hr pattern
                        r'(\$\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?(?:/hr|per hour))',
                        r'(\$\d+(?:,\d+)?(?:\s*-\s*\$?\s*\d+(?:,\d+)?)?)\s*(?:per week|/week)',
                        r'(£\d+(?:,\d+)?(?:\s*-\s*£?\d+(?:,\d+)?)?)',  # GBP pounds
                        r'Competitive|Negotiable|DOE'
                    ]
                    
                    for pattern in salary_patterns:
                        match = re.search(pattern, page_text, re.IGNORECASE)
                        if match:
                            salary = match.group(1) if 'Competitive' not in pattern else match.group(0)
                            detailed_data['salary_text'] = salary.strip()
                            self.logger.info(f"✅ Found salary via text pattern: {salary}")
                            break
            
            # ALTERNATIVE APPROACH 2: CSS selector-based extraction
            if not all([detailed_data.get('location_text'), detailed_data.get('company_name'), detailed_data.get('salary_text')]):
                self.logger.info("🔄 Trying CSS selector-based extraction...")
                
                # Try finding table cells or divs containing the data
                all_cells = page.query_selector_all('td, div, span, p')
                
                for cell in all_cells:
                    try:
                        cell_text = cell.inner_text().strip()
                        if not cell_text:
                            continue
                            
                        # Look for location patterns (DYNAMIC - ANY CITY/STATE/COUNTRY)
                        if not detailed_data.get('location_text'):
                            # Look for patterns like "City, State" or "City, Country"
                            if (',' in cell_text and 
                                len(cell_text.split(',')) >= 2 and 
                                len(cell_text) < 100 and
                                not any(skip in cell_text.lower() for skip in ['posted', 'ago', 'apply', 'view', 'salary'])):
                                detailed_data['location_text'] = cell_text
                                self.logger.info(f"✅ Found location via CSS: {cell_text}")
                        
                        # Look for company patterns (DYNAMIC - ANY COMPANY TYPE)
                        if not detailed_data.get('company_name'):
                            # Look for business entities
                            if (any(keyword in cell_text.lower() for keyword in ['inc', 'llc', 'ltd', 'corp', 'company', 'group', 'agency', 'services', 'vets', 'clinic', 'medical', 'health']) and 
                                len(cell_text) < 100 and
                                not any(skip in cell_text.lower() for skip in ['posted', 'ago', 'apply', 'view', 'location', 'salary'])):
                                detailed_data['company_name'] = cell_text
                                self.logger.info(f"✅ Found company via CSS: {cell_text}")
                        
                        # Look for salary patterns
                        if not detailed_data.get('salary_text'):
                            if '$' in cell_text and any(keyword in cell_text.lower() for keyword in ['hr', 'hour', 'week', '/']) and len(cell_text) < 50:
                                detailed_data['salary_text'] = cell_text
                                self.logger.info(f"✅ Found salary via CSS: {cell_text}")
                                
                    except:
                        continue
            
            # ALTERNATIVE APPROACH 3: Dynamic HTML content parsing (NO STATIC DATA)
            if not all([detailed_data.get('location_text'), detailed_data.get('company_name'), detailed_data.get('salary_text')]) and page_content:
                self.logger.info("🔄 Trying dynamic HTML content parsing...")
                
                # Dynamic location extraction from HTML
                if not detailed_data.get('location_text'):
                    # Look for table rows with location data
                    location_html_patterns = [
                        r'<tr[^>]*>.*?Location.*?<td[^>]*>([^<]+)</td>',
                        r'Location[^<]*</td>\s*<td[^>]*>([^<]+)</td>',
                        r'>Location[^<]*<[^>]*>([^<]+(?:,\s*[^<]+)*)</[^>]*>'
                    ]
                    
                    for pattern in location_html_patterns:
                        match = re.search(pattern, page_content, re.IGNORECASE | re.DOTALL)
                        if match:
                            location = match.group(1).strip()
                            if len(location) > 3 and len(location) < 150:
                                detailed_data['location_text'] = location
                                self.logger.info(f"✅ Found location via HTML parsing: {location}")
                                break
                
                # Dynamic company extraction from HTML
                if not detailed_data.get('company_name'):
                    company_html_patterns = [
                        r'<tr[^>]*>.*?Employment Agency.*?<td[^>]*>([^<]+)</td>',
                        r'Employment Agency[^<]*</td>\s*<td[^>]*>([^<]+)</td>',
                        r'>Employment Agency[^<]*<[^>]*>([^<]+)</[^>]*>',
                        r'<tr[^>]*>.*?Contact.*?<td[^>]*>([^<]+)</td>',
                        r'>Posted by[^<]*<[^>]*>([^<]+)</[^>]*>'
                    ]
                    
                    for pattern in company_html_patterns:
                        match = re.search(pattern, page_content, re.IGNORECASE | re.DOTALL)
                        if match:
                            company = match.group(1).strip()
                            if len(company) > 2 and len(company) < 100:
                                detailed_data['company_name'] = company
                                self.logger.info(f"✅ Found company via HTML parsing: {company}")
                                break
                
                # Dynamic salary extraction from HTML
                if not detailed_data.get('salary_text'):
                    salary_html_patterns = [
                        r'<tr[^>]*>.*?Rate.*?<td[^>]*>([^<]+)</td>',
                        r'Rate[^<]*</td>\s*<td[^>]*>([^<]+)</td>',
                        r'>Rate[^<]*<[^>]*>([^<]+)</[^>]*>',
                        r'<tr[^>]*>.*?Salary.*?<td[^>]*>([^<]+)</td>'
                    ]
                    
                    for pattern in salary_html_patterns:
                        match = re.search(pattern, page_content, re.IGNORECASE | re.DOTALL)
                        if match:
                            salary = match.group(1).strip()
                            if salary and len(salary) < 100:
                                detailed_data['salary_text'] = salary
                                self.logger.info(f"✅ Found salary via HTML parsing: {salary}")
                                break
            
            return detailed_data
            
        except Exception as e:
            self.logger.warning(f"Error extracting job details from page: {e}")
            return {}

    def extract_job_data(self, job_element):
        """Enhanced job data extraction with multiple fallback strategies."""
        try:
            job_data = {}
            
            # Get all text from the job element for fallback parsing
            full_text = ""
            try:
                full_text = job_element.inner_text()
                # Log first 200 characters for debugging
                if full_text:
                    self.logger.debug(f"📄 Job element text (first 200 chars): {full_text[:200]}...")
            except:
                pass
            
            # Job title extraction - JobServe specific selectors (CORRECTED)
            title_selectors = [
                '.sjJobTitle .sjJobLink',                  # JobServe main title structure - PRIMARY
                '.sjJobLink',                              # Direct job link
                'a[href*="jobid"]',                        # JobServe job links
                'h3 a',                                     # H3 title links
                '.job-title a', '.jobTitle a'              # Generic title classes
            ]
            
            job_data['job_title'] = ""
            for selector in title_selectors:
                try:
                    title_element = job_element.query_selector(selector)
                    if title_element:
                        title_text = title_element.inner_text().strip()
                        if title_text and len(title_text) < 200:  # Reasonable title length
                            job_data['job_title'] = title_text
                            break
                except:
                    continue
            
            # Fallback: extract title from text patterns
            if not job_data['job_title'] and full_text:
                # Look for job-like titles at the beginning of text
                lines = full_text.split('\n')
                for line in lines[:3]:  # Check first 3 lines
                    line = line.strip()
                    if (line and len(line) < 150 and 
                        not line.startswith('$') and 
                        not any(word in line.lower() for word in ['posted', 'days ago', 'hours ago'])):
                        job_data['job_title'] = line
                        break
            
            # Job URL extraction - JobServe specific (CORRECTED)
            url_selectors = [
                '.sjJobTitle .sjJobLink',                  # JobServe main link structure - PRIMARY
                '.sjJobLink',                              # Direct job link
                'a[href*="jobid"]',                        # JobServe specific URLs
                'h3 a',                                     # H3 links
                '.job-title a', '.jobTitle a'              # Generic title links
            ]
            
            job_data['job_url'] = ""
            for selector in url_selectors:
                try:
                    link_element = job_element.query_selector(selector)
                    if link_element:
                        href = link_element.get_attribute('href')
                        if href and ('jobid' in href or 'JobListing.aspx' in href or '/job/' in href):
                            job_data['job_url'] = urljoin(self.base_url, href)
                            break
                except:
                    continue
            
            # Company name extraction - JobServe specific (SEARCH PAGE APPROACH)
            company_selectors = [
                '.sjJobCompany',                           # JobServe company class - MAIN SELECTOR
                '.sjJobAdvertiser',                        # Alternative company selector
                '.sjJobRecruiter',                         # Recruiter selector
                '.company-name', '.companyName',           # Generic company classes
                '.company a', '.employer',                 # Company links
                '.advertiser', '.recruiter'                # Recruiter info
            ]
            
            job_data['company_name'] = ""
            for selector in company_selectors:
                try:
                    company_element = job_element.query_selector(selector)
                    if company_element:
                        company_text = company_element.inner_text().strip()
                        # Clean up company name
                        company_text = re.sub(r'^(by|from|at|posted by)\s+', '', company_text, flags=re.IGNORECASE)
                        if company_text and len(company_text) < 100:
                            job_data['company_name'] = company_text
                            break
                except:
                    continue
            
            # Enhanced fallback for company name from text analysis with improved patterns
            if not job_data['company_name'] and full_text:
                # Look for company patterns like "Tay Valley Vets", "Employment Agency: Company Name"
                company_patterns = [
                    r'Employment Agency:\s*([A-Za-z\s&\.]+)',
                    r'Contact:\s*([A-Za-z\s&\.]+)',
                    r'Employer:\s*([A-Za-z\s&\.]+)',
                    r'Company:\s*([A-Za-z\s&\.]+)',
                ]
                
                for pattern in company_patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        company_name = match.group(1).strip()
                        if len(company_name) > 3 and len(company_name) < 80:
                            job_data['company_name'] = company_name
                            break
                
                # Original fallback method if patterns don't work
                if not job_data['company_name']:
                    lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                    for line in lines:
                        if (line and 
                            len(line) > 3 and len(line) < 80 and
                            line != job_data.get('job_title', '') and
                            not line.startswith('$') and
                            not any(skip in line.lower() for skip in ['posted', 'ago', 'permanent', 'contract', 'per']) and
                            not any(state in line for state in self.australian_states.keys()) and
                            not re.match(r'^\d+\s*(hour|day|week|month)', line.lower())):
                            job_data['company_name'] = line
                            break
            
            # Set reasonable default if still not found
            if not job_data['company_name']:
                job_data['company_name'] = "-"
            
            # Location extraction - JobServe specific (SIMPLIFIED APPROACH)
            location_selectors = [
                '.sjJobLocationSalary',                    # JobServe location/salary combined - MAIN SELECTOR
                '.sjJobLocation',                          # Alternative location
                '.location', '.jobLocation',               # Generic location classes
                '.locality', '.city', '.region'           # Geographic terms
            ]
            
            # Enhanced text-based location extraction
            location_patterns = [
                r'([A-Za-z\s]+,\s*[A-Za-z\s]+,\s*(?:UK|Australia|New Zealand|Canada))',  # City, State/Region, Country
                r'([A-Za-z\s]+,\s*(?:NSW|VIC|QLD|WA|SA|TAS|ACT|NT))',  # Australian locations
                r'([A-Za-z\s]+,\s*(?:UK|United Kingdom))',  # UK locations
            ]
            
            job_data['location_text'] = ""
            for selector in location_selectors:
                try:
                    location_element = job_element.query_selector(selector)
                    if location_element:
                        location_text = location_element.inner_text().strip()
                        
                        # Handle JobServe's combined location/salary format
                        if selector == '.sjJobLocationSalary' and ' - ' in location_text:
                            # Split location and salary
                            parts = location_text.split(' - ')
                            location_part = parts[0].strip()
                            
                            # Check if first part is location (contains Australian states)
                            if any(state in location_part for state in self.australian_states.keys()):
                                job_data['location_text'] = location_part
                                
                                # Extract salary from remaining parts if not already found
                                if not job_data.get('salary_text') and len(parts) > 1:
                                    salary_part = ' - '.join(parts[1:])
                                    if any(symbol in salary_part for symbol in ['$', 'AUD', 'per', 'Competitive']):
                                        job_data['salary_text'] = salary_part.strip()
                                break
                        
                        # Regular location extraction - Enhanced for international locations
                        elif location_text:
                            # Check for Australian states first
                            if any(state in location_text for state in self.australian_states.keys()):
                                job_data['location_text'] = location_text
                                break
                            # Also accept UK locations and other international locations
                            elif any(country in location_text.upper() for country in ['UK', 'UNITED KINGDOM', 'AUSTRALIA', 'NEW ZEALAND', 'CANADA']):
                                if len(location_text) < 100:
                                    job_data['location_text'] = location_text
                                    break
                            # Accept any reasonable length location
                            elif len(location_text) < 100 and ',' in location_text:  # Likely a real location with city, state/country
                                job_data['location_text'] = location_text
                                break
                except:
                    continue
            
            # Extract location from full text using enhanced patterns
            if not job_data['location_text'] and full_text:
                # Try location patterns first
                for pattern in location_patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        job_data['location_text'] = match.group(1).strip()
                        break
                
                # Fallback: Look for Australian state abbreviations in text
                if not job_data['location_text']:
                    for state_abbrev in self.australian_states.keys():
                        if state_abbrev in full_text:
                            # Extract surrounding context
                            pattern = rf'([^,\n]*{state_abbrev}[^,\n]*)'
                            match = re.search(pattern, full_text)
                            if match:
                                job_data['location_text'] = match.group(1).strip()
                                break
            
            # Salary extraction - Enhanced for multiple formats (SEARCH PAGE APPROACH)
            salary_selectors = [
                '.salary', '.rate', '.pay',                # Salary classes
                '.compensation', '.wage',                  # Pay-related
                'td:nth-child(4)',                         # Table layout - 4th column
                'div[class*="salary"]',                    # Salary divs
                'span[class*="salary"]'                    # Salary spans
            ]
            
            job_data['salary_text'] = ""
            for selector in salary_selectors:
                try:
                    salary_element = job_element.query_selector(selector)
                    if salary_element:
                        salary_text = salary_element.inner_text().strip()
                        # Enhanced salary detection for multiple currencies and formats
                        if (any(symbol in salary_text for symbol in ['$', '£', '€', 'AUD', 'GBP', 'USD', 'EUR']) or
                            any(word in salary_text.upper() for word in ['COMPETITIVE', 'NEGOTIABLE', 'DOE', 'PER HOUR', 'PER DAY'])):
                            job_data['salary_text'] = salary_text
                            break
                except:
                    continue
            
            # Extract salary from full text if not found - Enhanced pattern matching
            if not job_data['salary_text'] and full_text:
                # Look for explicit salary/rate labels first
                salary_label_patterns = [
                    r'Rate:\s*([^\n\r]+)',
                    r'Salary:\s*([^\n\r]+)',
                    r'Pay:\s*([^\n\r]+)',
                    r'Compensation:\s*([^\n\r]+)',
                ]
                
                for pattern in salary_label_patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        salary_text = match.group(1).strip()
                        if salary_text:
                            job_data['salary_text'] = salary_text
                            break
                
                # Fallback to general salary patterns if label patterns don't work
                if not job_data['salary_text']:
                    salary_patterns = [
                        r'\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*(?:per|/)\s*(?:hour|day|week|month|year|annum))?',
                        r'£[\d,]+(?:\s*[-–]\s*£[\d,]+)?(?:\s*(?:per|/)\s*(?:hour|day|week|month|year|annum))?',
                        r'€[\d,]+(?:\s*[-–]\s*€[\d,]+)?(?:\s*(?:per|/)\s*(?:hour|day|week|month|year|annum))?',
                        r'(?:AUD|GBP|USD|EUR)\s*[\d,]+(?:\s*[-–]\s*(?:AUD|GBP|USD|EUR)\s*[\d,]+)?',
                        r'[\d,]+\s*(?:per|/)\s*(?:hour|day|week|month|year|annum)',
                        r'(?:Competitive|Negotiable|DOE)(?:\s+salary)?'
                    ]
                    
                    for pattern in salary_patterns:
                        match = re.search(pattern, full_text, re.IGNORECASE)
                        if match:
                            job_data['salary_text'] = match.group(0)
                            break
            
            # Job summary/description extraction - JobServe specific (CORRECTED)
            summary_selectors = [
                '.sjJobDesc',                              # JobServe description class - MAIN SELECTOR
                '.sjJobSummary',                           # Alternative summary
                '.sjJobDescription',                       # Alternative description
                '.job-description', '.description',        # Generic description classes
                '.summary', '.snippet'                     # Summary classes
            ]
            
            job_data['summary'] = ""
            for selector in summary_selectors:
                try:
                    summary_element = job_element.query_selector(selector)
                    if summary_element:
                        summary_text = summary_element.inner_text().strip()
                        if summary_text and len(summary_text) > 30:  # Substantial description
                            # Clean up the description text - remove truncation indicators
                            summary_text = re.sub(r'\.{3,}$', '', summary_text)  # Remove trailing ellipsis
                            summary_text = re.sub(r'\s*\.\.\.\s*$', '', summary_text)  # Remove spaced ellipsis
                            
                            # Don't limit length too much - keep full description
                            if len(summary_text) > 2000:
                                # Only truncate if extremely long, and do it cleanly
                                summary_text = summary_text[:1950] + "..."
                            
                            job_data['summary'] = summary_text
                            break
                except:
                    continue
            
            # Enhanced fallback for description extraction
            if not job_data['summary'] and full_text:
                lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                
                # Look for substantial content that's not title/company/location
                description_lines = []
                for line in lines:
                    if (line and 
                        len(line) > 40 and
                        line != job_data.get('job_title', '') and
                        line != job_data.get('company_name', '') and
                        line != job_data.get('location_text', '') and
                        not any(skip in line.lower() for skip in ['posted', 'ago', 'permanent', 'contract']) and
                        not line.startswith('$')):
                        description_lines.append(line)
                        if len(' '.join(description_lines)) > 200:  # Got enough content
                            break
                
                if description_lines:
                    job_data['summary'] = ' '.join(description_lines)
            
            # Date posted extraction - JobServe specific (CORRECTED)
            date_selectors = [
                '.sjJobWhen',                              # JobServe date class - MAIN SELECTOR
                '.sjJobPosted',                            # Alternative posted date
                '.sjJobDate',                              # Alternative date
                '.date', '.posted', '.time',               # Generic date classes
                '.published', '.datePosted'                # Posted date
            ]
            
            date_text = ""
            for selector in date_selectors:
                try:
                    date_element = job_element.query_selector(selector)
                    if date_element:
                        date_text = date_element.inner_text().strip()
                        if any(word in date_text.lower() for word in ['ago', 'today', 'yesterday', 'posted']):
                            break
                except:
                    continue
            
            # Extract date from full text if not found
            if not date_text and full_text:
                date_patterns = [
                    r'\d+\s*(?:day|hour|week|month)s?\s*ago',
                    r'posted\s*\d+\s*(?:day|hour|week|month)s?\s*ago',
                    r'today|yesterday'
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        date_text = match.group(0)
                        break
            
            job_data['posted_ago'] = date_text
            job_data['date_posted'] = self.parse_relative_date(date_text)
            
            # Job type detection
            job_data['job_type_text'] = ""
            type_keywords = {
                'full-time': ['full-time', 'full time', 'permanent'],
                'part-time': ['part-time', 'part time', 'casual'],
                'contract': ['contract', 'contractor', 'temporary', 'temp'],
                'internship': ['internship', 'intern', 'trainee', 'graduate'],
                'freelance': ['freelance', 'freelancer', 'consultant']
            }
            
            full_text_lower = full_text.lower()
            for job_type, keywords in type_keywords.items():
                if any(keyword in full_text_lower for keyword in keywords):
                    job_data['job_type_text'] = job_type.title()
                    break
            
            # Remote work detection
            job_data['remote_work'] = ""
            remote_keywords = ['remote', 'work from home', 'wfh', 'telecommute', 'hybrid']
            if any(keyword in full_text_lower for keyword in remote_keywords):
                if 'hybrid' in full_text_lower:
                    job_data['remote_work'] = "Hybrid"
                else:
                    job_data['remote_work'] = "Remote"
            
            # Set proper defaults for missing data (use "-" instead of placeholder text)
            if not job_data.get('location_text'):
                job_data['location_text'] = "-"
            
            if not job_data.get('summary'):
                job_data['summary'] = "-"
            
            if not job_data.get('salary_text'):
                job_data['salary_text'] = "-"
            
            # Clean up any remaining truncation indicators
            if job_data.get('summary') and job_data['summary'] != "-":
                job_data['summary'] = re.sub(r'\.{3,}$', '', job_data['summary'])
                job_data['summary'] = re.sub(r'\s*\.\.\.\s*$', '', job_data['summary'])
            
            self.logger.debug(f"Final job data: Title='{job_data.get('job_title')}', Company='{job_data.get('company_name')}', Location='{job_data.get('location_text')}', Description length={len(job_data.get('summary', ''))}")
            
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def parse_relative_date(self, date_text):
        """Parse relative date strings into datetime objects."""
        try:
            if not date_text:
                return datetime.now().date()
            
            date_text = date_text.lower().strip()
            today = datetime.now().date()
            
            # Handle "today" or "just posted"
            if any(word in date_text for word in ['today', 'just posted']):
                return today
            
            # Handle "yesterday"
            if 'yesterday' in date_text:
                return today - timedelta(days=1)
            
            # Extract number from "X days ago", "X hours ago", etc.
            numbers = re.findall(r'\d+', date_text)
            if numbers:
                number = int(numbers[0])
                
                if 'hour' in date_text:
                    return today  # Same day for hours
                elif 'day' in date_text:
                    return today - timedelta(days=number)
                elif 'week' in date_text:
                    return today - timedelta(weeks=number)
                elif 'month' in date_text:
                    return today - timedelta(days=number * 30)
            
            return today
            
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_text}': {e}")
            return datetime.now().date()
    
    def parse_australian_location(self, location_string):
        """Parse location string with Australian and international location support."""
        if not location_string:
            return "", "", "", "Australia"
        
        location_string = location_string.strip()
        city = ""
        state = ""
        country = "Australia"  # Default for backward compatibility
        
        # Detect country first
        if 'uk' in location_string.lower() or 'united kingdom' in location_string.lower():
            country = "United Kingdom"
        elif 'new zealand' in location_string.lower():
            country = "New Zealand"
        elif 'canada' in location_string.lower():
            country = "Canada"
        elif any(aus_state in location_string.upper() for aus_state in self.australian_states.keys()):
            country = "Australia"
        
        # Split by common separators
        parts = [p.strip() for p in re.split(r'[,\-\|]', location_string) if p.strip()]
        
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1]
            # Check if state part contains a known Australian state
            for abbrev, full_name in self.australian_states.items():
                if abbrev in state_part.upper():
                    state = full_name
                    break
            else:
                state = state_part
        elif len(parts) == 1:
            # Try to extract state from the single part
            location_parts = location_string.split()
            if len(location_parts) >= 2:
                potential_state = location_parts[-1].upper()
                if potential_state in self.australian_states:
                    state = self.australian_states[potential_state]
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
        """Enhanced salary parsing for Australian jobs."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        min_salary = None
        max_salary = None
        currency = "AUD"  # Default for Australia
        period = "yearly"
        
        try:
            # Detect currency - Enhanced for international currencies
            if '£' in salary_text or 'GBP' in salary_text.upper():
                currency = "GBP"
            elif '€' in salary_text or 'EUR' in salary_text.upper():
                currency = "EUR"
            elif 'USD' in salary_text.upper():
                currency = "USD"
            elif 'AUD' in salary_text.upper():
                currency = "AUD"
            elif '$' in salary_text:
                # Default to AUD for $ in Australian context, but could be USD in UK jobs
                currency = "AUD"  # Keep default but this could be enhanced further
            
            # Clean and extract numbers
            clean_text = re.sub(r'[^\d\s\-–,\.ka-z]', ' ', salary_text.lower())
            
            # Extract salary numbers (handle k for thousands)
            numbers = re.findall(r'\d+(?:\.\d+)?(?:k)?', clean_text)
            
            if numbers:
                parsed_numbers = []
                for num in numbers:
                    try:
                        if 'k' in num:
                            parsed_numbers.append(int(float(num.replace('k', '')) * 1000))
                        else:
                            value = int(float(num))
                            # Filter out unrealistic numbers (like years or IDs)
                            if 1000 <= value <= 1000000:
                                parsed_numbers.append(value)
                    except:
                        continue
                
                if len(parsed_numbers) >= 2:
                    min_salary = min(parsed_numbers)
                    max_salary = max(parsed_numbers)
                elif len(parsed_numbers) == 1:
                    min_salary = parsed_numbers[0]
                    max_salary = parsed_numbers[0]
            
            # Determine period
            if any(word in salary_text.lower() for word in ['hour', 'hr', 'hourly']):
                period = "hourly"
            elif any(word in salary_text.lower() for word in ['day', 'daily']):
                period = "daily"
            elif any(word in salary_text.lower() for word in ['week', 'weekly']):
                period = "weekly"
            elif any(word in salary_text.lower() for word in ['month', 'monthly']):
                period = "monthly"
            else:
                period = "yearly"
                
        except Exception as e:
            self.logger.warning(f"Error parsing salary '{salary_text}': {e}")
        
        return min_salary, max_salary, currency, period, salary_text
    
    def save_job_to_database_sync(self, job_data):
        """Thread-safe database save with enhanced duplicate detection."""
        try:
            connections.close_all()
            
            with transaction.atomic():
                job_url = job_data['job_url']
                job_title = job_data['job_title']
                company_name = job_data['company_name']
                
                # Enhanced duplicate detection
                if JobPosting.objects.filter(external_url=job_url).exists():
                    self.logger.info(f"Duplicate job skipped (URL): {job_title} at {company_name}")
                    self.duplicate_count += 1
                    return False
                
                if JobPosting.objects.filter(title=job_title, company__name=company_name).exists():
                    self.logger.info(f"Duplicate job skipped (Title+Company): {job_title} at {company_name}")
                    self.duplicate_count += 1
                    return False
                
                # Parse and create location
                location_name, city, state, country = self.parse_australian_location(
                    job_data.get('location_text', '')
                )
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
                
                # Create company
                company_slug = slugify(company_name)
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Jobs from JobServe Australia',
                        'website': '',
                        'company_size': 'medium'
                    }
                )
                
                # Parse salary
                min_salary, max_salary, currency, period, salary_display = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Determine job type
                job_type = 'full_time'
                job_type_text = job_data.get('job_type_text', '').lower()
                
                if 'part-time' in job_type_text or 'casual' in job_type_text:
                    job_type = 'part_time'
                elif 'contract' in job_type_text or 'temporary' in job_type_text:
                    job_type = 'contract'
                elif 'internship' in job_type_text or 'graduate' in job_type_text:
                    job_type = 'internship'
                elif 'freelance' in job_type_text:
                    job_type = 'freelance'
                
                # Determine work mode
                work_mode = 'onsite'
                if job_data.get('remote_work') == 'Remote':
                    work_mode = 'remote'
                elif job_data.get('remote_work') == 'Hybrid':
                    work_mode = 'hybrid'
                
                # Categorize job
                category = self.categorization_service.categorize_job(
                    job_title, 
                    job_data.get('summary', '')
                )
                
                # Get system user
                from django.contrib.auth import get_user_model
                User = get_user_model()
                scraper_user, created = User.objects.get_or_create(
                    username='jobserve_au_scraper',
                    defaults={
                        'email': 'scraper@jobserve-australia.local',
                        'is_active': False
                    }
                )
                
                # Create unique slug
                base_slug = slugify(job_title)
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_title,
                    slug=unique_slug,
                    company=company_obj,
                    location=location_obj,
                    posted_by=scraper_user,
                    description=job_data.get('summary', ''),
                    external_url=job_url,
                    external_source='jobserve_australia',
                    job_category=category,
                    job_type=job_type,
                    work_mode=work_mode,
                    salary_min=min_salary,
                    salary_max=max_salary,
                    salary_currency=currency,
                    salary_type=period,
                    salary_raw_text=salary_display,
                    posted_ago=job_data.get('posted_ago', ''),
                    date_posted=job_data.get('date_posted'),
                    status='active'
                )
                
                self.logger.info(f"✓ Saved job: {job_title} at {company_name}")
                self.logger.info(f"  Category: {category}")
                self.logger.info(f"  Location: {location_name}")
                
                if min_salary:
                    self.logger.info(f"  Salary: {currency} {min_salary:,} per {period}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving job to database: {e}")
            self.error_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Thread-safe wrapper for database operations."""
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.save_job_to_database_sync, job_data)
                return future.result(timeout=30)
        except Exception as e:
            self.logger.error(f"Database operation failed: {e}")
            return False
    
    def scrape_jobs_from_page(self, page):
        """Enhanced job scraping with better detection and empty page handling."""
        jobs_found = 0
        
        try:
            # Wait for page to load job listings
            self.human_delay(3, 6)
            
            # Check if page has any jobs at all
            page_content = page.content().lower()
            if '0 jobs found' in page_content or 'no matching jobs found' in page_content:
                self.logger.warning("Page shows '0 jobs found' - no jobs to scrape")
                return 0, False
            
            # Check for job count indicator
            jobs_count_match = re.search(r'(\d+)\s+jobs?\s+found', page_content)
            if jobs_count_match:
                jobs_available = int(jobs_count_match.group(1))
                self.logger.info(f"Page indicates {jobs_available} jobs available")
                if jobs_available == 0:
                    return 0, False
            else:
                # If we can't find a job count, check for actual job elements
                test_elements = page.query_selector_all('.sjJobItem, .job-item, .job-listing')
                if len(test_elements) == 0:
                    self.logger.warning("No job elements found on page - may be empty")
                    return 0, False
                else:
                    self.logger.info(f"Found {len(test_elements)} potential job elements to process")
            
            # Scroll to load all content
            self.scroll_page_naturally(page)
            
            # Multiple strategies to find job listings based on JobServe structure
            job_elements = []
            
            # Strategy 1: Look for JobServe-specific job listing patterns (UPDATED)
            job_container_selectors = [
                '.sjJobItem',                              # JobServe job containers - CORRECT STRUCTURE
                'div[class*="sjJob"]',                     # JobServe job-related divs
                'div[id*="JobItem"]'                       # JobServe job item divs
            ]
            
            for selector in job_container_selectors:
                try:
                    page.wait_for_selector(selector, timeout=5000)
                    elements = page.query_selector_all(selector)
                    
                    # Filter out non-job elements - Updated for JobServe
                    valid_elements = []
                    for element in elements:
                        try:
                            text = element.inner_text()
                            # Check if element contains job-like content based on JobServe patterns
                            if (text and len(text) > 20 and 
                                (any(indicator in text.lower() for indicator in ['driver', 'nurse', 'manager', 'engineer', 'surgeon', 'analyst', 'developer', 'specialist', 'coordinator', 'supervisor']) or
                                 element.query_selector('a[href*="jobid"]')) and  # Has job link
                                not any(skip in text.lower() for skip in ['filter', 'search', 'refine', 'sort by', 'cookies', 'enable', 'mobile site'])):
                                valid_elements.append(element)
                        except:
                            continue
                    
                    if len(valid_elements) > 2:  # Need reasonable number of jobs
                        job_elements = valid_elements
                        self.logger.info(f"Found {len(job_elements)} job listings using selector: {selector}")
                        break
                        
                except Exception as e:
                    continue
            
            # Strategy 2: Look for job links and get their containers - JobServe specific
            if not job_elements:
                try:
                    # JobServe uses specific URL patterns - confirmed working pattern
                    job_links = page.query_selector_all('a[href*="jobid"]')
                    containers = []
                    
                    for link in job_links:
                        try:
                            # Get the job container (parent elements) - h3 is the main container
                            container = link.query_selector('xpath=ancestor::h3')
                            if container:
                                containers.append(container)
                            else:
                                # If no h3 parent, try other containers
                                container = link.query_selector('xpath=ancestor::div | ancestor::article')
                                if container:
                                    containers.append(container)
                                else:
                                    containers.append(link)
                        except:
                            continue
                    
                    if containers:
                        job_elements = containers
                        self.logger.info(f"Found {len(job_elements)} job containers from JobServe jobid links")
                        
                except Exception as e:
                    self.logger.warning(f"JobServe link strategy failed: {e}")
            
            # Strategy 3: Fallback - look for any elements with job URLs
            if not job_elements:
                try:
                    all_elements = page.query_selector_all('div, tr, article')
                    for element in all_elements:
                        try:
                            if element.query_selector('a[href*="/job/"]'):
                                job_elements.append(element)
                        except:
                            continue
                    
                    if job_elements:
                        self.logger.info(f"Found {len(job_elements)} potential job elements as fallback")
                        
                except Exception as e:
                    self.logger.warning(f"Fallback strategy failed: {e}")
            
            self.logger.info(f"Processing {len(job_elements)} job elements")
            
            # Process each job element
            for i, job_element in enumerate(job_elements):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit of {self.job_limit}. Stopping.")
                        return jobs_found, True
                    
                    # Extract basic job data from search results
                    job_data = self.extract_job_data(job_element)
                    
                    if (job_data and 
                        job_data.get('job_title') and 
                        job_data.get('job_url') and
                        len(job_data['job_title']) > 5):  # Valid job data
                        
                        # Extract detailed information from individual job page
                        try:
                            if job_data.get('job_url'):
                                self.logger.info(f"🔍 Navigating to job detail page: {job_data.get('job_url')}")
                                detailed_data = self.extract_job_details_from_page(job_data['job_url'], page)
                                
                                # Merge detailed data with basic data (detailed data takes priority)
                                if detailed_data.get('location_text'):
                                    job_data['location_text'] = detailed_data['location_text']
                                    self.logger.info(f"✅ Found location: {detailed_data['location_text']}")
                                
                                if detailed_data.get('company_name'):
                                    job_data['company_name'] = detailed_data['company_name']
                                    self.logger.info(f"✅ Found company: {detailed_data['company_name']}")
                                
                                if detailed_data.get('salary_text'):
                                    job_data['salary_text'] = detailed_data['salary_text']
                                    self.logger.info(f"✅ Found salary: {detailed_data['salary_text']}")
                                
                                # Navigate back to search results
                                page.go_back(wait_until='domcontentloaded', timeout=15000)
                                self.human_delay(2, 4)
                                
                        except Exception as e:
                            self.logger.warning(f"Could not extract detailed data for job: {e}")
                        
                        # Log final extracted data
                        self.logger.info(f"📋 FINAL DATA - Title: '{job_data.get('job_title')}', Company: '{job_data.get('company_name')}', Location: '{job_data.get('location_text')}', Salary: '{job_data.get('salary_text')}')")
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            self.jobs_scraped += 1
                            jobs_found += 1
                            self.logger.info(f"Processed job {self.jobs_scraped}: {job_data['job_title']}")
                        
                        # Human-like delay between jobs
                        self.human_delay(1, 3)
                    else:
                        self.logger.debug(f"Skipped invalid job data at index {i}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing job element {i}: {e}")
                    self.error_count += 1
                    continue
            
            return jobs_found, False
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from page: {e}")
            return 0, False
    
    def navigate_to_next_page(self, page):
        """Enhanced next page navigation."""
        try:
            # Look for next page buttons
            next_selectors = [
                'a[aria-label*="Next"]',
                'a:has-text("Next")', 'a:has-text(">")',
                '.pagination a[rel="next"]',
                '.next', '.pagination-next',
                'input[value="Next"]', 'button:has-text("Next")',
                '.pager .next'
            ]
            
            next_element = None
            for selector in next_selectors:
                try:
                    next_element = page.query_selector(selector)
                    if next_element and next_element.is_enabled():
                        break
                except:
                    continue
            
            if next_element:
                self.logger.info("Navigating to next page...")
                
                # Scroll to next button naturally
                next_element.scroll_into_view_if_needed()
                self.human_delay(2, 4)
                
                # Click with human-like behavior
                next_element.click()
                
                # Wait for new page to load
                page.wait_for_load_state('domcontentloaded', timeout=30000)
                self.human_delay(3, 6)
                
                return True
            else:
                self.logger.info("No next page button found")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False
    
    def setup_australia_search(self, page):
        """Navigate directly to JobServe Australia search page with broader search."""
        try:
            self.logger.info("Navigating directly to JobServe Australia...")
            
            # Try the main Australia job search page first
            self.logger.info(f"Navigating to: {self.australia_search_url}")
            page.goto(self.australia_search_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(3, 5)
            
            # Log the actual URL we ended up on
            current_url = page.url
            self.logger.info(f"Current page URL: {current_url}")
            
            # Get page title and initial content for debugging
            page_title = page.title()
            self.logger.info(f"Page title: {page_title}")
            
            # Check for "0 jobs found" or empty results
            page_content = page.content().lower()
            if '0 jobs found' in page_content or 'no matching jobs found' in page_content:
                self.logger.warning("No jobs found with current search criteria. Trying global search...")
                
                # Try global search for all countries (where jobs actually exist)
                global_search_url = "https://www.jobserve.com/JobSearch.aspx"
                page.goto(global_search_url, wait_until='domcontentloaded', timeout=30000)
                self.human_delay(3, 5)
                
                # Check again
                page_content = page.content().lower()
                if '0 jobs found' in page_content:
                    self.logger.warning("Still no jobs found. Trying specific country with jobs...")
                    # Try United States where there are 151176 jobs available
                    us_search_url = "https://www.jobserve.com/us/en/JobSearch.aspx"
                    page.goto(us_search_url, wait_until='domcontentloaded', timeout=30000)
                    self.human_delay(3, 5)
            
            # Check if we have jobs now
            page_content = page.content().lower()
            if 'australia' in page_content or 'jobs found' in page_content:
                self.logger.info("Successfully loaded JobServe page with job listings")
                return True
            else:
                self.logger.warning("May not have found jobs, but continuing...")
                return True
                        
        except Exception as e:
            self.logger.error(f"Error navigating to JobServe page: {e}")
            return False
    
    def run(self):
        """Main scraping method with enhanced anti-detection."""
        print("🇦🇺 Professional JobServe Australia Job Scraper")
        print("=" * 55)
        print(f"Target: {self.job_limit or 'All'} Australian jobs")
        print("Database: Professional structure with enhanced duplicate detection")
        print("Features: Human-like behavior, anti-detection, robust parsing")
        print("=" * 55)
        
        self.logger.info("Starting JobServe Australia job scraper...")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        with sync_playwright() as p:
            # Launch browser with enhanced stealth settings
            browser = p.chromium.launch(
                headless=True,  # Keep visible for debugging
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-extensions',
                    '--no-first-run',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-ipc-flooding-protection',
                    '--disable-default-apps',
                    '--disable-sync',
                    '--no-default-browser-check',
                    '--disable-web-security',
                    '--allow-running-insecure-content',
                    f'--user-agent={random.choice(self.user_agents)}'
                ]
            )
            
            # Create context with enhanced stealth and NO CACHE
            context = browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Cache-Control': 'no-cache, no-store, must-revalidate',  # Force no cache
                    'Pragma': 'no-cache',  # HTTP 1.0 cache control
                    'Expires': '0',  # Force expiration
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"'
                }
            )
            
            # Add comprehensive stealth scripts
            context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // Mock chrome object
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };
                
                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-AU', 'en', 'en-US'],
                });
            """)
            
            page = context.new_page()
            
            try:
                # Navigate directly to JobServe Australia with retry logic
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        self.logger.info(f"Attempt {attempt + 1}: Navigating to JobServe Australia...")
                        
                        # Go directly to Australia page
                        if not self.setup_australia_search(page):
                            raise Exception("Failed to load Australia search page")
                        
                        # Accept cookies if banner exists
                        cookie_selectors = [
                            'button:has-text("Allow all cookies")',
                            'button:has-text("Accept all cookies")',
                            'button:has-text("Accept")',
                            'button[id*="cookie"]',
                            '.cookie-accept',
                            '#cookie-accept'
                        ]
                        
                        for selector in cookie_selectors:
                            try:
                                cookie_button = page.query_selector(selector)
                                if cookie_button:
                                    self.logger.info("Accepting cookies...")
                                    cookie_button.click()
                                    self.human_delay(1, 3)
                                    break
                            except:
                                continue
                        
                        self.logger.info(f"Successfully loaded Australia page on attempt {attempt + 1}")
                        break
                        
                    except Exception as e:
                        self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt == max_retries - 1:
                            raise
                        self.human_delay(5, 10)
                
                # Start scraping process
                page_number = 1
                consecutive_empty_pages = 0
                
                while True:
                    self.logger.info(f"Scraping page {page_number}...")
                    
                    # Scrape jobs from current page
                    jobs_found, should_stop = self.scrape_jobs_from_page(page)
                    
                    if should_stop:
                        self.logger.info("Job limit reached, stopping scraping.")
                        break
                    
                    if jobs_found == 0:
                        consecutive_empty_pages += 1
                        self.logger.warning(f"No jobs found on page {page_number} (consecutive empty: {consecutive_empty_pages})")
                        
                        if consecutive_empty_pages >= 2:
                            self.logger.info("Multiple consecutive empty pages, ending scraping.")
                            break
                    else:
                        consecutive_empty_pages = 0  # Reset counter
                    
                    # Try to navigate to next page
                    if not self.navigate_to_next_page(page):
                        self.logger.info("No more pages available.")
                        break
                    
                    page_number += 1
                    
                    # Safety limits
                    if page_number > 100:
                        self.logger.info("Reached maximum page limit (100).")
                        break
                
            except Exception as e:
                self.logger.error(f"Scraping failed: {e}")
                self.error_count += 1
            
            finally:
                browser.close()
        
        # Final statistics
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: JobPosting.objects.filter(external_source='jobserve_australia').count())
                total_jobs_in_db = future.result(timeout=10)
        except:
            total_jobs_in_db = "Unknown"
        
        # Print comprehensive results
        print("\n" + "=" * 55)
        print("🎉 JOBSERVE AUSTRALIA SCRAPING COMPLETED!")
        print("=" * 55)
        print(f"📊 Pages scraped: {page_number}")
        print(f"✅ Jobs successfully scraped: {self.jobs_scraped}")
        print(f"🔄 Duplicate jobs skipped: {self.duplicate_count}")
        print(f"❌ Errors encountered: {self.error_count}")
        print(f"💾 Total JobServe Australia jobs in database: {total_jobs_in_db}")
        print(f"📈 Success rate: {(self.jobs_scraped / max(1, self.jobs_scraped + self.duplicate_count + self.error_count)) * 100:.1f}%")
        print("=" * 55)
        
        self.logger.info("Scraping completed successfully!")

def main():
    """Main entry point with enhanced argument parsing."""
    print("🚀 JobServe Australia Professional Scraper")
    print("Enhanced with anti-detection and Australian job focus")
    print("-" * 55)
    
    job_limit = None
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"Job limit set to: {job_limit}")
        except ValueError:
            print("❌ Error: Job limit must be a number")
            print("Usage: python jobserve_australia_scraper_advanced.py [job_limit]")
            print("Example: python jobserve_australia_scraper_advanced.py 100")
            sys.exit(1)
    else:
        print("No job limit set - will scrape all available jobs")
    
    print("-" * 55)
    
    try:
        scraper = JobServeAustraliaScraper(job_limit=job_limit)
        scraper.run()
        
    except KeyboardInterrupt:
        print("\n🛑 Scraping interrupted by user")
    except Exception as e:
        print(f"\n💥 Scraping failed with error: {e}")
        raise

if __name__ == "__main__":
    main()