#!/usr/bin/env python3
"""
Professional Pedestrian Jobs Australia Scraper using Playwright
================================================================

Advanced Playwright-based scraper for Pedestrian Jobs (jobs.pedestrian.tv) that integrates with 
your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)  
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian creative/media job focus
- Pagination support for comprehensive scraping

Usage:
    python pedestrian_jobs_scraper_advanced.py [job_limit]
    
Examples:
    python pedestrian_jobs_scraper_advanced.py 50    # Scrape 50 jobs
    python pedestrian_jobs_scraper_advanced.py       # Scrape all available jobs
"""

import os
import sys
import django
import time
import random
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, quote_plus
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import requests

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from django.utils import timezone
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

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
    job_url: str
    description: str
    job_type: str
    salary_text: str
    work_mode: str
    external_id: str
    posted_ago: str
    tags: List[str]


class PedestrianJobsScraper:
    """Professional Pedestrian Jobs scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://jobs.pedestrian.tv"
        self.jobs_url = "https://jobs.pedestrian.tv/jobs"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # Job selectors based on actual HTML structure
        self.job_card_selector = ".job-listings-item"
        self.job_link_selector = ".job-details-link"
        self.company_selector = ".job-info-link-item[href*='/companies/']"
        self.location_selector = ".job-info-link-item[href*='/jobs/in-']"
        self.salary_selector = "span:contains('A$'), span:contains('$')"
        self.job_type_selector = ".job-info-link-item[href*='/jobs/'][href*='-time'], .job-info-link-item[href*='/jobs/internship'], .job-info-link-item[href*='/jobs/contract']"
        self.posted_selector = ".job-posted-date"
        self.tags_selector = ".job-tag"
        
    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pedestrian_jobs_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user = User.objects.filter(is_superuser=True).first()
            if user:
                return user
            user = User.objects.filter(username='scraper_bot').first()
            if not user:
                user = User.objects.create_user(
                    username='scraper_bot',
                    email='scraper@jobscraper.com',
                    first_name='Scraper',
                    last_name='Bot'
                )
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return User.objects.first()
    
    def human_sleep(self, min_seconds=1.0, max_seconds=3.0):
        """Sleep for a random human-like duration."""
        sleep_time = random.uniform(min_seconds, max_seconds)
        time.sleep(sleep_time)
    
    def get_or_create_company(self, company_name: str) -> Company:
        """Get or create a company record."""
        if not company_name:
            company_name = "Unknown Company"
        
        # Clean company name
        company_name = re.sub(r'\s+', ' ', company_name.strip())
        company_name = company_name[:200]  # Respect model field limit
        
        company, created = Company.objects.get_or_create(
            name=company_name,
            defaults={
                'description': f'Company profile for {company_name}',
                'website': ''
            }
        )
        return company
    
    def get_or_create_location(self, location_text: str) -> Optional[Location]:
        """Parse and get or create a location record."""
        if not location_text or not location_text.strip():
            self.logger.warning("No location text provided")
            return None
        
        # Clean location text
        location_text = re.sub(r'\s+', ' ', location_text.strip())
        location_text = location_text[:100]  # Respect model field limit
        
        self.logger.info(f"Processing location: '{location_text}'")
        
        # Parse Australian location format
        city = state = ""
        
        # Common patterns: "Sydney, NSW", "Melbourne, Victoria", "Remote (Sydney, NSW)"
        if "Remote" in location_text:
            location_text = re.sub(r'Remote\s*\(([^)]+)\)', r'\1', location_text)
        
        # Split by comma or other separators
        parts = [p.strip() for p in re.split(r'[,\u2023\u25B8\u203A\u25B6\u2794\u00BB]', location_text) if p.strip()]
        
        if parts:
            city = parts[0]
            if len(parts) > 1:
                state = parts[1]
                # Map common abbreviations
                state_mapping = {
                    'NSW': 'New South Wales',
                    'VIC': 'Victoria', 
                    'QLD': 'Queensland',
                    'WA': 'Western Australia',
                    'SA': 'South Australia',
                    'TAS': 'Tasmania',
                    'NT': 'Northern Territory',
                    'ACT': 'Australian Capital Territory'
                }
                state = state_mapping.get(state, state)
        
        try:
            location, created = Location.objects.get_or_create(
                name=location_text,
                defaults={
                    'city': city[:100],
                    'state': state[:100],
                    'country': 'Australia'
                }
            )
            if created:
                self.logger.info(f"Created new location: {location_text}")
            return location
        except Exception as e:
            self.logger.error(f"Error creating location '{location_text}': {e}")
            return None
    
    def parse_salary_text(self, salary_text: str) -> Tuple[Optional[Decimal], Optional[Decimal], str, str]:
        """Parse salary text and extract min, max, currency, and period."""
        if not salary_text:
            return None, None, "AUD", "yearly"
        
        # Clean salary text
        text = salary_text.replace(',', '').replace('$', '').strip()
        
        # Extract numbers
        numbers = [float(n) for n in re.findall(r'\d+(?:\.\d+)?', text)]
        
        # Determine period
        period = "yearly"
        text_lower = text.lower()
        if re.search(r'per\s*hour|\bph\b|hourly|/\s*hr', text_lower):
            period = "hourly"
        elif re.search(r'per\s*day|daily|/\s*day', text_lower):
            period = "daily"
        elif re.search(r'per\s*week|weekly|/\s*week', text_lower):
            period = "weekly"
        elif re.search(r'per\s*month|monthly|/\s*month', text_lower):
            period = "monthly"
        
        # Determine currency (default AUD for Australian site)
        currency = "AUD"
        if re.search(r'\bUSD\b|\$US', text):
            currency = "USD"
        elif re.search(r'\bEUR\b|€', text):
            currency = "EUR"
        elif re.search(r'\bGBP\b|£', text):
            currency = "GBP"
        
        # Extract min and max
        if not numbers:
            return None, None, currency, period
        
        if len(numbers) == 1:
            salary = Decimal(str(numbers[0]))
            return salary, salary, currency, period
        else:
            min_sal = Decimal(str(min(numbers)))
            max_sal = Decimal(str(max(numbers)))
            return min_sal, max_sal, currency, period
    
    def map_job_type(self, job_type_text: str) -> str:
        """Map job type text to standardized values."""
        if not job_type_text:
            return "full_time"
        
        text_lower = job_type_text.lower()
        
        if any(word in text_lower for word in ['part time', 'part-time', 'parttime']):
            return "part_time"
        elif any(word in text_lower for word in ['casual', 'temp', 'temporary']):
            return "casual" if 'casual' in text_lower else "temporary"
        elif any(word in text_lower for word in ['contract', 'contractor']):
            return "contract"
        elif any(word in text_lower for word in ['intern', 'internship']):
            return "internship"
        elif any(word in text_lower for word in ['freelance', 'freelancer']):
            return "freelance"
        else:
            return "full_time"
    
    def detect_work_mode(self, location_text: str, description: str = "") -> str:
        """Detect work mode from location and description."""
        combined_text = f"{location_text} {description}".lower()
        
        if any(word in combined_text for word in ['remote', 'work from home', 'wfh']):
            return "Remote"
        elif any(word in combined_text for word in ['hybrid', 'flexible']):
            return "Hybrid"
        else:
            return "On-site"
    
    def extract_job_details_from_page(self, job_url: str) -> Dict[str, str]:
        """Extract detailed job information from individual job page."""
        try:
            # Use requests for faster detail page fetching
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(job_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract description [[memory:6698010]]
            description = self.extract_job_description(soup)
            
            # Extract structured job details from the job-inner-detail-box
            job_type = ""
            salary = ""
            posted_ago = ""
            location = ""
            company = ""
            
            # Look for the job detail section (.job-inner-detail-box)
            detail_box = soup.find('div', class_='job-inner-detail-box')
            if detail_box:
                self.logger.info(f"DEBUG - Found job detail box for {job_url}")
                
                # Extract all the structured info from the detail box
                info_links = detail_box.find_all('a')
                info_spans = detail_box.find_all('span')
                
                for element in info_links + info_spans:
                    if not element:
                        continue
                        
                    text = element.get_text(strip=True)
                    href = element.get('href', '')
                    
                    self.logger.info(f"DEBUG - Processing element: '{text}' with href: '{href}'")
                    
                    # Extract company (href contains /companies/)
                    if '/companies/' in href and not company:
                        company = text
                        self.logger.info(f"DEBUG - Found company: {company}")
                    
                    # Extract job type (href contains job type patterns or text matches)
                    elif ('/jobs/full-time' in href or '/jobs/part-time' in href or 
                          '/jobs/casual-temp' in href or '/jobs/contract' in href or 
                          '/jobs/internship' in href or 
                          any(jt in text.lower() for jt in ['full-time', 'part-time', 'casual', 'temp', 'contract', 'internship', 'freelance'])):
                        job_type = text
                        self.logger.info(f"DEBUG - Found job type: {job_type}")
                    
                    # Extract location (href contains /jobs/in-)
                    elif '/jobs/in-' in href and not location:
                        location = text
                        self.logger.info(f"DEBUG - Found location: {location}")
                    
                    # Extract salary (text contains A$ or $ with numbers)
                    elif ('A$' in text or '$' in text) and any(c.isdigit() for c in text) and not salary:
                        # Clean up salary text
                        salary = text.strip()
                        self.logger.info(f"DEBUG - Found salary: {salary}")
                    
                    # Extract posted date (text contains "ago")
                    elif 'ago' in text.lower() and not posted_ago:
                        posted_ago = text
                        self.logger.info(f"DEBUG - Found posted date: {posted_ago}")
            
            # Fallback extraction if detail box approach doesn't work
            if not job_type:
                # Look for job type in traditional selectors
                for selector in ['.job-type', '.employment-type', '[class*="type"]', 'a[href*="/jobs/full-time"]', 'a[href*="/jobs/part-time"]', 'a[href*="/jobs/casual"]', 'a[href*="/jobs/contract"]', 'a[href*="/jobs/internship"]']:
                    element = soup.select_one(selector)
                    if element:
                        job_type = element.get_text(strip=True)
                        self.logger.info(f"DEBUG - Fallback job type: {job_type}")
                        break
            
            if not salary:
                # Look for salary in traditional selectors and A$ patterns
                for selector in ['.salary', '.pay', '[class*="salary"]', '[class*="pay"]']:
                    element = soup.select_one(selector)
                    if element:
                        salary = element.get_text(strip=True)
                        self.logger.info(f"DEBUG - Fallback salary: {salary}")
                        break
                
                # Search for A$ patterns in the entire page if still not found
                if not salary:
                    page_text = soup.get_text()
                    salary_match = re.search(r'A\$\s*\d+(?:,\d+)*(?:\.\d+)?\s*(?:-\s*A\$\s*\d+(?:,\d+)*(?:\.\d+)?)?\s*(?:/\s*(?:hour|day|week|month|year))?', page_text)
                    if salary_match:
                        salary = salary_match.group(0)
                        self.logger.info(f"DEBUG - Pattern match salary: {salary}")
            
            if not posted_ago:
                # Look for posting date
                for selector in ['.posted', '.date', '[class*="date"]', '[class*="ago"]']:
                    element = soup.select_one(selector)
                    if element:
                        posted_ago = element.get_text(strip=True)
                        self.logger.info(f"DEBUG - Fallback posted date: {posted_ago}")
                        break
            
            self.logger.info(f"DEBUG - Final extracted data: salary='{salary}', job_type='{job_type}', posted='{posted_ago}'")
            
            return {
                'description': description,
                'job_type': job_type,
                'salary': salary,
                'posted_ago': posted_ago,
                'location': location,
                'company': company
            }
            
        except Exception as e:
            self.logger.warning(f"Error extracting details from {job_url}: {e}")
            return {
                'description': "",
                'job_type': "",
                'salary': "",
                'posted_ago': "",
                'location': "",
                'company': ""
            }
    
    def extract_job_description(self, soup: BeautifulSoup) -> str:
        """Extract clean job description from job page.""" 
        # Primary selector for Pedestrian Jobs - the specific container with job details
        description_text = ""
        
        # First try the specific Pedestrian Jobs selector
        quill_container = soup.select_one('#quill-container-with-job-details.html-content')
        if quill_container:
            # Remove unwanted elements
            for unwanted in quill_container.select('nav, header, footer, .apply, .application, form, script, style'):
                unwanted.decompose()
            
            description_text = quill_container.get_text('\n', strip=True)
        
        # Fallback selectors if the primary one doesn't work
        if not description_text or len(description_text) < 100:
            fallback_selectors = [
                '.html-content',
                '.job-description',
                '.description',
                '.content',
                '.job-content',
                '[class*="description"]',
                'article',
                'main'
            ]
            
            for selector in fallback_selectors:
                element = soup.select_one(selector)
                if element:
                    # Remove unwanted elements
                    for unwanted in element.select('nav, header, footer, .apply, .application, form, script, style'):
                        unwanted.decompose()
                    
                    description_text = element.get_text('\n', strip=True)
                    if len(description_text) > 100:  # Ensure substantial content
                        break
        
        # Clean the description
        if description_text:
            # Remove excessive whitespace
            description_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', description_text)
            description_text = re.sub(r'^\s+|\s+$', '', description_text)
            
            # Remove common non-description content
            lines = description_text.split('\n')
            cleaned_lines = []
            
            for line in lines:
                line = line.strip()
                if not line or len(line) < 3:
                    continue
                
                # Skip navigation and UI elements
                if re.match(r'^(apply|share|save|login|register|home|jobs|back)$', line.lower()):
                    continue
                
                cleaned_lines.append(line)
            
            description_text = '\n'.join(cleaned_lines)
        
        return description_text[:5000]  # Limit length
    
    def scrape_jobs_page(self, page_url: str) -> List[ScrapedJob]:
        """Scrape jobs from a single page."""
        scraped_jobs = []
        
        try:
            self.page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
            self.human_sleep(2, 4)
            
            # Wait for job listings to load
            try:
                self.page.wait_for_selector(self.job_card_selector, timeout=10000)
            except Exception:
                self.logger.warning(f"No job cards found on page: {page_url}")
                return scraped_jobs
            
            # Get all job cards
            job_cards = self.page.locator(self.job_card_selector).all()
            self.logger.info(f"Found {len(job_cards)} job cards on page")
            
            for i, card in enumerate(job_cards):
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    break
                
                try:
                    # Extract basic information from card
                    job_data = self.extract_job_from_card(card)
                    if job_data:
                        scraped_jobs.append(job_data)
                        self.jobs_scraped += 1
                        
                        if self.jobs_scraped % 10 == 0:
                            self.logger.info(f"Scraped {self.jobs_scraped} jobs so far...")
                    
                    self.human_sleep(0.5, 1.5)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping job card {i}: {e}")
                    self.errors_count += 1
                    continue
            
        except Exception as e:
            self.logger.error(f"Error scraping page {page_url}: {e}")
            self.errors_count += 1
        
        return scraped_jobs
    
    def extract_job_from_card(self, card) -> Optional[ScrapedJob]:
        """Extract job information from a job card element."""
        try:
            # Extract job link and title from h3 element
            job_link_elem = card.locator(self.job_link_selector).first
            if not job_link_elem.count():
                return None
            
            job_url = job_link_elem.get_attribute("href")
            if not job_url:
                return None
            
            # Make URL absolute
            if job_url.startswith("/"):
                job_url = self.base_url + job_url
            
            # Extract title from h3 element inside the link
            title_elem = job_link_elem.locator("h3").first
            title = title_elem.text_content(timeout=5000) or "" if title_elem.count() else ""
            title = title.strip()
            
            if not title:
                return None
            
            # Extract company name - look for link with /companies/ in href
            company_name = ""
            company_elem = card.locator(self.company_selector).first
            if company_elem.count():
                company_name = company_elem.text_content(timeout=2000) or ""
            
            # Extract location - look for link with /jobs/in- in href first
            location_text = ""
            location_elem = card.locator(self.location_selector).first
            if location_elem.count():
                location_text = location_elem.text_content(timeout=2000) or ""
            
            # Enhanced location extraction
            if not location_text:
                # First try to get from the full card text using patterns
                try:
                    if 'card_full_text' not in locals():
                        card_full_text = card.text_content(timeout=2000) or ""
                    
                    # Location patterns - more specific to avoid grabbing too much text
                    location_patterns = [
                        r'•\s*([^•\n]*(?:Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Hobart|Darwin)[^•\n]*(?:NSW|VIC|QLD|WA|SA|ACT|TAS|NT|Australia)?[^•\n]*)\s*•',
                        r'•\s*([^•\n]*(?:NSW|VIC|QLD|WA|SA|ACT|TAS|NT)[^•\n]*)\s*•',
                        r'•\s*([^•\n]*Remote[^•\n]*)\s*•',
                        r'\n\s*([^\n]*(?:Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Hobart|Darwin)[^\n]*(?:NSW|VIC|QLD|WA|SA|ACT|TAS|NT|Australia)?[^\n]*)\s*\n',
                        r'(?:^|\n)\s*([^\n]*(?:Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|Hobart|Darwin)[^\n]*(?:NSW|VIC|QLD|WA|SA|ACT|TAS|NT|Australia)?[^\n]*)\s*(?:\n|$)',
                    ]
                    
                    for pattern in location_patterns:
                        match = re.search(pattern, card_full_text, re.IGNORECASE)
                        if match:
                            potential_location = match.group(1).strip()
                            # Validate it's actually a location (not job title, company, etc.)
                            if (any(loc in potential_location for loc in ['Australia', 'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Hobart', 'Darwin', 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT', 'Remote']) and
                                len(potential_location) < 150 and  # Not too long
                                not any(exclude in potential_location.lower() for exclude in ['intern', 'manager', 'coordinator', 'assistant', 'producer', 'designer', 'developer', 'analyst']) and  # Not job titles
                                potential_location.count(' ') < 10):  # Not too many words
                                location_text = potential_location
                                self.logger.info(f"DEBUG - Found location pattern: {location_text}")
                                break
                                
                except Exception as e:
                    self.logger.warning(f"Error extracting location from full text: {e}")
                
                # Fallback: check all job info items for location patterns
                if not location_text:
                    job_info_items = card.locator(".job-info-link-item, span, div").all()
                    for item in job_info_items:
                        text = item.text_content(timeout=1000) or ""
                        # Look for Australian location patterns
                        if any(loc in text for loc in ['Australia', 'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Hobart', 'Darwin', 'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT', 'Remote']):
                            location_text = text.strip()
                            self.logger.info(f"DEBUG - Found location in item: {location_text}")
                            break
            
            # Debug logging
            self.logger.info(f"DEBUG - Job: {title}")
            self.logger.info(f"DEBUG - Company: {company_name}")
            self.logger.info(f"DEBUG - Location raw: '{location_text}'")
            
            # Extract salary - comprehensive approach
            salary_text = ""
            # First try to get all text from the job card for comprehensive parsing
            try:
                card_full_text = card.text_content(timeout=2000) or ""
                self.logger.info(f"DEBUG - Full card text for salary: {card_full_text[:200]}...")
                
                # Look for salary patterns in the full text (excluding $0 values)
                salary_patterns = [
                    r'A\$\s*([1-9]\d{1,2}(?:,\d{3})*(?:\.\d+)?k?)\s*(?:-\s*A\$\s*(\d+(?:,\d+)*(?:\.\d+)?k?))?(?:\s*/\s*year)?',
                    r'\$\s*([1-9]\d{1,2}(?:,\d{3})*(?:\.\d+)?k?)\s*(?:-\s*\$\s*(\d+(?:,\d+)*(?:\.\d+)?k?))?(?:\s*/\s*year)?',
                    r'([1-9]\d{1,2}(?:,\d{3})*(?:\.\d+)?k?)\s*(?:-\s*(\d+(?:,\d+)*(?:\.\d+)?k?))?\s*(?:A\$|\$|AUD)',
                    r'A\$\s*([1-9]\d{1,2}(?:,\d{3})*k?)\s*-\s*A\$\s*(\d+(?:,\d+)*k?)',
                    r'\$([1-9]\d{1,2}(?:,\d{3})*k?)\s*-\s*\$(\d+(?:,\d+)*k?)',
                    r'([1-9]\d{1,2}k?)\s*-\s*(\d+k?)\s*(?:per\s+)?year',
                    r'A\$\s*([1-9]\d{1,2}k?)\s*/\s*year',
                    r'\$([1-9]\d{1,2}k?)\s*/\s*year',
                ]
                
                for pattern in salary_patterns:
                    match = re.search(pattern, card_full_text, re.IGNORECASE)
                    if match:
                        if match.group(2):  # Range salary
                            salary_text = f"A${match.group(1)} - A${match.group(2)} / year"
                        else:  # Single salary
                            salary_text = f"A${match.group(1)} / year"
                        self.logger.info(f"DEBUG - Found salary pattern: {salary_text}")
                        break
                
                # If no pattern match, look for any A$ or $ followed by numbers (exclude $0)
                if not salary_text:
                    simple_salary_match = re.search(r'(A\$\s*[1-9]\d*(?:,\d+)*(?:\.\d+)?k?(?:\s*-\s*A\$\s*\d+(?:,\d+)*(?:\.\d+)?k?)?(?:\s*/\s*year)?)', card_full_text, re.IGNORECASE)
                    if simple_salary_match:
                        salary_text = simple_salary_match.group(1)
                        self.logger.info(f"DEBUG - Found simple salary: {salary_text}")
                
            except Exception as e:
                self.logger.warning(f"Error extracting salary from full text: {e}")
            
            # Fallback: Look for salary in specific elements
            if not salary_text:
                job_info_items = card.locator(".job-info-link-item, span, div").all()
                for item in job_info_items:
                    text = item.text_content(timeout=1000) or ""
                    if "A$" in text or ("$" in text and any(c.isdigit() for c in text)):
                        salary_text = text.strip()
                        self.logger.info(f"DEBUG - Found salary in element: {salary_text}")
                        break
            
            # Extract job type - comprehensive approach
            job_type_text = ""
            # First use the full card text to find job type patterns
            try:
                if 'card_full_text' not in locals():
                    card_full_text = card.text_content(timeout=2000) or ""
                
                self.logger.info(f"DEBUG - Searching for job type in: {card_full_text[:300]}...")
                
                # Comprehensive job type patterns
                job_type_patterns = [
                    r'\b(Full[- ]?time|Part[- ]?time|Casual|Contract|Temporary|Internship|Freelance)\b',
                    r'\b(Full time|Part time)\b',
                    r'\b(FT|PT)\b',  # Abbreviations
                    r'(Full-time|Part-time)',
                    r'(Permanent|Temp)',
                    r'(Internship|Work Experience)',
                ]
                
                for pattern in job_type_patterns:
                    match = re.search(pattern, card_full_text, re.IGNORECASE)
                    if match:
                        job_type_text = match.group(1)
                        self.logger.info(f"DEBUG - Found job type pattern: {job_type_text}")
                        break
                        
            except Exception as e:
                self.logger.warning(f"Error extracting job type from full text: {e}")
            
            # Fallback: Try specific selectors
            if not job_type_text:
                job_type_elem = card.locator(self.job_type_selector).first
                if job_type_elem.count():
                    job_type_text = job_type_elem.text_content(timeout=2000) or ""
                    self.logger.info(f"DEBUG - Found job type in selector: {job_type_text}")
            
            # Second fallback: Search in all job info items
            if not job_type_text:
                job_info_items = card.locator(".job-info-link-item, span, div").all()
                for item in job_info_items:
                    text = item.text_content(timeout=1000) or ""
                    if any(jt in text.lower() for jt in ['full time', 'part time', 'casual', 'contract', 'internship', 'temporary', 'permanent', 'freelance']):
                        job_type_text = text.strip()
                        self.logger.info(f"DEBUG - Found job type in item: {job_type_text}")
                        break
            
            # Extract posted date
            posted_ago = ""
            posted_elem = card.locator(self.posted_selector).first
            if posted_elem.count():
                posted_text = posted_elem.text_content(timeout=2000) or ""
                # Extract just the "2d ago" part
                posted_ago = re.sub(r'.*?(\d+[dhwmy]\s+ago).*', r'\1', posted_text).strip()
            
            # Extract tags
            tags = []
            tags_elems = card.locator(self.tags_selector).all()
            for tag_elem in tags_elems:
                tag_text = tag_elem.text_content(timeout=1000) or ""
                if tag_text.strip():
                    tags.append(tag_text.strip())
            
            # Get detailed information from job page
            job_details = self.extract_job_details_from_page(job_url)
            
            # Extract external ID from URL (e.g., 156355841 from /jobs/156355841-public-relations-intern)
            external_id = ""
            url_match = re.search(r'/jobs/(\d+)', job_url)
            if url_match:
                external_id = url_match.group(1)
            
            # Prioritize job detail page data over card data
            final_job_type = job_details.get('job_type', '').strip() or job_type_text.strip()
            final_salary = job_details.get('salary', '').strip() or salary_text.strip()
            final_posted_ago = job_details.get('posted_ago', '').strip() or posted_ago.strip()
            final_company = job_details.get('company', '').strip() or company_name.strip()
            final_location = job_details.get('location', '').strip() or location_text.strip()
            
            self.logger.info(f"DEBUG - Final data prioritization:")
            self.logger.info(f"  Job Type: '{final_job_type}' (detail: '{job_details.get('job_type', '')}', card: '{job_type_text}')")
            self.logger.info(f"  Salary: '{final_salary}' (detail: '{job_details.get('salary', '')}', card: '{salary_text}')")
            self.logger.info(f"  Company: '{final_company}' (detail: '{job_details.get('company', '')}', card: '{company_name}')")
            self.logger.info(f"  Location: '{final_location}' (detail: '{job_details.get('location', '')}', card: '{location_text}')")
            
            # Create ScrapedJob object
            return ScrapedJob(
                title=title[:200],
                company_name=final_company,
                location_text=final_location,
                job_url=job_url,
                description=job_details.get('description', ''),
                job_type=final_job_type,
                salary_text=final_salary,
                work_mode=self.detect_work_mode(final_location, job_details.get('description', '')),
                external_id=external_id,
                posted_ago=final_posted_ago,
                tags=tags
            )
            
        except Exception as e:
            self.logger.error(f"Error extracting job from card: {e}")
            return None
    
    def save_jobs_to_database(self, scraped_jobs: List[ScrapedJob]):
        """Save scraped jobs to the database."""
        for job_data in scraped_jobs:
            try:
                # Check for duplicates
                if JobPosting.objects.filter(external_url=job_data.job_url).exists():
                    self.duplicates_found += 1
                    continue
                
                # Get or create company and location
                company = self.get_or_create_company(job_data.company_name)
                location = self.get_or_create_location(job_data.location_text)
                
                # Parse salary
                salary_min, salary_max, currency, salary_type = self.parse_salary_text(job_data.salary_text)
                
                # Map job type
                job_type = self.map_job_type(job_data.job_type)
                
                # Get job category
                category = JobCategorizationService.categorize_job(job_data.title, job_data.description)
                
                # Get keywords
                keywords = JobCategorizationService.get_job_keywords(job_data.title, job_data.description)
                tags = list(set(job_data.tags + keywords))  # Combine and deduplicate
                
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
                        work_mode=job_data.work_mode,
                        salary_min=salary_min,
                        salary_max=salary_max,
                        salary_currency=currency,
                        salary_type=salary_type,
                        salary_raw_text=job_data.salary_text[:200],
                        external_source="jobs.pedestrian.tv",
                        external_url=job_data.job_url,
                        external_id=job_data.external_id,
                        posted_ago=job_data.posted_ago,
                        tags=", ".join(tags),
                        additional_info={
                            "source_page": "jobs listing",
                            "scraped_at": timezone.now().isoformat(),
                            "scraper_version": "1.0"
                        }
                    )
                
                self.jobs_saved += 1
                
                if self.jobs_saved % 10 == 0:
                    self.logger.info(f"Saved {self.jobs_saved} jobs to database...")
                    
            except Exception as e:
                self.logger.error(f"Error saving job {job_data.title}: {e}")
                self.errors_count += 1
                continue
    
    def run_scraper(self):
        """Main scraper execution method."""
        self.logger.info("Starting Pedestrian Jobs scraper...")
        self.logger.info(f"Target: {self.jobs_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        start_time = time.time()
        
        try:
            with sync_playwright() as p:
                # Launch browser
                self.browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                
                self.context = self.browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                )
                
                self.page = self.context.new_page()
                self.page.set_default_timeout(30000)
                
                all_scraped_jobs = []
                current_page = 1
                
                # Start scraping from the first page
                page_url = self.jobs_url
                
                while True:
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        break
                    
                    self.logger.info(f"Scraping page {current_page}: {page_url}")
                    
                    # Scrape current page
                    page_jobs = self.scrape_jobs_page(page_url)
                    
                    if not page_jobs:
                        self.logger.info("No more jobs found. Stopping.")
                        break
                    
                    all_scraped_jobs.extend(page_jobs)
                    
                    # Try to find next page
                    next_page_url = self.get_next_page_url()
                    if not next_page_url:
                        self.logger.info("No next page found. Stopping.")
                        break
                    
                    page_url = next_page_url
                    current_page += 1
                    
                    # Human-like delay between pages
                    self.human_sleep(3, 6)
                
                self.browser.close()
            
            # Save all jobs to database (outside of Playwright context)
            self.logger.info(f"Saving {len(all_scraped_jobs)} jobs to database...")
            self.save_jobs_to_database(all_scraped_jobs)
                
        except Exception as e:
            self.logger.error(f"Critical error in scraper: {e}")
            if self.browser:
                self.browser.close()
            raise
        
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        self.logger.info("=" * 60)
        self.logger.info("SCRAPING COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Total jobs scraped: {self.jobs_scraped}")
        self.logger.info(f"New jobs saved: {self.jobs_saved}")
        self.logger.info(f"Duplicates skipped: {self.duplicates_found}")
        self.logger.info(f"Errors encountered: {self.errors_count}")
        self.logger.info(f"Total duration: {duration:.2f} seconds")
        self.logger.info(f"Average time per job: {duration/max(self.jobs_scraped, 1):.2f} seconds")
        self.logger.info("=" * 60)
    
    def get_next_page_url(self) -> Optional[str]:
        """Get the URL for the next page of results."""
        try:
            # Look for pagination container first
            pagination_container = self.page.locator('.jobs-listing-pagination, .pagination').first
            if not pagination_container.count():
                self.logger.info("No pagination container found")
                return None
            
            # Check if "Next" button is disabled (indicates last page)
            next_disabled_elements = pagination_container.locator('li.disabled').all()
            
            # Check if the disabled element is specifically the "Next" button
            is_next_disabled = False
            for disabled_elem in next_disabled_elements:
                elem_text = disabled_elem.text_content() or ""
                elem_html = disabled_elem.inner_html() or ""
                if ("next" in elem_text.lower() or 
                    "next" in elem_html.lower() or 
                    "aria-label" in elem_html and "next" in elem_html.lower() or
                    "rotate(180deg)" in elem_html):  # Next arrow (rotated)
                    is_next_disabled = True
                    break
            
            if is_next_disabled:
                self.logger.info("Next button is disabled - reached last page")
                return None
            
            # Look for next page link that's not disabled
            next_selectors = [
                'a[aria-label*="Next"]',
                'a[rel="next"]',
                'li:not(.disabled) a:has(svg[style*="rotate(180deg)"])',  # Next arrow (rotated left arrow)
                '.pagination li:last-child:not(.disabled) a'
            ]
            
            for selector in next_selectors:
                next_elem = pagination_container.locator(selector).first
                if next_elem.count():
                    href = next_elem.get_attribute("href")
                    if href and href not in ("#", "javascript:void(0)"):
                        if href.startswith("/"):
                            next_url = self.base_url + href
                        else:
                            next_url = href
                        self.logger.info(f"Found next page URL: {next_url}")
                        return next_url
            
            # Fallback: Try to find current page number and increment
            current_url = self.page.url
            
            # Check if we're on page 1 (no page parameter)
            if '?page=' not in current_url and '&page=' not in current_url:
                # First page, try page 2
                next_url = f"{self.jobs_url}?page=2"
                self.logger.info(f"From page 1, trying: {next_url}")
                
                # Test if page 2 actually exists by checking job count
                try:
                    current_job_count = self.page.locator(self.job_card_selector).count()
                    self.logger.info(f"Current page has {current_job_count} jobs")
                    
                    # If current page has fewer than 30 jobs, we might be on the last page
                    if current_job_count < 30:
                        self.logger.info("Current page has fewer than 30 jobs, might be last page")
                        # Still try page 2 to be sure
                        
                except Exception as e:
                    self.logger.warning(f"Error checking job count: {e}")
                
                return next_url
            
            # Extract current page number and increment
            page_match = re.search(r'[?&]page=(\d+)', current_url)
            if page_match:
                current_page = int(page_match.group(1))
                next_page = current_page + 1
                next_url = re.sub(r'page=\d+', f'page={next_page}', current_url)
                self.logger.info(f"From page {current_page}, trying: {next_url}")
                return next_url
            
            self.logger.info("No valid next page found")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding next page: {e}")
            return None


def main():
    """Main function to run the scraper."""
    # Parse command line arguments
    job_limit = None
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            print("Invalid job limit. Using default (no limit).")
    
    # Create and run scraper
    scraper = PedestrianJobsScraper(job_limit=job_limit)
    scraper.run_scraper()


def run(job_limit=100):
    """Automation entrypoint for Pedestrian Jobs scraper."""
    try:
        scraper = PedestrianJobsScraper(job_limit=job_limit)
        scraper.run_scraper()
        return {
            'success': True,
            'message': 'Pedestrian Jobs scraping completed'
        }
    except SystemExit as e:
        return {
            'success': int(getattr(e, 'code', 1)) == 0,
            'exit_code': getattr(e, 'code', 1)
        }
    except Exception as e:
        try:
            logging.getLogger(__name__).error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    main()
