#!/usr/bin/env python3
"""
Professional Jora Australia Job Scraper
========================================

Scrapes job listings from au.jora.com with:
- Enhanced duplicate detection (URL + title+company)
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization
- Human-like behavior to avoid bot detection
- Robust error handling and logging
- Thread-safe database operations

Usage:
    python jora_job_scraper_advanced.py [job_limit]
    
Examples:
    python jora_job_scraper_advanced.py 50    # Scrape 50 jobs
    python jora_job_scraper_advanced.py       # Scrape all jobs (no limit)
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
from django.utils import timezone

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from playwright.sync_api import sync_playwright
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

class JoraJobScraper:
    """Professional Jora Australia job scraper with enhanced duplicate detection."""
    
    def __init__(self, job_category="all", job_limit=None):
        """Initialize the scraper with optional job category and limit."""
        self.base_url = "https://au.jora.com"
        self.search_url = f"{self.base_url}/j"
        self.job_category = job_category
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.duplicate_count = 0
        self.error_count = 0
        self.pages_scraped = 0
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('jora_scraper_professional.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
    
    def human_delay(self, min_seconds=1, max_seconds=3):
        """Add human-like delay between actions."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def scroll_page(self, page):
        """Scroll the page naturally to load content."""
        try:
            # Scroll down gradually
            for i in range(3):
                page.evaluate(f"window.scrollTo(0, document.body.scrollHeight / 3 * {i + 1})")
                self.human_delay(0.5, 1.5)
            
            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            self.human_delay(0.5, 1)
        except Exception as e:
            self.logger.warning(f"Scroll failed: {e}")
    
    def extract_job_data(self, job_card):
        """Extract job data from Jora job card."""
        try:
            job_data = {}
            
            # Job title - Multiple selectors for Jora's layout
            title_selectors = [
                'h3 a',                            # Main job title link
                '.job-title a',                    # Job title class
                '[data-testid="job-title"] a',    # Test ID selector
                'a.job-title',                     # Link with job title class
                '.title a',                        # Generic title
                'h2 a', 'h1 a'                     # Heading links
            ]
            
            job_data['job_title'] = "No title"
            for selector in title_selectors:
                title_element = job_card.query_selector(selector)
                if title_element and title_element.inner_text().strip():
                    job_data['job_title'] = title_element.inner_text().strip()
                    break
            
            # Job URL - Multiple selectors
            url_selectors = [
                'h3 a',                            # Main title link
                '.job-title a',                    # Job title link
                '[data-testid="job-title"] a',    # Test ID link
                'a.job-title',                     # Title class link
                '.title a',                        # Generic title link
                'a[href*="/job/"]'                 # Any job link
            ]
            
            job_data['job_url'] = ""
            for selector in url_selectors:
                link_element = job_card.query_selector(selector)
                if link_element:
                    href = link_element.get_attribute('href')
                    if href:
                        job_data['job_url'] = urljoin(self.base_url, href)
                        break
            
            # Company name - Multiple selectors
            company_selectors = [
                '.company-name',                   # Direct company name class
                '[data-testid="company-name"]',   # Test ID company
                '.company a',                      # Company link
                '.employer',                       # Employer class
                'h3 + div',                        # Div after title
                '.job-company',                    # Job company class
                'p.company'                        # Paragraph company
            ]
            
            job_data['company_name'] = "Unknown Company"
            for selector in company_selectors:
                company_element = job_card.query_selector(selector)
                if company_element and company_element.inner_text().strip():
                    job_data['company_name'] = company_element.inner_text().strip()
                    break
            
            # Location - Multiple selectors for Jora
            location_selectors = [
                '.location',                       # Direct location class
                '[data-testid="job-location"]',   # Test ID location
                '.job-location',                   # Job location class
                '.locality',                       # Locality class
                'h3 + div + div',                  # Second div after title
                '.place',                          # Place class
                'span.location'                    # Span location
            ]
            
            job_data['location_text'] = ""
            for selector in location_selectors:
                location_element = job_card.query_selector(selector)
                if location_element and location_element.inner_text().strip():
                    job_data['location_text'] = location_element.inner_text().strip()
                    break
            
            # Job summary/snippet
            summary_selectors = [
                '.job-snippet',                    # Job snippet class
                '.description',                    # Description class
                '.job-description',                # Job description
                'p.snippet',                       # Paragraph snippet
                '.summary',                        # Summary class
                '.excerpt'                         # Excerpt class
            ]
            
            job_data['summary'] = ""
            for selector in summary_selectors:
                summary_element = job_card.query_selector(selector)
                if summary_element and summary_element.inner_text().strip():
                    job_data['summary'] = summary_element.inner_text().strip()
                    break
            
            # Salary information - Updated for Jora's current structure
            salary_selectors = [
                '.salary-info',                    # Jora salary info class
                '.job-salary',                     # Job salary class
                '[data-sal]',                      # Data salary attribute
                '.salary-range',                   # Salary range
                '.pay-rate',                       # Pay rate
                '.wage-info',                      # Wage info
                '.compensation-info',              # Compensation info
                'span[class*="salary"]',           # Any span with salary in class
                'div[class*="salary"]',            # Any div with salary in class
                '.job-meta .salary',               # Salary in job meta
                '.job-details .salary',            # Salary in job details
                'p:contains("$")',                 # Any paragraph containing $
                'span:contains("$")',              # Any span containing $
                'div:contains("$")'                # Any div containing $
            ]
            
            job_data['salary_text'] = ""
            
            # First try direct salary selectors
            for selector in salary_selectors:
                try:
                    salary_element = job_card.query_selector(selector)
                    if salary_element and salary_element.inner_text().strip():
                        salary_text = salary_element.inner_text().strip()
                        if '$' in salary_text:  # Only take text that contains dollar signs
                            job_data['salary_text'] = salary_text
                            break
                except:
                    continue
            
            # If no salary found, look for any text containing $ symbols in the job card
            if not job_data['salary_text']:
                try:
                    all_text = job_card.inner_text()
                    import re
                    # Look for salary patterns like $500, $25/hour, $50,000, etc.
                    salary_patterns = re.findall(r'\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*(?:per|/)\s*(?:hour|week|month|year|annum))?', all_text, re.IGNORECASE)
                    if salary_patterns:
                        job_data['salary_text'] = salary_patterns[0]
                except:
                    pass
            
            # Date posted (Jora uses relative dates like "2 days ago")
            date_selectors = [
                '.date',                           # Date class
                '[data-testid="date"]',           # Test ID date
                '.posted',                         # Posted class
                '.time',                           # Time class
                '.published'                       # Published class
            ]
            
            date_text = ""
            for selector in date_selectors:
                date_element = job_card.query_selector(selector)
                if date_element and date_element.inner_text().strip():
                    date_text = date_element.inner_text().strip()
                    break
            
            job_data['posted_ago'] = date_text
            job_data['date_posted'] = self.parse_relative_date(date_text)
            
            # Job type (full-time, part-time, etc.) - Enhanced for Jora
            type_selectors = [
                '.job-type',                       # Job type class
                '[data-testid="job-type"]',       # Test ID job type
                '.employment-type',                # Employment type
                '.work-type',                      # Work type
                '.contract-type',                  # Contract type
                '.employment-info',                # Employment info
                '.job-details .type',              # Type in job details
                '.job-meta .type',                 # Type in job meta
                'span[class*="type"]',             # Any span with type in class
                'div[class*="employment"]',        # Any div with employment in class
                '.type'                            # Generic type
            ]
            
            job_data['job_type_text'] = ""
            
            # First try direct type selectors
            for selector in type_selectors:
                try:
                    type_element = job_card.query_selector(selector)
                    if type_element and type_element.inner_text().strip():
                        job_data['job_type_text'] = type_element.inner_text().strip()
                        break
                except:
                    continue
            
            # If no job type found, search in the full job card text for employment keywords
            if not job_data['job_type_text']:
                try:
                    all_text = job_card.inner_text().lower()
                    
                    # Look for job type keywords in the text
                    if any(word in all_text for word in ['part-time', 'part time', 'casual']):
                        job_data['job_type_text'] = 'Part-time'
                    elif any(word in all_text for word in ['contract', 'contractor', 'temporary', 'temp']):
                        job_data['job_type_text'] = 'Contract'
                    elif any(word in all_text for word in ['internship', 'intern', 'trainee']):
                        job_data['job_type_text'] = 'Internship'
                    elif any(word in all_text for word in ['freelance', 'freelancer']):
                        job_data['job_type_text'] = 'Freelance'
                    elif any(word in all_text for word in ['full-time', 'full time', 'permanent']):
                        job_data['job_type_text'] = 'Full-time'
                except:
                    pass
            
            # Remote work indicator
            remote_indicators = ['.remote', '.work-from-home', '.wfh', '[data-remote="true"]']
            job_data['remote_work'] = ""
            for selector in remote_indicators:
                remote_element = job_card.query_selector(selector)
                if remote_element:
                    job_data['remote_work'] = "Remote"
                    break
            
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def extract_full_job_description(self, page, job_url):
        """Visit individual job page to extract full description."""
        try:
            if not job_url:
                return ""
            
            self.logger.info(f"Fetching full description from: {job_url[:100]}...")
            
            # Navigate to the individual job page
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(2, 4)
            
            # Target the specific job description container for Jora
            description_selectors = [
                '#job-description-container',           # Primary target - Jora's specific job description container
                '.job-description',                     # Fallback - Main job description
                '.description',                         # Fallback - Description class
                '.job-content',                         # Fallback - Job content
                '[data-testid="job-description"]',      # Fallback - Test ID description
                '.job-posting-description',             # Fallback - Job posting description
            ]
            
            full_description = ""
            
            # Try each selector to find the job description
            for selector in description_selectors:
                try:
                    description_element = page.query_selector(selector)
                    if description_element:
                        text = description_element.inner_text().strip()
                        if text and len(text) > 50:  # Ensure we get substantial content
                            full_description = text
                            if selector == '#job-description-container':
                                self.logger.info(f"Found SPECIFIC job-description-container ({len(text)} chars)")
                            else:
                                self.logger.info(f"Found description using fallback selector: {selector} ({len(text)} chars)")
                            break
                except Exception as e:
                    continue
            
            # If no specific selector worked, try to get all text content from main areas
            if not full_description:
                try:
                    # Get text from main content areas
                    main_content = page.query_selector('main, .main, #main, .container, .wrapper')
                    if main_content:
                        full_description = main_content.inner_text().strip()
                        self.logger.info(f"Extracted description from main content area ({len(full_description)} chars)")
                except Exception as e:
                    pass
            
            # Final fallback - get all visible text and filter for job-related content
            if not full_description:
                try:
                    all_text = page.inner_text('body')
                    # Basic filtering to get job-related content
                    lines = all_text.split('\n')
                    job_lines = []
                    for line in lines:
                        line = line.strip()
                        # Skip navigation, footer, and other non-job content
                        if line and len(line) > 20 and not any(skip_word in line.lower() for skip_word in 
                            ['cookie', 'privacy', 'terms', 'navigation', 'menu', 'footer', 'header', 
                             'subscribe', 'newsletter', 'social', 'follow us', 'contact us']):
                            job_lines.append(line)
                    
                    if job_lines:
                        full_description = '\n'.join(job_lines[:50])  # Take first 50 relevant lines
                        self.logger.info(f"Extracted filtered description ({len(full_description)} chars)")
                except Exception as e:
                    pass
            
            return full_description
            
        except Exception as e:
            self.logger.error(f"Error extracting full job description from {job_url}: {e}")
            return ""
    
    def parse_relative_date(self, date_text):
        """Parse relative date strings like 'Posted 3 days ago' into timezone-aware dates."""
        try:
            if not date_text:
                return timezone.now()
            
            date_text = date_text.lower()
            now = timezone.now()
            today = now.date()
            
            # Handle "today" or "just posted"
            if 'today' in date_text or 'just posted' in date_text:
                return now.replace(hour=9, minute=0, second=0, microsecond=0)  # Assume 9 AM posting
            
            # Handle "yesterday"
            if 'yesterday' in date_text:
                yesterday = now - timedelta(days=1)
                return yesterday.replace(hour=9, minute=0, second=0, microsecond=0)
            
            # Extract number from "X days ago", "X hours ago", etc.
            numbers = re.findall(r'\d+', date_text)
            if numbers:
                number = int(numbers[0])
                
                if 'hour' in date_text:
                    return now - timedelta(hours=number)
                elif 'day' in date_text:
                    past_date = now - timedelta(days=number)
                    return past_date.replace(hour=9, minute=0, second=0, microsecond=0)
                elif 'week' in date_text:
                    past_date = now - timedelta(weeks=number)
                    return past_date.replace(hour=9, minute=0, second=0, microsecond=0)
                elif 'month' in date_text:
                    past_date = now - timedelta(days=number * 30)
                    return past_date.replace(hour=9, minute=0, second=0, microsecond=0)
            
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
            
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_text}': {e}")
            return timezone.now()
    
    def parse_location(self, location_string):
        """Parse location string into city, state, country."""
        if not location_string:
            return "", "", "", "Australia"
        
        location_string = location_string.strip()
        city = ""
        state = ""
        country = "Australia"
        
        # Common Australian state mappings
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
        
        # Split by common separators
        parts = [p.strip() for p in re.split(r'[,\-\|]', location_string) if p.strip()]
        
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
        min_salary = None
        max_salary = None
        currency = "AUD"
        period = "yearly"
        
        try:
            # Remove currency symbols and clean text
            clean_text = re.sub(r'[^\d\s\-–,\.ka-z]', ' ', salary_text.lower())
            
            # Extract salary numbers (handle k for thousands)
            numbers = re.findall(r'\d+(?:\.\d+)?(?:k)?', clean_text)
            
            if numbers:
                # Convert 'k' notation to actual numbers
                parsed_numbers = []
                for num in numbers:
                    if 'k' in num:
                        parsed_numbers.append(int(float(num.replace('k', '')) * 1000))
                    else:
                        parsed_numbers.append(int(float(num)))
                
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
                    self.logger.info(f"Duplicate job skipped (URL): {job_title} at {company_name}")
                    self.duplicate_count += 1
                    return False
                
                # Check 2: Title + Company duplicate (semantic duplicate)
                if JobPosting.objects.filter(
                    title=job_title, 
                    company__name=company_name
                ).exists():
                    self.logger.info(f"Duplicate job skipped (Title+Company): {job_title} at {company_name}")
                    self.duplicate_count += 1
                    return False
                
                # Parse and get or create location
                location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
                location_obj = None
                if location_name:
                    location_obj, created = Location.objects.get_or_create(
                        name=location_name,
                        defaults={
                            'city': city if city else '',
                            'state': state if state else '',
                            'country': country if country else 'Australia'
                        }
                    )
                
                # Get or create company
                company_obj, created = Company.objects.get_or_create(
                    name=company_name,
                    defaults={
                        'slug': re.sub(r'[^a-zA-Z0-9\-_]', '-', company_name.lower())
                    }
                )
                
                # Parse salary
                min_salary, max_salary, currency, period, salary_display = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Determine job type - Enhanced logic
                job_type = 'full_time'  # Default
                job_type_text = job_data.get('job_type_text', '').lower()
                job_title_lower = job_title.lower()
                summary_lower = job_data.get('summary', '').lower()
                
                # Check job type text first
                if any(keyword in job_type_text for keyword in ['part-time', 'part time', 'casual']):
                    job_type = 'part_time'
                elif any(keyword in job_type_text for keyword in ['contract', 'contractor', 'temporary', 'temp']):
                    job_type = 'contract'
                elif any(keyword in job_type_text for keyword in ['internship', 'intern', 'trainee']):
                    job_type = 'internship'
                elif any(keyword in job_type_text for keyword in ['freelance', 'freelancer']):
                    job_type = 'freelance'
                # Also check job title and summary for type indicators
                elif any(keyword in job_title_lower for keyword in ['part-time', 'part time', 'casual']):
                    job_type = 'part_time'
                elif any(keyword in job_title_lower for keyword in ['contract', 'contractor']):
                    job_type = 'contract'
                elif any(keyword in summary_lower for keyword in ['part-time', 'part time', 'casual']):
                    job_type = 'part_time'
                
                # Determine work mode
                work_mode = 'onsite'  # Default
                if job_data.get('remote_work') or 'remote' in job_data.get('summary', '').lower():
                    work_mode = 'remote'
                elif 'hybrid' in job_data.get('summary', '').lower():
                    work_mode = 'hybrid'
                
                # Categorize job
                category = self.categorization_service.categorize_job(
                    job_title, 
                    job_data.get('summary', '')
                )
                
                # Get or create a system user for scraped jobs
                from django.contrib.auth import get_user_model
                User = get_user_model()
                scraper_user, created = User.objects.get_or_create(
                    username='jora_scraper',
                    defaults={
                        'email': 'scraper@jora.local',
                        'is_active': False  # System user, not for login
                    }
                )
                
                # Create job posting - full description preserved, only specific fields truncated for DB constraints
                job_posting = JobPosting.objects.create(
                    title=job_title[:200] if len(job_title) > 200 else job_title,  # CharField(200) - smart truncation
                    company=company_obj,
                    location=location_obj,
                    posted_by=scraper_user,
                    description=job_data.get('full_description', job_data.get('summary', '')),  # TextField - full description preserved!
                    external_url=job_url[:200] if len(job_url) > 200 else job_url,  # URLField(200) - smart truncation
                    external_source='jora_au',  # This is short, no truncation needed
                    job_category=category if category else 'other',
                    job_type=job_type if job_type else 'full_time',
                    work_mode=work_mode if work_mode else '',
                    salary_min=min_salary,
                    salary_max=max_salary,
                    salary_currency=currency if currency else 'AUD',
                    salary_type=period if period else 'yearly',
                    salary_raw_text=salary_display[:200] if salary_display and len(salary_display) > 200 else (salary_display or ''),
                    posted_ago=job_data.get('posted_ago', '')[:50] if len(job_data.get('posted_ago', '')) > 50 else job_data.get('posted_ago', ''),
                    date_posted=job_data.get('date_posted'),
                    status='active'
                )
                
                self.logger.info(f"Saved job: {job_title} at {company_name}")
                self.logger.info(f"  Category: {category}")
                self.logger.info(f"  Location: {location_name}")
                
                # Display salary info
                if min_salary and max_salary and min_salary == max_salary:
                    salary_info = f"{currency} {min_salary:,} per {period}"
                elif min_salary and max_salary:
                    salary_info = f"{currency} {min_salary:,} - {max_salary:,} per {period}"
                elif salary_display:
                    salary_info = salary_display
                else:
                    salary_info = "Salary not specified"
                
                self.logger.info(f"  Salary: {salary_info}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving job to database: {e}")
            self.logger.error(f"Job data: title='{job_title[:100]}...'")
            self.logger.error(f"  company='{company_name[:100]}...'")
            self.logger.error(f"  url='{job_url[:100]}...'")
            if job_data.get('full_description'):
                self.logger.error(f"  description length: {len(job_data.get('full_description', ''))} characters")
            
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
    
    def debug_page_structure(self, page):
        """Debug function to identify actual page structure."""
        try:
            self.logger.info("=== DEBUGGING PAGE STRUCTURE ===")
            
            # Get page title and URL
            title = page.title()
            url = page.url
            self.logger.info(f"Page Title: {title}")
            self.logger.info(f"Current URL: {url}")
            
            # Check for common job-related elements
            debug_script = """
                () => {
                    const results = {
                        allClasses: [],
                        jobElements: [],
                        dataAttributes: [],
                        textContent: document.body.innerText.substring(0, 500)
                    };
                    
                    // Get all unique classes
                    const allElements = document.querySelectorAll('*');
                    const classSet = new Set();
                    
                    allElements.forEach(el => {
                        if (el.className && typeof el.className === 'string') {
                            el.className.split(' ').forEach(cls => {
                                if (cls.trim() && (cls.includes('job') || cls.includes('result') || cls.includes('card') || cls.includes('listing'))) {
                                    classSet.add(cls.trim());
                                }
                            });
                        }
                        
                        // Check for data attributes
                        Array.from(el.attributes).forEach(attr => {
                            if (attr.name.startsWith('data-') && (attr.name.includes('job') || attr.name.includes('result'))) {
                                results.dataAttributes.push(attr.name);
                            }
                        });
                    });
                    
                    results.allClasses = Array.from(classSet);
                    
                    // Look for potential job containers
                    const jobSelectors = ['[class*="job"]', '[class*="result"]', '[class*="card"]', '[class*="listing"]', 'article'];
                    jobSelectors.forEach(selector => {
                        const elements = document.querySelectorAll(selector);
                        if (elements.length > 0) {
                            results.jobElements.push({
                                selector: selector,
                                count: elements.length,
                                sample: elements[0] ? elements[0].outerHTML.substring(0, 200) : ''
                            });
                        }
                    });
                    
                    return results;
                }
            """
            
            debug_results = page.evaluate(debug_script)
            
            self.logger.info(f"Job-related classes found: {debug_results['allClasses'][:20]}")
            self.logger.info(f"Data attributes found: {list(set(debug_results['dataAttributes']))[:10]}")
            self.logger.info(f"Page text preview: {debug_results['textContent'][:200]}...")
            
            for job_element in debug_results['jobElements']:
                self.logger.info(f"Selector '{job_element['selector']}': {job_element['count']} elements")
                if job_element['sample']:
                    self.logger.info(f"  Sample HTML: {job_element['sample']}...")
            
            self.logger.info("=== END DEBUG ===")
            
        except Exception as e:
            self.logger.error(f"Debug failed: {e}")

    def scrape_jobs_from_page(self, page):
        """Scrape all jobs from the current page."""
        jobs_found = 0
        jobs_processed = 0  # Track total jobs processed (including duplicates)
        
        try:
            # Debug page structure first
            self.debug_page_structure(page)
            
            # Wait for Jora's job results to load
            job_cards = []
            
            # First wait for the page to fully load
            try:
                page.wait_for_selector('.job, .job-card, .result, .listing', timeout=15000)
                self.human_delay(2, 4)
            except:
                self.logger.warning("Standard selectors not found, trying alternative approach...")
                pass
            
            # Enhanced Jora job card selectors (2024 update)
            selectors_to_try = [
                '[data-testid="job-card"]',        # Modern test ID selector
                '.job',                            # Main job container
                '.job-card',                       # Job card container
                '.result',                         # Result container
                '.listing',                        # Job listing
                '.job-item',                       # Job item
                'article',                         # Article elements
                '.search-result',                  # Search result
                '[class*="job"]',                  # Any class containing "job"
                '[class*="result"]',               # Any class containing "result"
                '[data-job-id]',                   # Elements with job ID data attribute
                '.card',                           # Generic card selector
                '.item'                            # Generic item selector
            ]
            
            for selector in selectors_to_try:
                try:
                    page.wait_for_selector(selector, timeout=3000)
                    potential_cards = page.query_selector_all(selector)
                    if potential_cards and len(potential_cards) > 2:  # Need at least 3 for valid results
                        job_cards = potential_cards
                        self.logger.info(f"Found {len(job_cards)} job cards using selector: {selector}")
                        break
                except:
                    continue
            
            # Enhanced fallback: look for job title links
            if not job_cards:
                try:
                    page.wait_for_selector('h3 a, .job-title a, a[href*="/job/"]', timeout=5000)
                    job_links = page.query_selector_all('h3 a, .job-title a, a[href*="/job/"]')
                    if job_links and len(job_links) > 2:
                        job_cards = job_links
                        self.logger.info(f"Using job title links as fallback: found {len(job_cards)}")
                except:
                    pass
            
            self.logger.info(f"Found {len(job_cards)} job listings on current page")
            
            for i, job_card in enumerate(job_cards):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                        return jobs_found, True, jobs_processed  # Signal to stop
                    
                    # Extract job data
                    job_data = self.extract_job_data(job_card)
                    
                    if job_data and job_data.get('job_title') and job_data.get('job_url'):
                        jobs_processed += 1  # Count all valid job cards processed
                        
                        # Extract full job description from individual job page using new context
                        try:
                            # Create new page context for job detail to avoid context conflicts
                            context = page.context
                            detail_page = context.new_page()
                            
                            full_description = self.extract_full_job_description(detail_page, job_data['job_url'])
                            job_data['full_description'] = full_description
                            self.logger.info(f"Extracted description: {len(full_description)} characters")
                            
                            # Close the detail page to free resources
                            detail_page.close()
                        except Exception as e:
                            self.logger.error(f"Failed to extract full description: {e}")
                            job_data['full_description'] = job_data.get('summary', '')
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            self.jobs_scraped += 1
                            jobs_found += 1
                        
                        # Add delay between job processing
                        self.human_delay(1, 3)  # Increased delay due to additional page visits
                    
                except Exception as e:
                    self.logger.error(f"Error processing job card {i}: {e}")
                    self.error_count += 1
                    continue
            
            return jobs_found, False, jobs_processed
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from page: {e}")
            return 0, False, 0
    
    def debug_pagination_structure(self, page):
        """Debug function to identify actual pagination structure."""
        try:
            self.logger.info("=== DEBUGGING PAGINATION STRUCTURE ===")
            
            # Check for pagination-related elements
            debug_script = """
                () => {
                    const results = {
                        paginationClasses: [],
                        paginationElements: [],
                        nextButtons: [],
                        pageNumbers: [],
                        allLinks: []
                    };
                    
                    // Get all unique pagination-related classes
                    const allElements = document.querySelectorAll('*');
                    const classSet = new Set();
                    
                    allElements.forEach(el => {
                        if (el.className && typeof el.className === 'string') {
                            el.className.split(' ').forEach(cls => {
                                if (cls.trim() && (cls.includes('page') || cls.includes('next') || cls.includes('prev') || cls.includes('pagination'))) {
                                    classSet.add(cls.trim());
                                }
                            });
                        }
                    });
                    
                    results.paginationClasses = Array.from(classSet);
                    
                    // Look for pagination containers
                    const paginationSelectors = ['[class*="page"]', '[class*="pagination"]', 'nav', '.pagination', '.pager'];
                    paginationSelectors.forEach(selector => {
                        const elements = document.querySelectorAll(selector);
                        if (elements.length > 0) {
                            results.paginationElements.push({
                                selector: selector,
                                count: elements.length,
                                sample: elements[0] ? elements[0].outerHTML.substring(0, 300) : ''
                            });
                        }
                    });
                    
                    // Look for next buttons specifically
                    const nextSelectors = [
                        'a[aria-label*="Next"]', 'a[aria-label*="next"]',
                        'button[aria-label*="Next"]', 'button[aria-label*="next"]',
                        '.next', 'a.next', 'button.next',
                        '[rel="next"]', 'a[rel="next"]',
                        'a:contains("Next")', 'button:contains("Next")',
                        'a:contains(">")', 'button:contains(">")'
                    ];
                    
                    nextSelectors.forEach(selector => {
                        try {
                            const elements = document.querySelectorAll(selector);
                            if (elements.length > 0) {
                                results.nextButtons.push({
                                    selector: selector,
                                    count: elements.length,
                                    sample: elements[0] ? elements[0].outerHTML.substring(0, 200) : '',
                                    disabled: elements[0] ? elements[0].disabled || elements[0].getAttribute('disabled') : false,
                                    href: elements[0] ? elements[0].href : null
                                });
                            }
                        } catch(e) {
                            // Skip selectors that cause errors
                        }
                    });
                    
                    // Look for page numbers
                    const pageElements = document.querySelectorAll('a[href*="page"], a[href*="p="], button[data-page]');
                    results.pageNumbers = Array.from(pageElements).slice(0, 5).map(el => ({
                        text: el.innerText,
                        href: el.href,
                        html: el.outerHTML.substring(0, 150)
                    }));
                    
                    // Get all links that might be pagination
                    const allLinks = document.querySelectorAll('a');
                    results.allLinks = Array.from(allLinks).filter(link => 
                        link.href && (
                            link.href.includes('page') || 
                            link.href.includes('p=') ||
                            link.innerText.match(/^\d+$/) ||
                            link.innerText.toLowerCase().includes('next') ||
                            link.innerText.includes('>')
                        )
                    ).slice(0, 10).map(link => ({
                        text: link.innerText.trim(),
                        href: link.href,
                        html: link.outerHTML.substring(0, 150)
                    }));
                    
                    return results;
                }
            """
            
            debug_results = page.evaluate(debug_script)
            
            self.logger.info(f"Pagination classes found: {debug_results['paginationClasses']}")
            
            for pagination_element in debug_results['paginationElements']:
                self.logger.info(f"Pagination element '{pagination_element['selector']}': {pagination_element['count']} elements")
                if pagination_element['sample']:
                    self.logger.info(f"  Sample HTML: {pagination_element['sample']}...")
            
            for next_button in debug_results['nextButtons']:
                self.logger.info(f"Next button '{next_button['selector']}': {next_button['count']} elements")
                self.logger.info(f"  Disabled: {next_button['disabled']}, Href: {next_button['href']}")
                if next_button['sample']:
                    self.logger.info(f"  Sample HTML: {next_button['sample']}...")
            
            self.logger.info(f"Page number elements found: {len(debug_results['pageNumbers'])}")
            for page_num in debug_results['pageNumbers']:
                self.logger.info(f"  Page: '{page_num['text']}' -> {page_num['href']}")
            
            self.logger.info(f"All pagination-related links found: {len(debug_results['allLinks'])}")
            for link in debug_results['allLinks']:
                self.logger.info(f"  Link: '{link['text']}' -> {link['href']}")
            
            self.logger.info("=== END PAGINATION DEBUG ===")
            
        except Exception as e:
            self.logger.error(f"Pagination debug failed: {e}")

    def go_to_next_page(self, page):
        """Navigate to the next page of results."""
        try:
            # Debug pagination structure first
            self.debug_pagination_structure(page)
            
            # Enhanced Jora pagination selectors based on debug findings (2024 update)
            next_selectors = [
                # Mobile pagination container (found in debug)
                '.mobile-pagination a:has-text("Next")',
                '.pagination-container a:has-text("Next")',
                '.multi-pages-pagination .next-page-button',
                
                # Standard selectors
                'a[aria-label="Next"], a[aria-label="Next Page"]',
                'a[aria-label="next"], a[aria-label="next page"]',
                'button[aria-label="Next"], button[aria-label="Next Page"]',
                'button[aria-label="next"], button[aria-label="next page"]',
                
                # Class-based selectors
                '.next-page-button',
                '.next',
                '.pagination-next',
                'a.next',
                'button.next',
                '.pager .next',
                
                # Attribute-based selectors
                'a[rel="next"]',
                'a[rel="nofollow"]:has-text("Next")',  # Found in debug output
                
                # Text-based selectors
                'a:has-text("Next")',
                'button:has-text("Next")',
                'a:has-text(">")',
                'button:has-text(">")',
                
                # Data attribute selectors
                '[data-testid="next-page"]',
                '[data-testid="pagination-next"]',
                
                # URL-based selectors (direct links to next page)
                'a[href*="&p=2"]',    # Page parameter in URL
                'a[href*="page=2"]',  # Direct link to page 2 if on page 1
                'a[href*="p=2"]'      # Alternative page parameter
            ]
            
            next_button = None
            working_selector = None
            
            for selector in next_selectors:
                try:
                    next_button = page.query_selector(selector)
                    if next_button:
                        # Check if button is enabled
                        is_disabled = next_button.get_attribute('disabled')
                        
                        # More comprehensive visibility check
                        is_visible = page.evaluate('''(element) => {
                            const rect = element.getBoundingClientRect();
                            const style = window.getComputedStyle(element);
                            return rect.width > 0 && 
                                   rect.height > 0 && 
                                   style.display !== 'none' && 
                                   style.visibility !== 'hidden' && 
                                   style.opacity !== '0';
                        }''', next_button)
                        
                        if not is_disabled and is_visible:
                            working_selector = selector
                            self.logger.info(f"Found working next button with selector: {selector}")
                            break
                        else:
                            self.logger.info(f"Found next button with selector '{selector}' but it's disabled ({is_disabled}) or not visible ({is_visible})")
                except Exception as e:
                    continue
            
            if next_button and working_selector:
                self.logger.info(f"Clicking next page using selector: {working_selector}")
                
                # Get current URL for comparison
                current_url = page.url
                self.logger.info(f"Current URL: {current_url}")
                
                try:
                    # Try different click approaches
                    
                    # Approach 1: Direct click without scrolling (for mobile pagination)
                    try:
                        next_button.click(timeout=5000)
                        self.logger.info("Clicked next button directly")
                    except:
                        # Approach 2: Force click if element is not in viewport
                        try:
                            next_button.click(force=True, timeout=5000)
                            self.logger.info("Force clicked next button")
                        except:
                            # Approach 3: JavaScript click
                            page.evaluate('(element) => element.click()', next_button)
                            self.logger.info("JavaScript clicked next button")
                    
                    # Wait for navigation
                    page.wait_for_load_state('domcontentloaded', timeout=30000)
                    self.human_delay(2, 4)
                    
                    # Verify we moved to a new page
                    new_url = page.url
                    self.logger.info(f"New URL: {new_url}")
                    
                    if new_url != current_url:
                        self.logger.info("Successfully navigated to next page!")
                        return True
                    else:
                        self.logger.warning("URL didn't change after clicking next button")
                        return False
                        
                except Exception as click_error:
                    self.logger.error(f"Failed to click next button: {click_error}")
                    return False
                
            else:
                self.logger.info("No next page available or next button disabled")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False
    
    def run(self):
        """Main scraping method."""
        print("🔍 Professional Jora Australia Job Scraper")
        print("=" * 50)
        print(f"Target: {self.job_limit or 'All'} jobs from all categories")
        print("Database: Professional structure with JobPosting, Company, Location")
        print("=" * 50)
        
        self.logger.info("Starting Professional Jora Australia job scraper...")
        self.logger.info(f"Target URL: {self.search_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        with sync_playwright() as p:
            # Launch browser with enhanced stealth settings to bypass Cloudflare
            browser = p.chromium.launch(
                headless=False,  # Visible browser for debugging
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
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-ipc-flooding-protection',
                    '--disable-default-apps',
                    '--disable-sync',
                    '--disable-translate',
                    '--hide-scrollbars',
                    '--metrics-recording-only',
                    '--mute-audio',
                    '--no-default-browser-check',
                    '--no-pings',
                    '--password-store=basic',
                    '--use-mock-keychain',
                    '--disable-component-extensions-with-background-pages',
                    '--disable-background-networking',
                    '--disable-component-update',
                    '--disable-client-side-phishing-detection',
                    '--disable-hang-monitor',
                    '--disable-popup-blocking',
                    '--disable-prompt-on-repost',
                    '--disable-domain-reliability',
                    '--disable-features=AudioServiceOutOfProcess',
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                ]
            )
            
            # Create context with stealth settings
            context = browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1'
                }
            )
            
            # Add enhanced stealth scripts to bypass Cloudflare detection
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
                if (window.navigator.permissions) {
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: 'granted' }) :
                            originalQuery(parameters)
                    );
                }
                
                // Hide automation indicators
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32',
                });
                
                // Mock screen properties
                Object.defineProperty(screen, 'colorDepth', {
                    get: () => 24,
                });
                
                // Remove automation-related properties
                delete navigator.__proto__.webdriver;
                
                // Mock connection
                Object.defineProperty(navigator, 'connection', {
                    get: () => ({
                        effectiveType: '4g',
                        rtt: 50,
                        downlink: 10
                    }),
                });
            """)
            
            page = context.new_page()
            
            try:
                # Navigate to Jora Australia with enhanced Cloudflare bypass
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        self.logger.info(f"Navigating to Jora Australia (attempt {attempt + 1})...")
                        
                        # Use Jora's job search URL
                        search_url = "https://au.jora.com/j?q=&l=Australia"
                        
                        # Navigate with longer timeout for Cloudflare challenges
                        page.goto(search_url, wait_until='domcontentloaded', timeout=90000)
                        
                        # Check for Cloudflare challenge
                        cloudflare_indicators = [
                            'Just a moment...',
                            'Checking your browser',
                            'Please wait while we check your browser',
                            'cf-browser-verification',
                            'cf-challenge-running'
                        ]
                        
                        page_content = page.content()
                        is_cloudflare_challenge = any(indicator in page_content for indicator in cloudflare_indicators)
                        
                        if is_cloudflare_challenge:
                            self.logger.info("Cloudflare challenge detected, waiting for completion...")
                            
                            # Wait for Cloudflare challenge to complete (up to 30 seconds)
                            for wait_time in range(30):
                                self.human_delay(1, 1.5)
                                current_content = page.content()
                                
                                # Check if challenge is completed
                                if not any(indicator in current_content for indicator in cloudflare_indicators):
                                    self.logger.info("Cloudflare challenge completed!")
                                    break
                                    
                                # Check for job-related content
                                if any(keyword in current_content.lower() for keyword in ['job', 'search', 'results']):
                                    self.logger.info("Job content detected, challenge likely passed!")
                                    break
                            else:
                                self.logger.warning("Cloudflare challenge timeout, retrying...")
                                continue
                        
                        # Additional wait for page to fully load
                        self.human_delay(3, 5)
                        
                        # Check if we successfully reached the job search page
                        final_content = page.content()
                        if len(final_content) < 1000:  # Too short, likely still blocked
                            raise Exception("Page content too short, likely still blocked")
                        
                        # Try to close cookie banner if it exists
                        try:
                            cookie_selectors = [
                                'button[id*="cookie"]',
                                'button[id*="accept"]', 
                                '.cookie-accept',
                                '[data-testid="cookie-accept"]',
                                '.gdpr-accept',
                                '#accept-cookies'
                            ]
                            
                            for selector in cookie_selectors:
                                cookie_button = page.query_selector(selector)
                                if cookie_button:
                                    cookie_button.click()
                                    self.human_delay(1, 2)
                                    break
                        except:
                            pass
                        
                        self.logger.info(f"Successfully loaded page on attempt {attempt + 1}")
                        break
                        
                    except Exception as e:
                        self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        if attempt == max_retries - 1:
                            raise
                        
                        # Exponential backoff with randomization
                        wait_time = (2 ** attempt) + random.uniform(1, 3)
                        self.logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                        time.sleep(wait_time)
                
                # Start scraping
                page_number = 1
                consecutive_pages_no_new_jobs = 0  # Safety counter
                max_consecutive_pages = 3  # Stop after 3 consecutive pages with no new jobs
                
                while True:
                    self.logger.info(f"Scraping page {page_number}...")
                    
                    # Scroll page to load all content
                    self.scroll_page(page)
                    
                    # Scrape jobs from current page
                    jobs_found, should_stop, jobs_processed = self.scrape_jobs_from_page(page)
                    
                    if should_stop:
                        self.logger.info("Job limit reached, stopping scraping.")
                        break
                    
                    # Improved logic: Check if we found any job listings at all (not just new ones)
                    if jobs_processed == 0:
                        self.logger.info(f"No job listings found on page {page_number}, ending scraping.")
                        break
                    elif jobs_found == 0:
                        consecutive_pages_no_new_jobs += 1
                        self.logger.info(f"All {jobs_processed} jobs on page {page_number} were duplicates, continuing to next page... ({consecutive_pages_no_new_jobs}/{max_consecutive_pages})")
                        
                        # Safety check: stop if too many consecutive pages with no new jobs
                        if consecutive_pages_no_new_jobs >= max_consecutive_pages:
                            self.logger.info(f"Stopping after {max_consecutive_pages} consecutive pages with no new jobs.")
                            break
                    else:
                        consecutive_pages_no_new_jobs = 0  # Reset counter when we find new jobs
                        self.logger.info(f"Found {jobs_found} new jobs out of {jobs_processed} total jobs on page {page_number}")
                    
                    # Try to go to next page
                    if not self.go_to_next_page(page):
                        self.logger.info("No more pages available.")
                        break
                    
                    page_number += 1
                    self.pages_scraped = page_number
                    
                    # Safety limit for pages
                    if page_number > 50:
                        self.logger.info("Reached maximum page limit (50).")
                        break
                
            except Exception as e:
                self.logger.error(f"Scraping failed: {e}")
                self.error_count += 1
            
            finally:
                browser.close()
        
        # Final statistics with thread-safe database call
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: JobPosting.objects.filter(external_source='jora_au').count())
                total_jobs_in_db = future.result(timeout=10)
        except Exception as e:
            self.logger.error(f"Error getting final job count: {e}")
            total_jobs_in_db = "Unknown"
        
        # Print final results
        self.logger.info("=" * 50)
        self.logger.info("PROFESSIONAL SCRAPING COMPLETED!")
        self.logger.info(f"Total pages scraped: {self.pages_scraped}")
        self.logger.info(f"Total jobs found: {self.jobs_scraped}")
        self.logger.info(f"Jobs saved to database: {self.jobs_scraped}")
        self.logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
        self.logger.info(f"Errors encountered: {self.error_count}")
        self.logger.info(f"Total Jora jobs in database: {total_jobs_in_db}")
        self.logger.info("=" * 50)

def main():
    """Main entry point."""
    job_limit = None
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            print("Usage: python jora_job_scraper_advanced.py [job_limit]")
            print("job_limit must be a number")
            sys.exit(1)
    
    scraper = JoraJobScraper(job_limit=job_limit)
    scraper.run()

if __name__ == "__main__":
    main()