#!/usr/bin/env python3
"""
Professional Mission Australia Workday Job Scraper
=================================================

Scrapes job listings from Mission Australia's Workday-powered careers portal with:
- Enhanced duplicate detection (URL + title+company)
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization
- Human-like behavior to avoid bot detection
- Robust error handling and logging
- Thread-safe database operations
- Adaptive scraping for Workday job portal

Mission Australia uses Workday's talent acquisition platform at:
https://missionaustralia.wd3.myworkdayjobs.com/MissionAustralia

Features:
- Workday-specific job card detection and extraction
- Company information extraction (Mission Australia)
- Australian location parsing
- Salary and job type classification
- Professional error handling and retry logic
- Human-like browsing patterns

Usage:
    python missionaustralia_workday_scraper.py [job_limit]
    
Examples:
    python missionaustralia_workday_scraper.py 50    # Scrape 50 jobs
    python missionaustralia_workday_scraper.py       # Scrape all jobs (no limit)
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
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from playwright.sync_api import sync_playwright
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService


class MissionAustraliaWorkdayJobScraper:
    """Professional Mission Australia Workday job scraper with enhanced duplicate detection."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://missionaustralia.wd3.myworkdayjobs.com"
        self.search_url = f"{self.base_url}/MissionAustralia"
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
                logging.FileHandler('missionaustralia_workday_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        # User agents for rotation (Workday sites prefer standard browsers)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
        ]
        
        # Mission Australia company information
        self.company_name = "Mission Australia"
        self.company_description = "Mission Australia is a national Christian charity that has been reducing homelessness and strengthening communities across Australia for over 160 years."
    
    def human_delay(self, min_seconds=2, max_seconds=5):
        """Add human-like delay between actions (longer for corporate sites)."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def clean_job_description(self, description_text):
        """Clean and format job description text."""
        if not description_text:
            return ""
        
        # Remove common unwanted prefixes and headers
        unwanted_prefixes = [
            'Job description', 'Job Description', 'JOB DESCRIPTION',
            'Description:', 'DESCRIPTION:', 'Role description', 'Role Description',
            'ROLE DESCRIPTION', 'Position description', 'Position Description',
            'POSITION DESCRIPTION', 'About the role:', 'About the Role:',
            'ABOUT THE ROLE:', 'Job Summary:', 'JOB SUMMARY:', 'Summary:', 'SUMMARY:'
        ]
        
        # Remove unwanted prefixes and HTML artifacts
        cleaned_text = description_text.strip()
        
        # Remove leading numbers followed by "Job description" pattern
        cleaned_text = re.sub(r'^\d+\s*Job description\s*', '', cleaned_text, flags=re.IGNORECASE)
        cleaned_text = re.sub(r'^\d+\s*Description\s*', '', cleaned_text, flags=re.IGNORECASE)
        cleaned_text = re.sub(r'^\d+\s*Role description\s*', '', cleaned_text, flags=re.IGNORECASE)
        cleaned_text = re.sub(r'^\d+\s*Position description\s*', '', cleaned_text, flags=re.IGNORECASE)
        
        # Remove other unwanted prefixes
        for prefix in unwanted_prefixes:
            if cleaned_text.startswith(prefix):
                cleaned_text = cleaned_text[len(prefix):].strip()
                if cleaned_text.startswith(':') or cleaned_text.startswith('-'):
                    cleaned_text = cleaned_text[1:].strip()
                break
        
        # Remove leading single digits or numbers that appear at start
        cleaned_text = re.sub(r'^\d+\s*', '', cleaned_text).strip()
        
        # Cut off content after "Our culture" section
        cutoff_patterns = [
            r'Our culture.*',  # Remove everything from "Our culture" onwards
            r'If you live with disability.*',  # Remove disability support text
            r'We strongly encourage applications.*',  # Remove Aboriginal/Torres Strait Islander text
            r'Find out more about a career.*',  # Remove career info
            r'How to apply.*',  # Remove application instructions
            r'Click \'Apply\'.*',  # Remove apply button text
            r'As a committed.*Circle Back Initiative.*',  # Remove circle back text
            r'Applications are shortlisted.*'  # Remove shortlisting text
        ]
        
        # Apply cutoff patterns
        for pattern in cutoff_patterns:
            match = re.search(pattern, cleaned_text, flags=re.IGNORECASE | re.DOTALL)
            if match:
                cleaned_text = cleaned_text[:match.start()].strip()
                break
        
        # Remove HTML-like artifacts and unwanted text patterns
        unwanted_patterns = [
            r'^\s*0\s*',  # Leading zero
            r'^\s*\d+\s*$',  # Just a number on its own line
            r'aria-hidden="true".*?(?=\w)',  # aria-hidden attributes
            r'class="[^"]*"',  # class attributes
            r'span.*?(?=\w)',  # span tags
            r'div.*?(?=\w)',  # div tags
        ]
        
        for pattern in unwanted_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE | re.MULTILINE)
        
        # Remove extra whitespace but preserve list formatting and structure
        lines = []
        for line in cleaned_text.split('\n'):
            line = line.strip()
            if line:
                lines.append(line)
        
        cleaned_text = '\n'.join(lines)
        
        # Remove leading and trailing whitespace
        cleaned_text = cleaned_text.strip()
        
        return cleaned_text
    
    def scroll_page(self, page):
        """Scroll the page naturally to load content."""
        try:
            # Scroll down gradually
            for i in range(4):
                page.evaluate(f"window.scrollTo(0, document.body.scrollHeight / 4 * {i + 1})")
                self.human_delay(1, 2)
            
            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            self.human_delay(1, 2)
        except Exception as e:
            self.logger.warning(f"Scroll failed: {e}")
    
    def extract_full_job_description(self, job_url, page):
        """
        Visit individual job page to extract the complete job description and salary
        Returns: dict with 'description' and 'salary_info' keys
        """
        try:
            self.logger.debug(f"Visiting job detail page: {job_url}")
            
            # Navigate to the job detail page
            page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(2, 4)
            
            # Wait for content to load
            try:
                page.wait_for_load_state('networkidle', timeout=5000)
            except:
                pass
            
            # Try multiple selectors to find the job description - Based on actual HTML
            description_selectors = [
                # Mission Australia specific selector (from provided HTML)
                'div[data-automation-id="jobPostingDescription"]',
                '[data-automation-id="jobPostingDescription"]',
                # Also try to get the main content area that includes the structured info
                'main .content',
                '[role="main"]',
                # Workday-specific selectors
                '[data-automation-id="job-description"]',
                '.jobDescriptionText',
                '.jobDescription',
                '[data-automation-id="richTextEditor"]',
                # Generic job description selectors
                '.job-description',
                '.job-details', 
                '.description',
                '.job-content',
                '.content',
                '.job-detail-content',
                '.job-summary',
                '.position-details',
                '.vacancy-details',
                '#job-description',
                '#description',
                # Workday container selectors
                '.wd-tab-content',
                '.wd-job-content',
                '[data-automation-id*="description"]',
                '[data-automation-id*="content"]'
            ]
            
            full_description = ""
            raw_html_content = ""
            
            for selector in description_selectors:
                try:
                    description_element = page.query_selector(selector)
                    if description_element:
                        # Get both text and HTML content for better salary extraction
                        desc_text = description_element.inner_text().strip()
                        raw_html_content = description_element.inner_html()
                        desc_text = self.clean_job_description(desc_text)
                        
                        if desc_text and len(desc_text) > 50:  # Must have substantial content
                            full_description = desc_text
                            self.logger.debug(f"Found full description using selector: {selector}")
                            break
                except:
                    continue
            
            # If no specific description selector found, try to extract from page text
            if not full_description:
                try:
                    # Get all text from main content area
                    main_content = page.query_selector('main') or page.query_selector('body')
                    if main_content:
                        page_text = main_content.inner_text()
                        
                        # Look for description patterns in the text
                        lines = page_text.split('\n')
                        description_lines = []
                        collecting = False
                        
                        for line in lines:
                            line = line.strip()
                            
                            # Start collecting after certain keywords
                            if any(keyword in line.lower() for keyword in [
                                'about the role', 'job description', 'role description',
                                'responsibilities', 'key responsibilities', 'duties',
                                'what you will do', 'what you\'ll do', 'position overview',
                                'about this position', 'the role', 'about mission australia'
                            ]):
                                collecting = True
                                description_lines.append(line)
                                continue
                            
                            # Stop collecting at certain keywords
                            if collecting and any(keyword in line.lower() for keyword in [
                                'apply now', 'how to apply', 'application process',
                                'contact us', 'share this job', 'save this job',
                                'application deadline', 'closing date', 'apply online'
                            ]):
                                break
                            
                            # Collect lines while we're collecting
                            if collecting and line:
                                description_lines.append(line)
                        
                        if description_lines:
                            raw_description = '\n'.join(description_lines)
                            full_description = self.clean_job_description(raw_description)
                            self.logger.debug("Extracted description from page text analysis")
                        
                        # If still no description, get substantial content from page
                        if not full_description:
                            # Look for content blocks with substantial text
                            content_blocks = []
                            for line in lines:
                                line = line.strip()
                                if (line and len(line) > 20 and  # Substantial content
                                    not any(skip in line.lower() for skip in [
                                        'skip to', 'navigation', 'search', 'apply now',
                                        'footer', 'header', 'menu', 'button', 'cookie'
                                    ])):
                                    content_blocks.append(line)
                            
                            if content_blocks and len(content_blocks) > 3:
                                # Take the main content as description
                                full_description = '\n'.join(content_blocks[:10])  # First 10 substantial lines
                                self.logger.debug("Using main page content as description")
                
                except Exception as e:
                    self.logger.debug(f"Error extracting description from page text: {e}")
            
            # Extract salary information from both text and HTML content
            salary_info = self.extract_salary_from_description(full_description)
            
            # If no salary found in cleaned text, try from raw HTML
            if not salary_info and raw_html_content:
                salary_info = self.extract_salary_from_description(raw_html_content)
            
            # Return description and salary
            result = {
                'description': full_description if full_description else None,
                'salary_info': salary_info
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error extracting full job description from {job_url}: {e}")
            return {'description': None, 'salary_info': None}

    def extract_salary_from_description(self, description_text):
        """Dynamically extract salary information from job description text."""
        if not description_text:
            return None
        
        try:
            # Step 1: Find lines that contain salary indicators
            salary_indicators = ['salary', 'remuneration', 'package', 'compensation', 'pay']
            salary_lines = []
            
            # Split text into lines and search
            lines = description_text.split('\n')
            for line in lines:
                line_clean = line.strip()
                if any(indicator in line_clean.lower() for indicator in salary_indicators):
                    # Check if line contains dollar signs or money amounts
                    if '$' in line_clean or re.search(r'\d+[kK]', line_clean):
                        salary_lines.append(line_clean)
            
            # Step 2: Extract complete salary information from identified lines
            for line in salary_lines:
                # Clean HTML tags if present
                line = re.sub(r'<[^>]*>', '', line).strip()
                
                # Dynamic pattern to capture everything after "Salary:" or similar
                salary_match = None
                
                # Look for "Salary:" followed by content
                salary_pattern = r'[Ss]alary\s*[:]\s*(.+?)(?:\n|$|\.(?:\s|$))'
                match = re.search(salary_pattern, line, re.IGNORECASE)
                if match:
                    salary_match = match.group(1).strip()
                
                # If no "Salary:" found, look for dollar amounts with context
                if not salary_match:
                    # Look for dollar amounts and capture surrounding context
                    dollar_pattern = r'(\$[\d,]+[KkMm]?(?:\+|\.|\s)*(?:[^\.]*?)(?:super|packaging|benefits|annum|hour|year|pa)?[^\.]*)'
                    match = re.search(dollar_pattern, line, re.IGNORECASE)
                    if match:
                        potential_salary = match.group(1).strip()
                        # Validate it's a reasonable salary context
                        if len(potential_salary) > 3 and any(word in potential_salary.lower() for word in ['super', 'package', 'packaging', '+', 'k', '$']):
                            salary_match = potential_salary
                
                # Step 3: Post-process and validate the match
                if salary_match:
                    # Remove leading/trailing punctuation
                    salary_match = re.sub(r'^[^\w$]+|[^\w]+$', '', salary_match).strip()
                    
                    # Ensure it contains monetary information
                    if '$' in salary_match or re.search(r'\d+[kK]', salary_match):
                        # Limit length to reasonable salary description
                        if len(salary_match) > 200:
                            # Take first reasonable chunk
                            sentences = re.split(r'[\.!;]', salary_match)
                            salary_match = sentences[0].strip() if sentences else salary_match[:200]
                        
                        self.logger.debug(f"Dynamically found salary: {salary_match}")
                        return salary_match
            
            # Step 4: Fallback - look for any dollar amounts in the text
            fallback_patterns = [
                r'\$[\d,]+[KkMm]?\+?\s*(?:super|package|packaging|benefits|per\s*(?:hour|annum|year))?',
                r'\$[\d,]+\s*[-–]\s*\$[\d,]+',
                r'\$[\d,]+'
            ]
            
            for pattern in fallback_patterns:
                matches = re.findall(pattern, description_text, re.IGNORECASE)
                if matches:
                    # Return the longest/most detailed match
                    best_match = max(matches, key=len)
                    if len(best_match) > 3:
                        self.logger.debug(f"Fallback salary found: {best_match}")
                        return best_match
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error extracting salary from description: {e}")
            return None

    def extract_job_data(self, job_card):
        """Extract job data from Workday job card."""
        try:
            job_data = {}
            
            # Job title - Based on actual HTML structure
            title_selectors = [
                'a[data-automation-id="jobTitle"]',  # Exact match from provided HTML
                'h3 a',  # Most common pattern in Workday
                '[data-automation-id*="title"] a',
                '.jobTitle a',
                '.job-title a',
                'a[href*="/job/"]',
                'a[href*="/jobs/"]',
                'h2 a', 'h1 a',
                '.position-title a',
                '.vacancy-title a'
            ]
            
            job_data['job_title'] = ""
            for selector in title_selectors:
                title_element = job_card.query_selector(selector)
                if title_element and title_element.inner_text().strip():
                    job_data['job_title'] = title_element.inner_text().strip()
                    break
            
            # Job URL - Extract from job title link or card link
            url_selectors = [
                'a[data-automation-id="jobTitle"]',  # Exact match from provided HTML
                'h3 a',
                'a[href*="/job/"]',
                'a[href*="/jobs/"]',
                '.jobTitle a',
                '.job-title a',
                'a[data-automation-id*="title"]'
            ]
            
            job_data['job_url'] = ""
            for selector in url_selectors:
                link_element = job_card.query_selector(selector)
                if link_element:
                    href = link_element.get_attribute('href')
                    if href:
                        job_data['job_url'] = urljoin(self.base_url, href)
                        break
            
            # Company name - Always Mission Australia for this scraper
            job_data['company_name'] = self.company_name
            
            # Location - Based on actual HTML structure
            location_selectors = [
                'div[data-automation-id="locations"] dd',  # Exact match from provided HTML
                '[data-automation-id="locations"]',
                'dd.css-129m7dg',  # From the provided HTML structure
                '[data-automation-id="location"]',
                '[data-automation-id*="location"]',
                '.jobLocation',
                '.job-location',
                '.location',
                '.workplace-location',
                'dd[data-automation-id="location"]',
                'span[data-automation-id="location"]',
                '.wd-facet-location'
            ]
            
            job_data['location_text'] = ""
            for selector in location_selectors:
                location_element = job_card.query_selector(selector)
                if location_element and location_element.inner_text().strip():
                    location_text = location_element.inner_text().strip()
                    if location_text:
                        job_data['location_text'] = location_text
                        break
            
            # If no location found, try pattern matching in card text
            if not job_data.get('location_text'):
                try:
                    card_text = job_card.inner_text()
                    lines = card_text.split('\n')
                    
                    # Look for Australian location patterns
                    aus_states = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']
                    aus_cities = ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 
                                 'Canberra', 'Darwin', 'Hobart', 'Gold Coast', 'Newcastle',
                                 'Wollongong', 'Geelong', 'Townsville', 'Cairns']
                    
                    for line in lines:
                        line = line.strip()
                        if any(state in line for state in aus_states) or any(city in line for city in aus_cities):
                            if line and len(line) < 100:  # Reasonable location length
                                job_data['location_text'] = line
                                break
                except:
                    pass
            
            # Job summary/snippet - Extract from job card
            summary_selectors = [
                '[data-automation-id="jobSummary"]',
                '[data-automation-id*="summary"]',
                '.jobSummary',
                '.job-snippet',
                '.description',
                '.job-description',
                '.summary',
                '.excerpt',
                '.job-summary'
            ]
            
            job_data['summary'] = ""
            for selector in summary_selectors:
                summary_element = job_card.query_selector(selector)
                if summary_element and summary_element.inner_text().strip():
                    summary_text = summary_element.inner_text().strip()
                    summary_text = self.clean_job_description(summary_text)
                    if summary_text:
                        job_data['summary'] = summary_text
                        break
            
            # If no summary found using selectors, extract from job card text
            if not job_data.get('summary'):
                try:
                    card_text = job_card.inner_text()
                    lines = card_text.split('\n')
                    
                    # Look for the main description paragraph
                    for i, line in enumerate(lines):
                        line = line.strip()
                        # Skip short lines, titles, etc.
                        if (line and len(line) > 30 and  # Substantial content
                            not line.isdigit() and
                            line != job_data.get('job_title', '') and
                            line != job_data.get('location_text', '')):
                            
                            job_data['summary'] = line
                            break
                except:
                    pass
            
            # Salary information - Workday patterns
            salary_selectors = [
                '[data-automation-id="salary"]',
                '[data-automation-id*="salary"]',
                '.salary',
                '.pay-rate',
                '.remuneration',
                '.package',
                '.salary-range',
                '.compensation'
            ]
            
            job_data['salary_text'] = ""
            for selector in salary_selectors:
                salary_element = job_card.query_selector(selector)
                if salary_element and salary_element.inner_text().strip():
                    salary_text = salary_element.inner_text().strip()
                    if any(char in salary_text for char in ['$', 'AUD', 'salary', 'per']):
                        job_data['salary_text'] = salary_text
                        break
            
            # If no salary found, look for salary patterns in card text
            if not job_data['salary_text']:
                try:
                    all_text = job_card.inner_text()
                    lines = all_text.split('\n')
                    
                    for line in lines:
                        line = line.strip()
                        # Look for various salary patterns
                        if any(pattern in line.lower() for pattern in ['$', 'salary', 'per hour', 'per annum']):
                            salary_patterns = re.findall(
                                r'(\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*per\s*\w+)?)',
                                line,
                                re.IGNORECASE
                            )
                            if salary_patterns:
                                job_data['salary_text'] = salary_patterns[0]
                                break
                except:
                    pass
            
            # Date posted - Based on actual HTML structure
            date_selectors = [
                'div[data-automation-id="postedOn"] dd',  # Exact match from provided HTML
                '[data-automation-id="postedOn"]',
                'dd.css-129m7dg',  # From the provided HTML structure
                '[data-automation-id*="date"]',
                '.posted-date',
                '.date',
                '.published',
                '.date-posted'
            ]
            
            date_text = ""
            for selector in date_selectors:
                date_element = job_card.query_selector(selector)
                if date_element and date_element.inner_text().strip():
                    date_text = date_element.inner_text().strip()
                    break
            
            job_data['posted_ago'] = date_text
            job_data['date_posted'] = self.parse_relative_date(date_text)
            
            # Job type - Based on actual HTML structure
            type_selectors = [
                'div[data-automation-id="time"] dd',  # Exact match from provided HTML
                '[data-automation-id="time"]',
                'dd.css-129m7dg',  # From the provided HTML structure
                '[data-automation-id="jobType"]',
                '[data-automation-id*="type"]',
                '.job-type',
                '.employment-type',
                '.work-type'
            ]
            
            job_data['job_type_text'] = ""
            for selector in type_selectors:
                type_element = job_card.query_selector(selector)
                if type_element and type_element.inner_text().strip():
                    job_data['job_type_text'] = type_element.inner_text().strip()
                    break
            
            # If no job type found, search in text
            if not job_data['job_type_text']:
                try:
                    all_text = job_card.inner_text().lower()
                    
                    if any(word in all_text for word in ['part-time', 'part time', 'casual']):
                        job_data['job_type_text'] = 'Part-time'
                    elif any(word in all_text for word in ['contract', 'contractor', 'temporary']):
                        job_data['job_type_text'] = 'Contract'
                    elif any(word in all_text for word in ['permanent', 'ongoing']):
                        job_data['job_type_text'] = 'Permanent'
                    elif any(word in all_text for word in ['full-time', 'full time']):
                        job_data['job_type_text'] = 'Full-time'
                except:
                    pass
            
            # Remote work indicator
            job_data['remote_work'] = ""
            try:
                all_text = job_card.inner_text().lower()
                if any(word in all_text for word in ['remote', 'work from home', 'telecommute']):
                    job_data['remote_work'] = "Remote"
                elif 'hybrid' in all_text:
                    job_data['remote_work'] = "Hybrid"
            except:
                pass
            
            self.logger.debug(f"Extracted job data: Title='{job_data.get('job_title')}', URL='{job_data.get('job_url')}'")
            
            return job_data
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def parse_relative_date(self, date_text):
        """Parse relative date strings and formal dates."""
        try:
            from django.utils import timezone as django_timezone
            if not date_text:
                return django_timezone.now().date()
            
            date_text = date_text.lower().strip()
            today = django_timezone.now().date()
            
            # Handle "today" or "just posted"
            if 'today' in date_text or 'just posted' in date_text:
                return today
            
            # Handle "yesterday"
            if 'yesterday' in date_text:
                return today - timedelta(days=1)
            
            # Handle formal dates (DD/MM/YYYY, DD-MM-YYYY, etc.)
            date_patterns = [
                r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})',
                r'(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_text)
                if match:
                    try:
                        if len(match.group(1)) == 4:  # YYYY-MM-DD format
                            year, month, day = match.groups()
                        else:  # DD-MM-YYYY format
                            day, month, year = match.groups()
                        return datetime(int(year), int(month), int(day)).date()
                    except:
                        continue
            
            # Extract number from "X days ago", "X hours ago", etc.
            numbers = re.findall(r'\d+', date_text)
            if numbers:
                number = int(numbers[0])
                
                if 'hour' in date_text:
                    return today
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
        """Parse salary information."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
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
                # Validate required data before saving
                job_url = job_data['job_url']
                job_title = job_data['job_title']
                company_name = job_data['company_name']
                
                # Enhanced duplicate detection: Check both URL and title+company
                
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
                            'city': city,
                            'state': state,
                            'country': country
                        }
                    )
                
                # Get or create company (Mission Australia)
                company_obj, created = Company.objects.get_or_create(
                    name=self.company_name,
                    defaults={
                        'slug': 'mission-australia',
                        'description': self.company_description,
                        'website': 'https://www.missionaustralia.com.au/',
                        'company_size': 'large'
                    }
                )
                
                # Parse salary
                min_salary, max_salary, currency, period, salary_display = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Determine job type
                job_type = 'full_time'  # Default
                job_type_text = job_data.get('job_type_text', '').lower()
                job_title_lower = job_title.lower()
                summary_lower = job_data.get('summary', '').lower()
                
                # Check job type text first
                if any(keyword in job_type_text for keyword in ['part-time', 'part time', 'casual']):
                    job_type = 'part_time'
                elif any(keyword in job_type_text for keyword in ['contract', 'contractor', 'temporary']):
                    job_type = 'contract'
                elif any(keyword in job_type_text for keyword in ['permanent', 'ongoing']):
                    job_type = 'full_time'
                # Also check job title and summary for type indicators
                elif any(keyword in job_title_lower for keyword in ['part-time', 'part time', 'casual']):
                    job_type = 'part_time'
                elif any(keyword in job_title_lower for keyword in ['contract', 'contractor']):
                    job_type = 'contract'
                elif any(keyword in summary_lower for keyword in ['part-time', 'part time', 'casual']):
                    job_type = 'part_time'
                
                # Determine work mode
                work_mode = 'onsite'  # Default
                if job_data.get('remote_work'):
                    if 'hybrid' in job_data.get('remote_work', '').lower():
                        work_mode = 'hybrid'
                    else:
                        work_mode = 'remote'
                elif any(word in job_data.get('summary', '').lower() for word in ['hybrid', 'flexible work']):
                    work_mode = 'hybrid'
                elif any(word in job_data.get('summary', '').lower() for word in ['remote', 'work from home']):
                    work_mode = 'remote'
                
                # Categorize job using AI-like categorization service
                category = self.categorization_service.categorize_job(
                    job_title, 
                    job_data.get('summary', '')
                )
                
                # Get or create a system user for scraped jobs
                from django.contrib.auth import get_user_model
                User = get_user_model()
                scraper_user, created = User.objects.get_or_create(
                    username='mission_australia_scraper',
                    defaults={
                        'email': 'scraper@missionaustralia.local',
                        'is_active': False  # System user, not for login
                    }
                )
                
                # Create unique slug
                from django.utils.text import slugify
                base_slug = slugify(job_title)
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Prepare additional_info without date objects (for JSON serialization)
                additional_info = dict(job_data)
                # Convert date objects to strings for JSON compatibility
                if 'date_posted' in additional_info and hasattr(additional_info['date_posted'], 'isoformat'):
                    additional_info['date_posted'] = additional_info['date_posted'].isoformat()
                
                # Make date_posted timezone-aware
                from django.utils import timezone as django_timezone
                date_posted = job_data.get('date_posted')
                if date_posted:
                    if hasattr(date_posted, 'year') and not hasattr(date_posted, 'hour'):
                        # It's a date object, convert to datetime at start of day
                        date_posted = django_timezone.make_aware(
                            datetime.combine(date_posted, datetime.min.time())
                        )
                    elif hasattr(date_posted, 'tzinfo'):
                        # It's a datetime object, make it timezone-aware if needed
                        if date_posted.tzinfo is None:
                            date_posted = django_timezone.make_aware(date_posted)
                    else:
                        # Default to current timezone-aware datetime
                        date_posted = django_timezone.now()
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_title,
                    slug=unique_slug,
                    company=company_obj,
                    location=location_obj,
                    posted_by=scraper_user,
                    description=job_data.get('summary', ''),
                    external_url=job_url,
                    external_source='mission_australia_workday',
                    job_category=category,
                    job_type=job_type,
                    work_mode=work_mode,
                    salary_min=min_salary,
                    salary_max=max_salary,
                    salary_currency=currency,
                    salary_type=period,
                    salary_raw_text=salary_display,
                    posted_ago=job_data.get('posted_ago', ''),
                    date_posted=date_posted,
                    status='active',
                    additional_info=additional_info  # Store all extracted data with JSON-serializable dates
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
    
    def handle_workday_loading(self, page):
        """Handle Workday-specific loading and interface elements."""
        try:
            # Wait for Workday to load
            self.logger.info("Waiting for Workday interface to load...")
            
            # Wait for jobs to load (Workday uses dynamic loading)
            loading_selectors = [
                '[data-automation-id="loadingIndicator"]',
                '.loading',
                '.wd-spinner',
                '[aria-label="Loading"]'
            ]
            
            # Wait for loading indicators to disappear
            for selector in loading_selectors:
                try:
                    # Wait for element to appear then disappear
                    page.wait_for_selector(selector, timeout=5000)
                    page.wait_for_selector(selector, state='hidden', timeout=15000)
                    self.logger.info(f"Loading indicator {selector} finished")
                    break
                except:
                    continue
            
            # Additional wait for content to stabilize
            self.human_delay(3, 5)
            
            # Handle cookie consent if present
            try:
                cookie_selectors = [
                    'button:has-text("Accept")',
                    'button:has-text("I agree")',
                    'button:has-text("OK")',
                    '[data-automation-id="cookieAccept"]',
                    '.cookie-accept'
                ]
                for selector in cookie_selectors:
                    cookie_button = page.query_selector(selector)
                    if cookie_button and cookie_button.is_visible():
                        self.logger.info("Accepting cookies...")
                        cookie_button.click()
                        self.human_delay(1, 2)
                        break
            except:
                pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling Workday loading: {e}")
            return False
    
    def navigate_to_next_page(self, page):
        """Navigate to the next page using pagination controls."""
        try:
            self.logger.info("Attempting to navigate to next page...")
            
            # Wait for pagination controls to be ready
            self.human_delay(2, 4)
            
            # Try to find the next button using the HTML structure provided
            next_button_selectors = [
                'button[aria-label="next"]',  # Exact match from provided HTML
                'button[data-uxi-element-id="next"]',
                'button[data-uxi-widget-type="stepToNextButton"]',
                '.css-1ujhc41',  # Class from provided HTML
                'nav[aria-label="pagination"] button:has(svg)',  # Button with arrow icon
                'button:has(.wd-icon-chevron-right-small)',  # Button with right chevron
            ]
            
            next_button = None
            for selector in next_button_selectors:
                try:
                    next_button = page.query_selector(selector)
                    if next_button and next_button.is_visible():
                        self.logger.debug(f"Found next button using selector: {selector}")
                        break
                except:
                    continue
            
            if not next_button:
                self.logger.info("Next button not found or not visible")
                return False
            
            # Check if the next button is disabled
            if next_button.is_disabled():
                self.logger.info("Next button is disabled, reached last page")
                return False
            
            # Click the next button
            try:
                next_button.click()
                self.logger.info("Clicked next page button")
                
                # Wait for page to load
                self.human_delay(3, 5)
                
                # Wait for new content to load
                try:
                    page.wait_for_load_state('networkidle', timeout=10000)
                except:
                    pass
                
                # Handle Workday loading for the new page
                self.handle_workday_loading(page)
                
                return True
                
            except Exception as e:
                self.logger.warning(f"Failed to click next button: {e}")
                return False
        
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False
    
    def scrape_jobs_from_page(self, page):
        """Scrape all jobs from the current page."""
        jobs_found = 0
        
        try:
            # Handle Workday loading
            if not self.handle_workday_loading(page):
                self.logger.warning("Workday loading issues, continuing anyway...")
            
            # Workday-specific job selectors - Based on actual HTML structure
            selectors_to_try = [
                # Mission Australia specific selectors (from provided HTML)
                'li.css-1q2dra3',  # Primary job card container
                'ul[aria-label*="Page"] li',  # Job list items
                '[data-automation-id="jobPosting"]',
                '[data-automation-id*="job"]',
                '.jobPosting',
                '.job-posting',
                '.wd-card',
                'li[data-automation-id]',
                'div[data-automation-id*="job"]',
                # Generic job card selectors
                '.job-card',
                '.job-item',
                '.job-listing',
                '.search-result',
                'article',
                '.listing-item',
                'li[role="listitem"]',
                'div[role="listitem"]'
            ]
            
            job_cards = []
            
            # Try each selector with timeout
            for selector in selectors_to_try:
                try:
                    page.wait_for_selector(selector, timeout=10000)
                    potential_cards = page.query_selector_all(selector)
                    if potential_cards and len(potential_cards) > 0:
                        # Filter for visible cards with content
                        visible_cards = []
                        for card in potential_cards:
                            try:
                                if card.is_visible():
                                    card_text = card.inner_text().strip()
                                    if card_text and len(card_text) > 20:  # Has substantial content
                                        visible_cards.append(card)
                            except:
                                continue
                        
                        if visible_cards:
                            job_cards = visible_cards
                            self.logger.info(f"Found {len(job_cards)} job cards using selector: {selector}")
                            break
                except:
                    continue
            
            if not job_cards:
                self.logger.warning("No job listings found on page")
                return 0
            
            self.logger.info(f"Processing {len(job_cards)} job listings on current page")
            
            for i, job_card in enumerate(job_cards):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                        return -1  # Signal to stop
                    
                    # Extract job data
                    job_data = self.extract_job_data(job_card)
                    
                    if job_data and job_data.get('job_title') and job_data.get('job_url'):
                        # Extract full description from individual job page
                        if job_data.get('job_url'):
                            try:
                                # Create a new page for job detail extraction
                                detail_page = page.context.new_page()
                                try:
                                    job_details = self.extract_full_job_description(job_data['job_url'], detail_page)
                                    
                                    # Use full description if available
                                    if job_details and job_details.get('description'):
                                        job_data['summary'] = job_details['description']
                                        self.logger.debug(f"Extracted full description for: {job_data['job_title']}")
                                    
                                    # Use salary information from description if available
                                    if job_details and job_details.get('salary_info'):
                                        job_data['salary_text'] = job_details['salary_info']
                                        self.logger.debug(f"Extracted salary from description: {job_details['salary_info']}")
                                    
                                finally:
                                    detail_page.close()
                            except Exception as e:
                                self.logger.warning(f"Failed to extract full description for {job_data['job_title']}: {e}")
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            self.jobs_scraped += 1
                            jobs_found += 1
                            self.logger.info(f"Processed job {i+1}/{len(job_cards)}: {job_data['job_title']}")
                        
                        # Add delay between job processing
                        self.human_delay(1, 3)
                    else:
                        self.logger.debug(f"Skipped job card {i+1} - missing required data")
                    
                except Exception as e:
                    self.logger.error(f"Error processing job card {i+1}: {e}")
                    self.error_count += 1
                    continue
            
            return jobs_found
            
        except Exception as e:
            self.logger.error(f"Error scraping jobs from page: {e}")
            return 0
    
    def run(self):
        """Main scraping method."""
        print("🔍 Professional Mission Australia Workday Job Scraper")
        print("=" * 60)
        print(f"Target: {self.job_limit or 'All'} jobs from Mission Australia careers portal")
        print("Database: Professional structure with JobPosting, Company, Location")
        print("Source: Workday-powered careers portal")
        print("=" * 60)
        
        self.logger.info("Starting Professional Mission Australia Workday job scraper...")
        self.logger.info(f"Starting URL: {self.search_url}")
        self.logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        
        with sync_playwright() as p:
            # Launch browser with Workday-compatible settings
            browser = p.chromium.launch(
                headless=True,  # Visible browser for debugging
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-first-run',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-web-security',  # Help with CORS issues
                    '--disable-features=VizDisplayCompositor',
                    '--ignore-certificate-errors-spki-list',
                    '--ignore-ssl-errors',
                    '--allow-running-insecure-content'
                ]
            )
            
            # Create context with Workday-compatible settings
            context = browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                    # Removed 'Upgrade-Insecure-Requests' to avoid CORS issues with Workday
                }
            )
            
            # Add stealth scripts to avoid Workday detection
            context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Override the plugins property to use a custom getter
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // Override the languages property to use a custom getter
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en', 'en-AU'],
                });
                
                // Override the permissions property
                const originalQuery = window.navigator.permissions.query;
                return originalQuery.apply(this, arguments);
            """)
            
            page = context.new_page()
            
            try:
                # Navigate to Mission Australia careers page
                self.logger.info(f"Navigating to Mission Australia careers: {self.search_url}")
                page.goto(self.search_url, wait_until='domcontentloaded', timeout=60000)
                self.human_delay(5, 8)
                
                # Handle Workday loading
                self.handle_workday_loading(page)
                
                # Start scraping with pagination support
                self.logger.info("Starting job extraction with pagination...")
                
                current_page = 1
                total_jobs_found = 0
                
                while True:
                    self.logger.info(f"Processing page {current_page}...")
                    
                    # Scroll page to ensure all content is loaded
                    self.scroll_page(page)
                    
                    # Scrape jobs from the current page
                    jobs_found = self.scrape_jobs_from_page(page)
                    
                    if jobs_found == -1:  # Job limit reached
                        self.logger.info("Job limit reached, stopping pagination")
                        break
                    
                    total_jobs_found += jobs_found
                    self.pages_scraped += 1
                    
                    self.logger.info(f"Page {current_page} completed: {jobs_found} jobs extracted")
                    
                    # Check if we should continue to next page
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info("Job limit reached, stopping pagination")
                        break
                    
                    # Try to navigate to next page
                    if not self.navigate_to_next_page(page):
                        self.logger.info("No more pages available or navigation failed")
                        break
                    
                    current_page += 1
                    
                    # Add delay between pages
                    self.human_delay(3, 6)
                
                if total_jobs_found > 0:
                    self.logger.info(f"Successfully extracted {total_jobs_found} jobs from {self.pages_scraped} pages")
                else:
                    self.logger.warning("No jobs were extracted from Mission Australia")
                
            except Exception as e:
                self.logger.error(f"Scraping failed: {e}")
                self.error_count += 1
            
            finally:
                browser.close()
        
        # Final statistics with thread-safe database call
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: JobPosting.objects.filter(external_source='mission_australia_workday').count())
                total_jobs_in_db = future.result(timeout=10)
        except Exception as e:
            self.logger.error(f"Error getting final job count: {e}")
            total_jobs_in_db = "Unknown"
        
        # Print final results
        self.logger.info("=" * 60)
        self.logger.info("MISSION AUSTRALIA WORKDAY SCRAPING COMPLETED!")
        self.logger.info(f"Pages processed: {self.pages_scraped}")
        self.logger.info(f"Total jobs found: {self.jobs_scraped}")
        self.logger.info(f"Jobs saved to database: {self.jobs_scraped}")
        self.logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
        self.logger.info(f"Errors encountered: {self.error_count}")
        self.logger.info(f"Total Mission Australia jobs in database: {total_jobs_in_db}")
        self.logger.info("=" * 60)


def main():
    """Main entry point."""
    job_limit = None
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except ValueError:
            print("Usage: python missionaustralia_workday_scraper.py [job_limit]")
            print("job_limit must be a number")
            sys.exit(1)
    
    scraper = MissionAustraliaWorkdayJobScraper(job_limit=job_limit)
    scraper.run()


if __name__ == "__main__":
    main()
