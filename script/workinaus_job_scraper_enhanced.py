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
from bs4 import BeautifulSoup

# Django setup
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
    handlers=[logging.FileHandler('scraper_workinaus_enhanced.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class WorkinAUSScraperEnhanced:
    def __init__(self, headless=True, job_limit=5, job_category="all", location="all"):
        """Initialize the enhanced scraper with detailed job extraction."""
        logger.info("Initializing Enhanced WorkinAUS scraper...")
        
        self.headless = headless
        self.base_url = "https://workinaus.com.au"
        self.search_path = "/job/searched"
        self.page_param = "pageNo"
        self.job_limit = job_limit
        self.job_category = job_category
        self.location = location

        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0

        logger.info(f"Job limit: {job_limit} | Category: {job_category} | Location: {location}")
        
        # Get or create system user
        self.system_user = self.get_or_create_system_user()
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()

    def get_or_create_system_user(self):
        """Get or create system user for job postings."""
        try:
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
                logger.info("Found existing system user: workinaus_scraper_system")
            
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {e}")
            return None

    def human_delay(self, a=1.0, b=3.0):
        """Add a human-like delay between actions."""
        delay = random.uniform(a, b)
        time.sleep(delay)

    def parse_date(self, date_string):
        """Parse date string to extract date information."""
        if not date_string:
            return None
        
        s = date_string.lower().strip()
        now = timezone.now()
        
        if 'today' in s:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        if 'yesterday' in s:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        m = re.search(r'(\d+)\s*(hour|day|week|month)s?\s*ago', s)
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            delta = dict(hour=timedelta(hours=n),
                         day=timedelta(days=n),
                         week=timedelta(weeks=n),
                         month=timedelta(days=30*n)).get(unit, None)
            if delta:
                return now - delta
        
        return now.replace(hour=9, minute=0, second=0, microsecond=0)

    def parse_salary(self, salary_text):
        """Parse salary text to extract min and max values."""
        if not salary_text:
            return None, None
        
        # Remove common words and clean up
        salary_text = salary_text.lower().replace('salary', '').replace('package', '').strip()
        
        # Look for salary ranges like "$77,000 - $80,000 Annual" or "$77k - $80k"
        range_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)\s*-\s*\$?(\d+(?:,\d+)?(?:k|000)?)', salary_text)
        if range_match:
            min_sal = self._normalize_salary(range_match.group(1))
            max_sal = self._normalize_salary(range_match.group(2))
            return min_sal, max_sal
        
        # Look for single salary like "$60,000" or "$60k"
        single_match = re.search(r'\$?(\d+(?:,\d+)?(?:k|000)?)', salary_text)
        if single_match:
            salary = self._normalize_salary(single_match.group(1))
            return salary, salary
        
        return None, None

    def _normalize_salary(self, salary_str):
        """Convert salary string to numeric value."""
        try:
            salary_str = salary_str.lower().replace(',', '').strip()
            if 'k' in salary_str:
                return int(salary_str.replace('k', '')) * 1000
            return int(salary_str)
        except (ValueError, AttributeError):
            return None

    def convert_to_html_format(self, description):
        """Convert plain text description to HTML format while preserving structure."""
        if not description:
            return ""
        
        # Split into lines and process
        lines = description.strip().split('\n')
        html_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Convert to HTML with basic formatting
            line = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            # Detect headings (lines that are all caps or end with colons)
            if (line.isupper() and len(line) > 3) or line.endswith(':'):
                html_lines.append(f'<h3>{line}</h3>')
            # Detect bullet points
            elif line.startswith('•') or line.startswith('-') or line.startswith('*'):
                html_lines.append(f'<li>{line[1:].strip()}</li>')
            # Regular text
            else:
                html_lines.append(f'<p>{line}</p>')
        
        # Wrap bullet points in ul tags
        html_content = '\n'.join(html_lines)
        html_content = re.sub(r'(<li>.*?</li>)', r'<ul>\1</ul>', html_content, flags=re.DOTALL)
        html_content = re.sub(r'</ul>\s*<ul>', '', html_content)  # Merge adjacent ul tags
        
        return html_content

    def extract_skills_from_description(self, description):
        """Extract skills and preferred skills from job description using keyword matching."""
        if not description:
            return [], []
        
        # Convert to lowercase for matching
        desc_lower = description.lower()
        
        # Common technical skills
        technical_skills = [
            'python', 'java', 'javascript', 'react', 'angular', 'vue', 'node.js', 'html', 'css',
            'sql', 'mysql', 'postgresql', 'mongodb', 'docker', 'kubernetes', 'aws', 'azure',
            'git', 'linux', 'windows', 'c++', 'c#', '.net', 'php', 'ruby', 'django', 'flask',
            'spring', 'hibernate', 'rest api', 'graphql', 'microservices', 'devops', 'ci/cd',
            'jenkins', 'terraform', 'ansible', 'redis', 'elasticsearch', 'kafka', 'spark',
            'machine learning', 'data science', 'artificial intelligence', 'deep learning',
            'tensorflow', 'pytorch', 'pandas', 'numpy', 'tableau', 'power bi', 'excel',
            'photoshop', 'illustrator', 'figma', 'sketch', 'autocad', 'solidworks'
        ]
        
        # Soft skills
        soft_skills = [
            'communication', 'leadership', 'teamwork', 'problem solving', 'analytical thinking',
            'project management', 'time management', 'customer service', 'sales', 'marketing',
            'negotiation', 'presentation', 'mentoring', 'training', 'coaching', 'collaboration',
            'critical thinking', 'creativity', 'adaptability', 'attention to detail',
            'organizational skills', 'multitasking', 'decision making', 'conflict resolution'
        ]
        
        # Industry-specific skills
        industry_skills = [
            'accounting', 'finance', 'healthcare', 'nursing', 'teaching', 'education',
            'engineering', 'construction', 'manufacturing', 'logistics', 'supply chain',
            'quality assurance', 'quality control', 'safety', 'compliance', 'audit',
            'legal', 'paralegal', 'hr', 'recruitment', 'payroll', 'benefits',
            'retail', 'hospitality', 'customer service', 'food service', 'kitchen',
            'cooking', 'chef', 'bartending', 'housekeeping', 'maintenance'
        ]
        
        all_skills = technical_skills + soft_skills + industry_skills
        
        # Find skills mentioned in description
        found_skills = []
        preferred_skills = []
        
        for skill in all_skills:
            if skill in desc_lower:
                # Check if it's mentioned as preferred/nice-to-have
                skill_context = self._get_skill_context(desc_lower, skill)
                if any(word in skill_context for word in ['prefer', 'nice', 'bonus', 'plus', 'advantage', 'desirable']):
                    preferred_skills.append(skill.title())
                else:
                    found_skills.append(skill.title())
        
        # Remove duplicates and limit to reasonable numbers
        found_skills = list(set(found_skills))[:10]  # Limit to 10 main skills
        preferred_skills = list(set(preferred_skills))[:5]  # Limit to 5 preferred skills
        
        return found_skills, preferred_skills

    def _get_skill_context(self, description, skill):
        """Get the context around a skill mention to determine if it's required or preferred."""
        skill_index = description.find(skill)
        if skill_index == -1:
            return ""
        
        # Get 100 characters before and after the skill mention
        start = max(0, skill_index - 100)
        end = min(len(description), skill_index + len(skill) + 100)
        
        return description[start:end]

    def extract_dates_from_description(self, description, card_text=""):
        """Extract posting date and closing date from job description and card text."""
        posting_date = None
        closing_date = None
        
        # Combine description and card text for date extraction
        full_text = f"{description} {card_text}".lower()
        
        # Patterns for posting dates
        posting_patterns = [
            r'posted\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'posted\s+(\d{1,2}\s+\w+\s+\d{2,4})',
            r'date\s+posted[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'(\d{1,2})\s+(days?|weeks?|months?)\s+ago',
        ]
        
        # Patterns for closing dates
        closing_patterns = [
            r'closes?\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'deadline[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'apply\s+by[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'closing\s+date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'expires?\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        ]
        
        # Extract posting date
        for pattern in posting_patterns:
            match = re.search(pattern, full_text)
            if match:
                posting_date = match.group(1)
                break
        
        # Extract closing date
        for pattern in closing_patterns:
            match = re.search(pattern, full_text)
            if match:
                closing_date = match.group(1)
                break
        
        return posting_date, closing_date

    def parse_specific_date(self, date_string):
        """Parse specific date strings like '15/12/2024' or '15 Dec 2024'."""
        if not date_string:
            return None
        
        try:
            # Try various date formats (Australian DD/MM/YYYY format first)
            date_formats = [
                '%d/%m/%Y',    # 18/09/2025 (Australian format - most common)
                '%d/%m/%y',    # 18/09/25
                '%d-%m-%Y',    # 18-09-2025
                '%d-%m-%y',    # 18-09-25
                '%d %B %Y',    # 18 September 2025
                '%d %b %Y',    # 18 Sep 2025
                '%B %d, %Y',   # September 18, 2025
                '%b %d, %Y',   # Sep 18, 2025
                '%Y-%m-%d',    # 2025-09-18
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_string.strip(), fmt)
                    # Convert to timezone-aware datetime
                    return timezone.make_aware(parsed_date)
                except ValueError:
                    continue
            
            # If all formats fail, return None
            logger.warning(f"Could not parse date: {date_string}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing date '{date_string}': {e}")
            return None

    def extract_job_data_from_card(self, job_card):
        """Extract basic job data from the job card (left side)."""
        try:
            job_data = {}
            
            # Get the full card text to parse manually
            card_text = job_card.evaluate("el => el.innerText")
            lines = [line.strip() for line in card_text.split('\n') if line.strip()]
            
            # Extract Company name (from h2)
            job_data['company_name'] = ""
            try:
                company_element = job_card.query_selector('h2')
                if company_element:
                    company_name = company_element.evaluate("el => el.innerText").strip()
                    if company_name and len(company_name) > 2:
                        job_data['company_name'] = company_name
            except:
                pass
            
            # Extract Job Title (text analysis approach)
            job_data['job_title'] = ""
            try:
                company_name = job_data.get('company_name', '')
                australian_states = [
                    'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT',
                    'New South Wales', 'Victoria', 'Queensland', 'Western Australia',
                    'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory'
                ]
                
                for line in lines:
                    if (line and len(line) > 3 and len(line) < 80 and
                        line != company_name and
                        line != 'FEATURED' and
                        not any(word in line for word in ['Full time', 'Part time', 'Casual', 'Contract']) and
                        not any(state in line for state in australian_states) and
                        not '/' in line and
                        not '$' in line and
                        not 'seeking' in line.lower() and
                        not 'Apply' in line and
                        not ',' in line):
                        job_data['job_title'] = line
                        break
            except:
                pass
            
            # Extract Location
            job_data['location_text'] = ""
            try:
                australian_states = [
                    'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT',
                    'New South Wales', 'Victoria', 'Queensland', 'Western Australia',
                    'South Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory'
                ]
                
                for line in lines:
                    if any(state in line for state in australian_states):
                        if len(line) > 5 and len(line) < 100:
                            job_data['location_text'] = line
                            break
            except:
                pass
            
            # Extract Salary information
            job_data['salary_text'] = ""
            try:
                for line in lines:
                    if '$' in line and ('Annual' in line or 'Hourly' in line):
                        job_data['salary_text'] = line
                        break
            except:
                pass
            
            # Extract Job Type
            job_data['job_type_text'] = ""
            try:
                for line in lines:
                    if any(word in line for word in ['Full time', 'Part time', 'Casual', 'Contract']):
                        job_data['job_type_text'] = line
                        break
            except:
                pass
            
            # Extract Posted and Closing dates from card text
            job_data['posted_ago'] = ""
            job_data['posted_date'] = ""
            job_data['closing_date'] = ""
            
            try:
                # Look for posted date patterns in card text
                posted_patterns = [
                    r'posted\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                    r'posted\s+(\d{1,2}/\d{1,2}/\d{2,4})',
                ]
                
                # Look for closing date patterns
                closing_patterns = [
                    r'closes?\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                    r'closes?\s+(\d{1,2}/\d{1,2}/\d{2,4})',
                ]
                
                for line in lines:
                    line_lower = line.lower()
                    
                    # Check for posted date
                    for pattern in posted_patterns:
                        match = re.search(pattern, line_lower)
                        if match:
                            job_data['posted_date'] = match.group(1)
                            job_data['posted_ago'] = line.strip()
                            logger.info(f"Found posted date: {match.group(1)}")
                            break
                    
                    # Check for closing date
                    for pattern in closing_patterns:
                        match = re.search(pattern, line_lower)
                        if match:
                            job_data['closing_date'] = match.group(1)
                            logger.info(f"Found closing date: {match.group(1)}")
                            break
            except Exception as e:
                logger.warning(f"Error extracting dates from card: {e}")
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting card data: {e}")
            return None

    def extract_detailed_description(self, page, job_card):
        """Click on job card and extract detailed description from right panel."""
        try:
            logger.info("Clicking on job card to get detailed description...")
            
            # Click on the job card to open detailed view
            job_card.click()
            self.human_delay(2, 3)  # Wait for detail panel to load
            
            # Look for the detailed description in the right panel
            detailed_description = ""
            raw_html = ""
            
            # Try multiple selectors for the job description in the detail panel
            description_selectors = [
                '.job-description',
                '[class*="job-summary"]',
                '[class*="description"]',
                '.job-detail',
                '.job-info',
                'div:has-text("Job Summary")',
                # Look for text areas with substantial content
                'div:has-text("We are seeking")',
                'div:has-text("Requirements")',
                'div:has-text("Responsibilities")',
            ]
            
            # Try to find the description in the detail panel
            for selector in description_selectors:
                try:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        text = desc_element.evaluate("el => el.innerText").strip()
                        html = desc_element.evaluate("el => el.innerHTML").strip()
                        if text and len(text) > 100:  # Make sure it's substantial content
                            detailed_description = text
                            raw_html = html
                            logger.info(f"Found detailed description ({len(text)} chars) using selector: {selector}")
                            break
                except:
                    continue
            
            # If no specific selector worked, try to find the main content area
            if not detailed_description:
                try:
                    # Look for the right panel or main content area
                    content_areas = page.query_selector_all('div, section, article')
                    for area in content_areas:
                        try:
                            text = area.evaluate("el => el.innerText").strip()
                            html = area.evaluate("el => el.innerHTML").strip()
                            # Look for substantial text that contains job-related keywords
                            if (text and len(text) > 200 and 
                                any(keyword in text.lower() for keyword in [
                                    'job summary', 'we are seeking', 'requirements', 'responsibilities',
                                    'experience', 'skills', 'qualifications', 'duties'
                                ])):
                                detailed_description = text
                                raw_html = html
                                logger.info(f"Found detailed description ({len(text)} chars) in content area")
                                break
                        except:
                            continue
                except:
                    pass
            
            # Clean up the description and convert to HTML
            if detailed_description:
                # Remove excessive whitespace and normalize
                detailed_description = re.sub(r'\s+', ' ', detailed_description)
                detailed_description = detailed_description.strip()
                
                # Remove "View all jobs" and similar navigation text
                patterns_to_remove = [
                    r'view all jobs.*$',
                    r'apply now.*$',
                    r'back to search.*$',
                    r'share this job.*$',
                    r'print this job.*$'
                ]
                
                for pattern in patterns_to_remove:
                    detailed_description = re.sub(pattern, '', detailed_description, flags=re.IGNORECASE)
                
                detailed_description = detailed_description.strip()
                
                # Convert to HTML format if we don't have raw HTML or it's not well formatted
                if not raw_html or len(raw_html.strip()) < len(detailed_description):
                    html_description = self.convert_to_html_format(detailed_description)
                else:
                    # Clean up the raw HTML
                    html_description = self.clean_html(raw_html)
                
                logger.info(f"Extracted detailed description: {detailed_description[:200]}...")
                return {
                    'text': detailed_description,
                    'html': html_description
                }
            
            logger.warning("Could not extract detailed description")
            return {
                'text': "",
                'html': ""
            }
            
        except Exception as e:
            logger.error(f"Error extracting detailed description: {e}")
            return {
                'text': "",
                'html': ""
            }

    def clean_html(self, raw_html):
        """Clean and format raw HTML content."""
        if not raw_html:
            return ""
        
        try:
            # Parse with BeautifulSoup to clean up
            soup = BeautifulSoup(raw_html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get clean HTML
            clean_html = str(soup)
            
            # Basic cleanup
            clean_html = re.sub(r'\s+', ' ', clean_html)
            clean_html = clean_html.strip()
            
            return clean_html
        except Exception as e:
            logger.warning(f"Error cleaning HTML: {e}")
            # Fallback to converting text to HTML
            text_version = BeautifulSoup(raw_html, 'html.parser').get_text()
            return self.convert_to_html_format(text_version)

    def find_job_cards(self, page):
        """Find individual job card elements within the WorkinAUS structure."""
        try:
            # Find the main jobs container first
            jobs_container = page.query_selector('.jobs-listing')
            if jobs_container:
                # Look for individual job sections within this container
                individual_jobs = jobs_container.query_selector_all('section.rounded-7')
                if individual_jobs:
                    logger.info(f"Found {len(individual_jobs)} individual job sections")
                    return individual_jobs
        except Exception as e:
            logger.warning(f"Error finding job cards: {e}")
        
        return []

    def is_valid_job_data(self, job_data):
        """Validate job data before saving."""
        if not job_data:
            return False
        
        # Must have a valid job title
        if not job_data.get('job_title') or len(job_data['job_title'].strip()) < 3:
            logger.debug(f"Invalid: No title or title too short")
            return False
        
        # Must have a company name
        if not job_data.get('company_name') or len(job_data['company_name'].strip()) < 2:
            logger.debug(f"Invalid: No company name")
            return False
        
        # Must have a location
        if not job_data.get('location_text') or len(job_data['location_text'].strip()) < 3:
            logger.debug(f"Invalid: No location")
            return False
        
        logger.debug(f"Valid job data: {job_data['job_title']} at {job_data['company_name']}")
        return True

    def scrape_page(self, page):
        """Scrape jobs from current page with enhanced description extraction."""
        try:
            # Wait for page to load
            page.wait_for_load_state('networkidle', timeout=30000)
            self.human_delay(2, 4)
            
            # Find job cards
            job_cards = self.find_job_cards(page)
            
            if not job_cards:
                logger.warning("No job cards found on this page")
                return 0
            
            logger.info(f"Found {len(job_cards)} job cards on current page")
            
            jobs_processed = 0
            
            for i, job_card in enumerate(job_cards):
                try:
                    # Check job limit EARLY
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Stopping scraping.")
                        return -1  # Signal to stop
                    
                    # Scroll job card into view
                    job_card.scroll_into_view_if_needed()
                    self.human_delay(0.5, 1.0)
                    
                    # Extract basic job data from card
                    job_data = self.extract_job_data_from_card(job_card)
                    
                    if job_data and self.is_valid_job_data(job_data):
                        logger.info(f"Processing valid job {i+1}: {job_data['job_title']} at {job_data['company_name']}")
                        
                        # Extract detailed description by clicking on the card
                        description_data = self.extract_detailed_description(page, job_card)
                        if description_data['text']:
                            job_data['summary'] = description_data['text']
                            job_data['html_description'] = description_data['html']
                            
                            # Extract skills from description
                            skills, preferred_skills = self.extract_skills_from_description(description_data['text'])
                            job_data['skills'] = ', '.join(skills)
                            job_data['preferred_skills'] = ', '.join(preferred_skills)
                            
                            # Use dates from card first (more reliable), then from description as fallback
                            if not job_data.get('posted_date') or not job_data.get('closing_date'):
                                card_text = job_card.evaluate("el => el.innerText")
                                desc_posting_date, desc_closing_date = self.extract_dates_from_description(description_data['text'], card_text)
                                
                                # Use card dates if available, otherwise use description dates
                                if not job_data.get('posted_date') and desc_posting_date:
                                    job_data['posted_date'] = desc_posting_date
                                if not job_data.get('closing_date') and desc_closing_date:
                                    job_data['closing_date'] = desc_closing_date
                        else:
                            # Fallback to basic summary from card
                            job_data['summary'] = f"Job at {job_data['company_name']} for {job_data['job_title']} position."
                            job_data['html_description'] = self.convert_to_html_format(job_data['summary'])
                            job_data['skills'] = ""
                            job_data['preferred_skills'] = ""
                        
                        # Save to database
                        if self.save_job_to_database(job_data):
                            jobs_processed += 1
                            self.scraped_count += 1
                            logger.info(f"Successfully saved job {self.scraped_count}: {job_data['job_title']}")
                        else:
                            logger.error(f"Failed to save job: {job_data['job_title']}")
                        
                        # Add delay between jobs to be respectful
                        self.human_delay(1, 2)
                        
                    else:
                        logger.debug(f"Skipping invalid job data for card {i+1}")
                        
                except Exception as e:
                    logger.error(f"Error processing job card {i+1}: {e}")
                    self.error_count += 1
                    continue
            
            return jobs_processed
            
        except Exception as e:
            logger.error(f"Error scraping page: {e}")
            return 0

    def save_job_to_database_sync(self, job_data):
        """Save job data to database synchronously."""
        try:
            with transaction.atomic():
                # Check for duplicates by title + company combination
                existing = JobPosting.objects.filter(
                    title=job_data.get('job_title', ''),
                    company__name=job_data.get('company_name', ''),
                    external_source='workinaus.com.au'
                ).first()
                
                if existing:
                    self.duplicate_count += 1
                    logger.info(f"Duplicate job found: {job_data.get('job_title', 'Unknown')} at {job_data.get('company_name', 'Unknown')}")
                    return existing
                
                # Get or create company
                company = None
                if job_data.get('company_name'):
                    company, _ = Company.objects.get_or_create(
                        name=job_data['company_name'],
                        defaults={
                            'description': f'Company from WorkinAUS scraper',
                            'website': '',
                            'company_size': 'medium'
                        }
                    )
                
                # Get or create location
                location = None
                if job_data.get('location_text'):
                    location_name = job_data['location_text']
                    location, _ = Location.objects.get_or_create(
                        name=location_name,
                        defaults={
                            'country': 'Australia',
                            'state': location_name if any(state in location_name for state in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']) else ''
                        }
                    )
                
                # Parse salary if available
                salary_min, salary_max = None, None
                if job_data.get('salary_text'):
                    salary_min, salary_max = self.parse_salary(job_data['salary_text'])
                
                # Generate a unique external URL
                import hashlib
                unique_data = f"{job_data.get('job_title', '')}-{job_data.get('company_name', '')}-{job_data.get('location_text', '')}"
                url_hash = hashlib.md5(unique_data.encode()).hexdigest()[:8]
                external_url = f"https://workinaus.com.au/job/generated-{url_hash}"
                
                # Determine job category using title and description
                job_category = self._categorize_job(job_data.get('job_title', ''), job_data.get('summary', ''))
                
                # Process posting and closing dates
                parsed_posting_date = None
                
                # Try to parse the posted_date first (from card extraction)
                if job_data.get('posted_date'):
                    parsed_posting_date = self.parse_specific_date(job_data['posted_date'])
                    logger.info(f"Parsed posting date from card: {job_data['posted_date']} -> {parsed_posting_date}")
                
                # Fallback to parsing posted_ago text or current time
                if not parsed_posting_date:
                    parsed_posting_date = self.parse_date(job_data.get('posted_ago')) or timezone.now()
                    logger.info(f"Using fallback posting date: {parsed_posting_date}")
                
                # Log closing date if available
                if job_data.get('closing_date'):
                    logger.info(f"Found closing date: {job_data['closing_date']}")
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data.get('job_title', 'Unknown Position'),
                    company=company,
                    location=location,
                    description=job_data.get('html_description', job_data.get('summary', '')),
                    external_url=external_url,
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency='AUD',
                    salary_type='yearly',
                    salary_raw_text=job_data.get('salary_text', ''),
                    job_type=self._map_job_type(job_data.get('job_type_text', '')),
                    job_category=job_category,
                    date_posted=parsed_posting_date,
                    posted_by=self.system_user,
                    external_source='workinaus.com.au',
                    status='active',
                    posted_ago=job_data.get('posted_ago', ''),
                    skills=job_data.get('skills', ''),
                    preferred_skills=job_data.get('preferred_skills', ''),
                    job_closing_date=job_data.get('closing_date', ''),
                    additional_info=job_data
                )
                
                return job_posting
                
        except Exception as e:
            logger.error(f"Error saving job to database: {e}")
            return None

    def save_job_to_database(self, job_data):
        """Save job data to database with thread safety."""
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.save_job_to_database_sync, job_data)
                result = future.result(timeout=30)
                return result
        except Exception as e:
            logger.error(f"Error in threaded save: {e}")
            return None

    def _map_job_type(self, job_type_text):
        """Map job type text to model choices."""
        if not job_type_text:
            return 'full_time'
        
        job_type_lower = job_type_text.lower()
        
        if any(word in job_type_lower for word in ['full-time', 'full time', 'permanent']):
            return 'full_time'
        elif any(word in job_type_lower for word in ['part-time', 'part time', 'casual']):
            return 'part_time'
        elif any(word in job_type_lower for word in ['contract', 'temporary', 'temp']):
            return 'contract'
        elif any(word in job_type_lower for word in ['internship', 'graduate']):
            return 'internship'
        elif any(word in job_type_lower for word in ['freelance']):
            return 'freelance'
        else:
            return 'full_time'

    def _categorize_job(self, job_title, job_description):
        """Categorize job based on title and description."""
        if not job_title:
            return 'other'
        
        title_lower = job_title.lower()
        desc_lower = (job_description or '').lower()
        combined_text = f"{title_lower} {desc_lower}"
        
        # Hospitality & Tourism (check early for chef/cook jobs)
        if any(keyword in combined_text for keyword in [
            'chef', 'cook', 'kitchen', 'restaurant', 'hotel', 'hospitality', 'tourism',
            'barista', 'waiter', 'waitress', 'bar', 'catering', 'food service', 'beverage',
            'culinary', 'sous chef', 'head chef', 'line cook', 'kitchen hand'
        ]):
            return 'hospitality'
        
        # Agriculture & Environment (check early for landscape jobs)  
        if any(keyword in combined_text for keyword in [
            'landscape gardener', 'landscaping', 'gardener', 'horticulture', 'grounds keeper',
            'agriculture', 'farming', 'farm', 'agricultural', 'environmental', 'sustainability',
            'forestry', 'mining', 'grounds', 'turf', 'irrigation', 'pesticide', 'fertilizer'
        ]):
            return 'agriculture'

        # Engineering & Construction (check after specific categories)
        if any(keyword in combined_text for keyword in [
            'civil engineer', 'mechanical engineer', 'electrical engineer', 'structural engineer',
            'construction', 'architect', 'surveyor', 'project manager construction', 'foreman', 
            'builder', 'tradesman', 'carpenter', 'plumber', 'electrician', 'fitter', 'welder',
            'construction manager', 'site manager', 'building', 'infrastructure'
        ]):
            return 'engineering'
        
        # Technology & IT
        if any(keyword in combined_text for keyword in [
            'software', 'developer', 'programmer', 'software engineer', 'it ', 'tech', 'data analyst',
            'system analyst', 'database', 'web developer', 'mobile developer', 'java', 'python', 
            'javascript', 'react', 'angular', 'node', 'sql', 'cloud', 'devops', 'cybersecurity', 
            'network administrator', 'system administrator', 'software architect', 'full stack',
            'front end', 'back end', 'ui developer', 'ux developer'
        ]):
            return 'technology'
        
        # Healthcare & Medical
        if any(keyword in combined_text for keyword in [
            'nurse', 'doctor', 'medical', 'healthcare', 'health', 'clinical', 'therapy',
            'physiotherapy', 'dental', 'pharmacy', 'paramedic', 'aged care', 'disability'
        ]):
            return 'healthcare'
        
        # Education & Training
        if any(keyword in combined_text for keyword in [
            'teacher', 'educator', 'tutor', 'trainer', 'professor', 'instructor', 'childcare',
            'early childhood', 'education', 'school', 'university', 'training', 'academic'
        ]):
            return 'education'
        
        # Finance & Accounting
        if any(keyword in combined_text for keyword in [
            'accountant', 'finance', 'financial', 'banking', 'investment', 'audit', 'tax',
            'bookkeeper', 'economist', 'actuary', 'insurance', 'credit', 'loan'
        ]):
            return 'finance'
        
        # Sales & Marketing
        if any(keyword in combined_text for keyword in [
            'sales', 'marketing', 'business development', 'account manager', 'customer service',
            'retail', 'commercial', 'advertising', 'promotion', 'brand', 'digital marketing',
            'public relations', 'pr ', 'communications', 'media', 'campaign', 'estimating manager'
        ]):
            return 'sales_marketing'
        


        
        # Administrative & Office
        if any(keyword in combined_text for keyword in [
            'administrative', 'admin', 'office', 'receptionist', 'secretary', 'assistant',
            'coordinator', 'clerk', 'data entry', 'support', 'executive assistant',
            'senior consultant', 'consultant', 'advisor', 'analyst', 'administrator'
        ]):
            return 'administrative'
        
        # Manufacturing & Production
        if any(keyword in combined_text for keyword in [
            'production', 'manufacturing', 'factory', 'warehouse', 'logistics', 'supply chain',
            'operator', 'machinist', 'quality control', 'supervisor', 'forklift'
        ]):
            return 'manufacturing'
        
        # Legal & Government
        if any(keyword in combined_text for keyword in [
            'lawyer', 'legal', 'solicitor', 'barrister', 'paralegal', 'compliance',
            'government', 'public service', 'policy', 'regulation'
        ]):
            return 'legal'
        
        # Creative & Design
        if any(keyword in combined_text for keyword in [
            'designer', 'graphic', 'creative', 'artist', 'photographer', 'video', 'multimedia',
            'ux', 'ui', 'design', 'creative director', 'animator'
        ]):
            return 'creative'
        
        # Transportation & Logistics
        if any(keyword in combined_text for keyword in [
            'driver', 'transport', 'delivery', 'courier', 'logistics', 'truck', 'bus',
            'taxi', 'pilot', 'shipping', 'freight'
        ]):
            return 'transportation'
        

        
        # Human Resources
        if any(keyword in combined_text for keyword in [
            'human resources', 'hr ', 'recruitment', 'recruiter', 'talent acquisition',
            'people', 'workforce', 'payroll', 'benefits'
        ]):
            return 'human_resources'
        
        # Customer Service
        if any(keyword in combined_text for keyword in [
            'customer service', 'call center', 'support', 'help desk', 'customer care',
            'client service', 'customer experience'
        ]):
            return 'customer_service'
        
        # Default fallback
        return 'other'

    def _search_url_for_page(self, page_no):
        """Generate search URL for a specific page."""
        params = {self.page_param: page_no}
        if self.job_category != "all":
            params['category'] = self.job_category
        if self.location != "all":
            params['location'] = self.location
        
        query_string = urlencode(params)
        url = f"{self.base_url}{self.search_path}?{query_string}"
        return url

    def run(self):
        """Main scraping method with enhanced description extraction."""
        logger.info("Starting Enhanced WorkinAUS scraper")
        logger.info(f"Job limit: {self.job_limit} | Category: {self.job_category} | Location: {self.location}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            try:
                page_no = 1
                total_processed = 0

                while True:
                    # Check limit before loading new page
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}. Stopping.")
                        break
                    
                    url = self._search_url_for_page(page_no)
                    logger.info(f"Navigating to page {page_no}: {url}")
                    
                    try:
                        page.goto(url, wait_until='domcontentloaded', timeout=60000)
                        self.human_delay(2.0, 3.5)
                    except Exception as e:
                        logger.error(f"Error navigating to page {page_no}: {e}")
                        break

                    # Scrape current page
                    jobs_on_page = self.scrape_page(page)
                    
                    if jobs_on_page == -1:
                        logger.info("Job limit reached during page processing.")
                        break
                    if jobs_on_page == 0:
                        logger.info(f"No jobs found on page {page_no}. Stopping.")
                        break

                    total_processed += jobs_on_page
                    logger.info(f"Page {page_no} processed: {jobs_on_page} jobs (total: {self.scraped_count})")

                    # Check if we should continue to next page
                    if self.job_limit and self.scraped_count >= self.job_limit:
                        logger.info("Reached job limit. Stopping pagination.")
                        break

                    page_no += 1
                    self.human_delay(4.0, 7.0)

                logger.info("=" * 50)
                logger.info("ENHANCED WORKINAUS SCRAPING COMPLETED")
                logger.info(f"Pages visited: {page_no}")
                logger.info(f"Jobs saved: {self.scraped_count}")
                logger.info(f"Duplicates skipped: {self.duplicate_count}")
                logger.info(f"Errors: {self.error_count}")
                logger.info("=" * 50)

            except Exception as e:
                logger.error(f"Fatal error: {e}")
                raise
            finally:
                browser.close()


def main():
    """Main function to run the enhanced scraper."""
    print("🔍 Enhanced WorkinAUS Job Scraper with Detailed Descriptions")
    print("="*60)
    
    # Parse command line arguments
    job_limit = 5  # Default
    job_category = "all"
    location = "all"
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"Job limit from command line: {job_limit}")
        except ValueError:
            print("Invalid job limit. Using default: 5")
    
    if len(sys.argv) > 2:
        job_category = sys.argv[2].lower()
        print(f"Job category from command line: {job_category}")
    
    if len(sys.argv) > 3:
        location = sys.argv[3].lower()
        print(f"Location from command line: {location}")
    
    print(f"Target: {job_limit} jobs")
    print(f"Category: {job_category}")
    print(f"Location: {location}")
    print("Enhancement: Clicking on cards to extract full detailed descriptions")
    print("="*60)

    scraper = WorkinAUSScraperEnhanced(
        headless=True,
        job_limit=job_limit,
        job_category=job_category,
        location=location
    )
    
    try:
        scraper.run()
        print("Enhanced scraper completed successfully!")
    except KeyboardInterrupt:
        print("Interrupted by user")
        logger.info("Interrupted by user")
    except Exception as e:
        print(f"Run failed: {e}")
        logger.error(f"Run failed: {e}")
        raise


if __name__ == "__main__":
    main()


def run(job_limit=50, job_category="all", location="all"):
    """Automation entrypoint for WorkinAUS enhanced scraper."""
    try:
        scraper = WorkinAUSScraperEnhanced(
            headless=True,
            job_limit=job_limit,
            job_category=job_category,
            location=location
        )
        scraper.run()
        return {
            'success': True,
            'jobs_scraped': getattr(scraper, 'scraped_count', None),
            'duplicate_count': getattr(scraper, 'duplicate_count', None),
            'error_count': getattr(scraper, 'error_count', None),
            'message': 'WorkinAUS enhanced scraping completed'
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
