#!/usr/bin/env python3
"""
Professional Pro Bono Australia Job Scraper using Playwright
=============================================================

Advanced Playwright-based scraper for Pro Bono Australia (https://probonoaustralia.com.au/search-jobs/) 
that integrates with your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Social sector and non-profit industry optimization

Features:
- 🎯 Smart job data extraction from Pro Bono Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional non-profit job categorization

Usage:
    python probonoaustralia_scraper.py [job_limit]
    
Examples:
    python probonoaustralia_scraper.py 20    # Scrape 20 jobs
    python probonoaustralia_scraper.py       # Scrape all available jobs
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
        logging.FileHandler('probonoaustralia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProBonoAustraliaScraper:
    """
    Professional scraper for Pro Bono Australia job listings
    """
    
    def __init__(self, max_jobs=None, headless=True, max_pages=None):
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.headless = headless
        self.base_url = "https://probonoaustralia.com.au"
        self.search_url = "https://probonoaustralia.com.au/search-jobs/"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'new_jobs': 0,
            'duplicate_jobs': 0,
            'errors': 0,
            'companies_created': 0,
            'locations_created': 0,
            'pages_scraped': 0,
            'total_pages_found': 0
        }
        
        # Get or create default user for job postings
        self.default_user, _ = User.objects.get_or_create(
            username='probonoaustralia_scraper',
            defaults={'email': 'scraper@probonoaustralia.com.au'}
        )
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        logger.info("Pro Bono Australia Scraper initialized")
        if max_jobs:
            logger.info(f"Job limit: {max_jobs}")
        else:
            logger.info("No job limit set - will scrape all available jobs")

    def human_delay(self, min_delay=1, max_delay=3):
        """Add human-like delays between requests"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def extract_salary_info(self, text):
        """Extract salary information from text"""
        if not text:
            return None, None, 'yearly', ''
            
        text = text.strip()
        original_text = text
        
        # Common salary patterns for non-profit sector
        patterns = [
            r'(\$[\d,]+)\s*-\s*(\$[\d,]+)',  # $50,000 - $60,000
            r'(\$[\d,]+)\s*to\s*(\$[\d,]+)',  # $50,000 to $60,000
            r'(\$[\d,]+)\s*\+',               # $50,000+
            r'(\$[\d,]+)',                    # $50,000
        ]
        
        salary_min = None
        salary_max = None
        salary_type = 'yearly'
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 2:
                        salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                        salary_max = Decimal(match.group(2).replace('$', '').replace(',', ''))
                    else:
                        salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                        if '+' in text:
                            # For $50,000+ format
                            salary_max = None
                    break
                except (ValueError, AttributeError):
                    continue
        
        # Determine salary type
        if 'hour' in text.lower():
            salary_type = 'hourly'
        elif 'day' in text.lower():
            salary_type = 'daily'
        elif 'week' in text.lower():
            salary_type = 'weekly'
        elif 'month' in text.lower():
            salary_type = 'monthly'
        
        return salary_min, salary_max, salary_type, original_text

    def parse_closing_date(self, date_text):
        """Parse closing date from various formats"""
        if not date_text:
            return None
            
        try:
            # Common formats: "27 Sep, 2025", "12 September, 2025"
            date_text = date_text.strip().replace('Closing:', '').strip()
            
            # Try different date formats
            formats = [
                '%d %b, %Y',      # 27 Sep, 2025
                '%d %B, %Y',      # 27 September, 2025
                '%d/%m/%Y',       # 27/09/2025
                '%d-%m-%Y',       # 27-09-2025
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_text, fmt).date()
                except ValueError:
                    continue
                    
        except Exception as e:
            logger.warning(f"Could not parse date: {date_text} - {e}")
            
        return None

    def get_or_create_company(self, company_name, company_url=None):
        """Get or create company with proper handling"""
        if not company_name or company_name.strip() == '':
            company_name = 'Unknown Company'
            
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                website=company_url if company_url else '',
                description=f'Non-profit/Social Sector organization posting jobs on Pro Bono Australia'
            )
            self.stats['companies_created'] += 1
            logger.info(f"Created new company: {company_name}")
            
        return company

    def get_or_create_location(self, location_text):
        """Get or create location with proper handling"""
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
            
        location_text = location_text.strip()
        
        # Clean up location text
        location_text = re.sub(r'\s+', ' ', location_text)
        
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

    def extract_job_data(self, job_element, page):
        """Extract basic job data from listing page (title and URL only)"""
        try:
            job_data = {}
            
            # Extract job title and URL from the postTitle link
            title_element = job_element.query_selector('a.postTitle')
            if title_element:
                job_data['title'] = title_element.inner_text().strip()
                job_data['url'] = title_element.get_attribute('href')
                if not job_data['url'].startswith('http'):
                    job_data['url'] = urljoin(self.base_url, job_data['url'])
            else:
                logger.warning("No title element found")
                return None
            
            # Extract time posted or featured status (for reference)
            job_data['posted_ago'] = ''
            time_element = job_element.query_selector('.daysago')
            if time_element:
                job_data['posted_ago'] = time_element.inner_text().strip()
            else:
                # Check for featured
                featured_element = job_element.query_selector('.featuredtext')
                if featured_element:
                    job_data['posted_ago'] = 'Featured'
            
            # Check if featured
            class_attr = job_element.get_attribute('class') or ''
            job_data['is_featured'] = ('featured' in class_attr or 
                                     job_data['posted_ago'] == 'Featured')
            
            # Validate essential data
            if not job_data['title'] or len(job_data['title']) < 2:
                logger.warning("Job title too short or empty")
                return None
            
            # Truncate title to avoid database errors
            job_data['title'] = job_data['title'][:200]
            job_data['posted_ago'] = job_data['posted_ago'][:50]
            
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
            page.wait_for_selector('.organisation-head-wrap-new, body', timeout=10000)
            
            job_details = {
                'description': 'Job listing from Pro Bono Australia.',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'full_time',
                'closing_date': None,
                'profession': '',
                'sector': ''
            }
            
            # Extract organization/company name
            org_element = page.query_selector('p.org-add:has-text("Organisation")')
            if org_element:
                org_text = org_element.inner_text()
                # Extract text after "Organisation : "
                if "Organisation :" in org_text:
                    job_details['company'] = org_text.split("Organisation :")[1].strip()
            
            # Extract location (take first location from the list)
            location_element = page.query_selector('p.org-add:has-text("Location")')
            if location_element:
                # Get the first location link
                first_location_link = location_element.query_selector('a')
                if first_location_link:
                    job_details['location'] = first_location_link.inner_text().strip()
            
            # Extract work type
            work_type_element = page.query_selector('p.org-add:has-text("Work type")')
            if work_type_element:
                work_type_text = work_type_element.inner_text().lower()
                if 'part-time' in work_type_text:
                    job_details['job_type'] = 'part_time'
                elif 'contract' in work_type_text:
                    job_details['job_type'] = 'contract'
                elif 'casual' in work_type_text:
                    job_details['job_type'] = 'casual'
                elif 'temporary' in work_type_text:
                    job_details['job_type'] = 'temporary'
                # Default is already 'full_time'
            
            # Extract salary information
            salary_element = page.query_selector('p.org-add:has-text("Salary :")')
            if salary_element:
                salary_text = salary_element.inner_text()
                if "Salary :" in salary_text:
                    salary_raw = salary_text.split("Salary :")[1].strip()
                    job_details['salary_raw_text'] = salary_raw
                    
                    # Parse salary range: $110,000 - $130,000 + superannuation + salary packaging options
                    salary_pattern = r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)'
                    salary_match = re.search(salary_pattern, salary_raw)
                    if salary_match:
                        try:
                            job_details['salary_min'] = Decimal(salary_match.group(1).replace(',', ''))
                            job_details['salary_max'] = Decimal(salary_match.group(2).replace(',', ''))
                        except:
                            pass
                    else:
                        # Try single salary: $100,000
                        single_salary_pattern = r'\$(\d{1,3}(?:,\d{3})*)'
                        single_match = re.search(single_salary_pattern, salary_raw)
                        if single_match:
                            try:
                                job_details['salary_min'] = Decimal(single_match.group(1).replace(',', ''))
                            except:
                                pass
            
            # Extract salary type
            salary_type_element = page.query_selector('p.org-add:has-text("Salary type")')
            if salary_type_element:
                salary_type_text = salary_type_element.inner_text().lower()
                if 'hourly' in salary_type_text:
                    job_details['salary_type'] = 'hourly'
                elif 'monthly' in salary_type_text:
                    job_details['salary_type'] = 'monthly'
                elif 'weekly' in salary_type_text:
                    job_details['salary_type'] = 'weekly'
                # Default is already 'yearly'
            
            # Extract closing date
            closing_element = page.query_selector('p.org-add:has-text("Application closing date")')
            if closing_element:
                closing_text = closing_element.inner_text()
                if "Application closing date :" in closing_text:
                    date_text = closing_text.split("Application closing date :")[1].strip()
                    job_details['closing_date'] = self.parse_closing_date(date_text)
            
            # Extract profession
            profession_element = page.query_selector('p.org-add:has-text("Profession")')
            if profession_element:
                profession_links = profession_element.query_selector_all('a')
                professions = [link.inner_text().strip() for link in profession_links]
                job_details['profession'] = ', '.join(professions)
            
            # Extract sector
            sector_element = page.query_selector('p.org-add:has-text("Sector")')
            if sector_element:
                sector_links = sector_element.query_selector_all('a')
                sectors = [link.inner_text().strip() for link in sector_links]
                job_details['sector'] = ', '.join(sectors)
            
            # Extract job description from Pro Bono Australia specific structure
            description = ''
            
            # Primary selector: About the role section
            try:
                about_role_section = page.query_selector('#about-role')
                if about_role_section:
                    # Get the org-excerpt content within the about-role section
                    org_excerpt = about_role_section.query_selector('.org-excerpt')
                    if org_excerpt:
                        description = org_excerpt.inner_text().strip()
                        logger.info("Found description in #about-role .org-excerpt")
                    else:
                        # Fallback: get all content from about-role section
                        description = about_role_section.inner_text().strip()
                        # Remove the header text
                        if description.startswith('About the role'):
                            description = description.replace('About the role', '').strip()
                        logger.info("Found description in #about-role section")
            except Exception as e:
                logger.warning(f"Error extracting from #about-role: {e}")
            
            # Secondary selectors if primary fails
            if not description or len(description) < 100:
                description_selectors = [
                    '.org-excerpt',             # Specific to Pro Bono Australia
                    '.organisation-details-wrap .org-excerpt', # More specific path
                    '.tabs .org-excerpt',       # Within tabs structure
                    '.entry-content',           # WordPress default content area
                    '.post-content',            # Post content area
                    '.job-description',         # Job-specific description
                    '.job-content',             # Job content area
                ]
                
                for selector in description_selectors:
                    try:
                        desc_element = page.query_selector(selector)
                        if desc_element:
                            desc_text = desc_element.inner_text().strip()
                            # Check if this looks like a real job description (more than basic info)
                            if len(desc_text) > 100 and desc_text not in job_details['company']:
                                description = desc_text
                                logger.info(f"Found description using fallback selector: {selector}")
                                break
                    except Exception as e:
                        continue
            
            # If no description found, try to get the main content area
            if not description:
                try:
                    # Try to get the main content of the page
                    main_content = page.query_selector('main, .main, #main, .container, #content, .content-area')
                    if main_content:
                        full_text = main_content.inner_text().strip()
                        # Look for substantial content that's not just the header info
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                        content_lines = []
                        
                        # Skip header information and get to the actual job description
                        skip_keywords = ['organisation', 'location', 'work type', 'profession', 'sector', 'salary', 'closing date']
                        for line in lines:
                            if len(line) > 20 and not any(keyword in line.lower() for keyword in skip_keywords):
                                content_lines.append(line)
                        
                        if content_lines:
                            description = '\n'.join(content_lines)  # Take all meaningful lines without restriction
                            logger.info("Extracted description from main content area")
                except Exception as e:
                    logger.warning(f"Error extracting from main content: {e}")
            
            if description and len(description) > 50:
                job_details['description'] = description  # Store complete description without any length restrictions
            else:
                # Enhanced fallback description with more context
                sectors = job_details.get('sector', '')
                professions = job_details.get('profession', '')
                
                fallback_parts = [f"Position: {job_details.get('title', 'Job Position')}"]
                fallback_parts.append(f"Organisation: {job_details['company']}")
                fallback_parts.append(f"Location: {job_details['location']}")
                
                if sectors:
                    fallback_parts.append(f"Sector: {sectors}")
                if professions:
                    fallback_parts.append(f"Profession: {professions}")
                if job_details['salary_raw_text']:
                    fallback_parts.append(f"Salary: {job_details['salary_raw_text']}")
                
                fallback_parts.append(f"For full job details, visit: {job_url}")
                
                job_details['description'] = '\n'.join(fallback_parts)
                logger.warning(f"Using enhanced fallback description for {job_url}")
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Could not get job details for {job_url}: {e}")
            return {
                'description': 'No description available',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'full_time',
                'closing_date': None,
                'profession': '',
                'sector': ''
            }

    def categorize_job(self, title, description, company_name):
        """Categorize job using the categorization service"""
        try:
            category = self.categorization_service.categorize_job(title, description)
            
            # Map to specific non-profit categories if applicable
            title_lower = title.lower()
            desc_lower = description.lower()
            
            # Non-profit specific categorizations
            if any(term in title_lower for term in ['fundraising', 'development', 'donor']):
                return 'fundraising'
            elif any(term in title_lower for term in ['volunteer', 'community']):
                return 'community_services'
            elif any(term in title_lower for term in ['policy', 'advocacy', 'government']):
                return 'policy_advocacy'
            elif any(term in title_lower for term in ['program', 'project']):
                return 'program_management'
            elif any(term in title_lower for term in ['communications', 'marketing', 'media']):
                return 'marketing'
            
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
                
                # Get detailed job information from the individual job page
                job_details = self.get_job_details(job_data['url'], page)
                
                # Get or create company using details from job page
                company = self.get_or_create_company(job_details['company'])
                
                # Get or create location using details from job page
                location = self.get_or_create_location(job_details['location'])
                
                # Categorize job
                category = self.categorize_job(job_data['title'], job_details['description'], job_details['company'])
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],  # Truncate title to fit CharField limit
                    description=job_details['description'],
                    company=company,
                    location=location,
                    posted_by=self.default_user,
                    job_category=category,
                    job_type=job_details['job_type'],
                    salary_min=job_details['salary_min'],
                    salary_max=job_details['salary_max'],
                    salary_type=job_details['salary_type'],
                    salary_raw_text=job_details['salary_raw_text'][:200] if job_details['salary_raw_text'] else '',
                    external_source='probonoaustralia.com.au',
                    external_url=job_data['url'][:500],  # Truncate URL if too long
                    posted_ago=job_data.get('posted_ago', '')[:50],  # Truncate posted_ago
                    status='active',
                    additional_info={
                        'is_featured': job_data.get('is_featured', False),
                        'closing_date': job_details['closing_date'].isoformat() if job_details['closing_date'] else None,
                        'profession': job_details['profession'],
                        'sector': job_details['sector'],
                        'scrape_timestamp': datetime.now().isoformat()
                    }
                )
                
                logger.info(f"Saved job: {job_data['title']} at {company.name}")
                self.stats['new_jobs'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    def get_pagination_info(self, page):
        """Extract pagination information from the page"""
        try:
            # Look for pagination div with class 'paginate-purple'
            pagination_div = page.query_selector('.paginate-purple')
            if not pagination_div:
                logger.info("No pagination found - single page")
                return 1, []  # Only 1 page, no additional pages
            
            # Find all page links
            page_links = pagination_div.query_selector_all('a')
            page_numbers = []
            
            for link in page_links:
                href = link.get_attribute('href')
                if href and 'pages=' in href:
                    try:
                        # Extract page number from URL like "/search-jobs/?pages=2&"
                        page_num = int(href.split('pages=')[1].split('&')[0])
                        page_numbers.append(page_num)
                    except (ValueError, IndexError):
                        continue
            
            # Get the highest page number to determine total pages
            total_pages = max(page_numbers) if page_numbers else 1
            
            # Also check for current page and any span elements
            current_page_elem = pagination_div.query_selector('.current')
            current_page = 1
            if current_page_elem:
                try:
                    current_page = int(current_page_elem.inner_text())
                except ValueError:
                    current_page = 1
            
            logger.info(f"Pagination detected: Current page {current_page}, Total pages: {total_pages}")
            return total_pages, page_numbers
            
        except Exception as e:
            logger.warning(f"Error detecting pagination: {e}")
            return 1, []  # Fallback to single page

    def build_page_url(self, page_number):
        """Build URL for a specific page"""
        if page_number == 1:
            return self.search_url
        else:
            return f"{self.search_url}?pages={page_number}&"

    def scrape_jobs(self):
        """Main scraping method with pagination support"""
        logger.info("Starting Pro Bono Australia job scraping...")
        
        if self.max_pages:
            logger.info(f"Max pages to scrape: {self.max_pages}")
        
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(user_agent=self.user_agent)
            page = context.new_page()
            
            try:
                # First, navigate to the main page to detect pagination
                logger.info(f"Navigating to: {self.search_url}")
                page.goto(self.search_url)
                self.human_delay(3, 5)
                
                # Wait for page to load
                page.wait_for_selector('.job-listing, .search-results, body', timeout=15000)
                
                # Detect pagination
                total_pages, page_numbers = self.get_pagination_info(page)
                self.stats['total_pages_found'] = total_pages
                
                # Determine which pages to scrape
                pages_to_scrape = []
                if self.max_pages:
                    pages_to_scrape = list(range(1, min(self.max_pages + 1, total_pages + 1)))
                else:
                    pages_to_scrape = list(range(1, total_pages + 1))
                
                logger.info(f"Will scrape {len(pages_to_scrape)} pages: {pages_to_scrape}")
                
                all_jobs_to_process = []
                
                # Scrape each page
                for page_num in pages_to_scrape:
                    try:
                        logger.info(f"=" * 60)
                        logger.info(f"SCRAPING PAGE {page_num} of {total_pages}")
                        logger.info(f"=" * 60)
                        
                        # Navigate to the page
                        page_url = self.build_page_url(page_num)
                        logger.info(f"Navigating to: {page_url}")
                        page.goto(page_url)
                        self.human_delay(2, 4)
                        
                        # Wait for page to load
                        page.wait_for_selector('.job-listing, .search-results, body', timeout=15000)
                        
                        # Find job listings using the actual HTML structure
                        job_elements = page.query_selector_all('div.all-jobs-list div[class*="post-"][class*="job"][class*="type-job"]')
                        
                        if job_elements:
                            logger.info(f"Found {len(job_elements)} jobs on page {page_num}")
                        else:
                            # Fallback selectors
                            job_selectors = [
                                '.job-listing',
                                '.job-item', 
                                '.search-result',
                                '.job-card',
                                'article',
                                '.job'
                            ]
                            
                            for selector in job_selectors:
                                elements = page.query_selector_all(selector)
                                if elements:
                                    job_elements = elements
                                    logger.info(f"Found {len(elements)} jobs on page {page_num} using fallback selector: {selector}")
                                    break
                        
                        if not job_elements:
                            logger.warning(f"No job elements found on page {page_num}")
                            continue
                        
                        # Extract job data from this page
                        page_jobs = []
                        for i, job_element in enumerate(job_elements, 1):
                            try:
                                logger.info(f"Extracting job {i}/{len(job_elements)} from page {page_num}")
                                job_data = self.extract_job_data(job_element, page)
                                if job_data:
                                    job_data['page_number'] = page_num  # Add page number for tracking
                                    page_jobs.append(job_data)
                                else:
                                    logger.warning(f"Could not extract data for job {i} on page {page_num}")
                            except Exception as e:
                                logger.error(f"Error extracting job {i} on page {page_num}: {e}")
                                continue
                        
                        logger.info(f"Successfully extracted {len(page_jobs)} jobs from page {page_num}")
                        all_jobs_to_process.extend(page_jobs)
                        self.stats['pages_scraped'] += 1
                        
                        # Check if we've reached the job limit
                        if self.max_jobs and len(all_jobs_to_process) >= self.max_jobs:
                            logger.info(f"Reached job limit of {self.max_jobs}, stopping pagination")
                            all_jobs_to_process = all_jobs_to_process[:self.max_jobs]
                            break
                            
                    except Exception as e:
                        logger.error(f"Error scraping page {page_num}: {e}")
                        self.stats['errors'] += 1
                        continue
                
                logger.info(f"=" * 60)
                logger.info(f"PROCESSING ALL COLLECTED JOBS")
                logger.info(f"=" * 60)
                logger.info(f"Total jobs collected from all pages: {len(all_jobs_to_process)}")
                
                # Now process each job by visiting individual pages
                for i, job_data in enumerate(all_jobs_to_process, 1):
                    try:
                        page_num = job_data.get('page_number', 'Unknown')
                        logger.info(f"Processing job {i}/{len(all_jobs_to_process)} (from page {page_num}): {job_data['title']}")
                        
                        self.stats['total_processed'] += 1
                        success = self.save_job(job_data, page)
                        
                        if success:
                            logger.info(f"Successfully saved: {job_data['title']}")
                        
                        # Add delay between jobs
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
        logger.info(f"Pages found: {self.stats['total_pages_found']}")
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Total jobs processed: {self.stats['total_processed']}")
        logger.info(f"New jobs saved: {self.stats['new_jobs']}")
        logger.info(f"Duplicate jobs skipped: {self.stats['duplicate_jobs']}")
        logger.info(f"Companies created: {self.stats['companies_created']}")
        logger.info(f"Locations created: {self.stats['locations_created']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main function"""
    max_jobs = None
    max_pages = None
    
    # Parse command line arguments
    # Usage: python script.py [max_jobs] [max_pages]
    # Examples:
    #   python script.py 20        - Scrape max 20 jobs from all pages
    #   python script.py 20 3      - Scrape max 20 jobs from first 3 pages
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
    scraper = ProBonoAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
    scraper.scrape_jobs()


if __name__ == "__main__":
    main()


def run(max_jobs=None, max_pages=None):
    """Automation entrypoint for Pro Bono Australia scraper."""
    try:
        scraper = ProBonoAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
        scraper.scrape_jobs()
        return {
            'success': True,
            'stats': getattr(scraper, 'stats', {}),
            'message': 'Pro Bono Australia scraping completed'
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
