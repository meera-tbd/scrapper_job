#!/usr/bin/env python3
"""
Professional Scout Jobs Australia Scraper using Playwright
==========================================================

Advanced Playwright-based scraper for Scout Jobs Australia (scoutjobs.com.au) 
that integrates with the existing seek_scraper_project database structure.

Scout Jobs specializes in retail, hospitality, advertising, marketing, design, 
arts, architecture, and media jobs across Australia.

Features:
- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian-specific optimization

Usage:
    python scoutjobs_australia_scraper.py [job_limit]
    
Examples:
    python scoutjobs_australia_scraper.py 20    # Scrape 20 Australian jobs
    python scoutjobs_australia_scraper.py       # Scrape all available jobs
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
# Add the project root to the Python path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
except NameError:
    # Handle case when __file__ is not defined (e.g., in interactive mode)
    project_root = os.getcwd()
sys.path.append(project_root)

django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

# Import your existing models and services
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()


class ScoutJobsAustraliaJobScraper:
    """Professional Scout Jobs Australia job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://scoutjobs.com.au"
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
        
        # Scout Jobs specific industries and positions mapping
        self.scout_industries = {
            'retail': ['sales assistant', 'area/regional manager', 'buyer', 'department manager', 
                      'merchandise planner', 'store manager', 'visual merchandiser', 'hair and beauty services', 
                      'warehousing & distribution'],
            'advertising': ['account management', 'advertising account management', 'advertising management', 
                           'brand management', 'digital & search marketing', 'direct marketing & crm', 
                           'event management', 'market research & analysis', 'marketing assistants/coordinators', 
                           'marketing management', 'promotions', 'public relations', 'trade marketing', 'sales'],
            'design': ['architecture', 'art direction', 'fashion & textile design', 'graphic design', 
                      'illustration & animation', 'industrial design', 'interior design', 'performing arts'],
            'media': ['editing', 'film/television', 'photography', 'product management & development', 
                     'production', 'publishing', 'web & interaction design', 'web development', 'writing'],
            'hospitality': ['hospitality management', 'bar staff', 'baristas', 'chef', 'cook', 'wait staff', 
                           'kitchen hand', 'baker', 'coffee roaster', 'delivery driver', 'front of house & guest services']
        }
        
        # Australian locations available on Scout Jobs
        self.scout_locations = [
            'melbourne', 'sydney', 'brisbane', 'adelaide', 'perth', 'tasmania', 
            'regional victoria', 'regional new south wales', 'canberra', 'darwin'
        ]
        
        # Work types available
        self.work_types = ['full time', 'part time', 'casual']
        
        # Salary ranges (Australian dollars)
        self.salary_ranges = [
            ('0', '40'),
            ('40', '60'), 
            ('60', '80'),
            ('80', '100'),
            ('100', '120'),
            ('120', '150'),
            ('150', '200')
        ]
    
    def setup_logging(self):
        """Setup logging configuration."""
        # Configure logging with UTF-8 encoding for compatibility
        file_handler = logging.FileHandler('scoutjobs_australia_scraper.log', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Set up formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  # Set to INFO for clean output
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user, created = User.objects.get_or_create(
                username='scoutjobs_australia_bot',
                defaults={
                    'email': 'scoutjobs.australia.bot@jobscraper.local',
                    'first_name': 'Scout Jobs Australia',
                    'last_name': 'Scraper Bot'
                }
            )
            if created:
                self.logger.info("Created new bot user for Scout Jobs Australia scraping")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return None
    
    def human_delay(self, min_delay=2, max_delay=5):
        """Add human-like delays to avoid detection."""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def setup_browser(self):
        """Setup Playwright browser with stealth configuration."""
        self.logger.info("Setting up Playwright browser for Scout Jobs Australia...")
        
        playwright = sync_playwright().start()
        
        # Browser configuration for anti-detection
        self.browser = playwright.chromium.launch(
            headless=True,  # Visible browser for better success rate
            slow_mo=1000,    # Add delay between actions
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-blink-features=AutomationControlled',
                '--disable-automation',
                '--disable-extensions'
            ]
        )
        
        # Create context with Australian settings
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-AU',  # Australian locale
            timezone_id='Australia/Sydney',
            geolocation={'latitude': -33.8688, 'longitude': 151.2093},  # Sydney coordinates
            permissions=['geolocation']
        )
        
        # Add stealth scripts
        self.context.add_init_script("""
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            // Mock languages for Australia
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-AU', 'en-US', 'en'],
            });
            
            // Mock chrome object
            window.chrome = {
                runtime: {}
            };
        """)
        
        # Create page
        self.page = self.context.new_page()
        
        # Set additional headers for Australia
        self.page.set_extra_http_headers({
            'Accept-Language': 'en-AU,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        self.logger.info("Browser setup completed successfully")
    
    def close_browser(self):
        """Clean up browser resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            self.logger.info("Browser closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing browser: {e}")
    
    def should_scrape_main_page(self):
        """Simply return True since we're only scraping the main jobs page."""
        return True
    
    def navigate_to_jobs_page(self, page_number=1):
        """Navigate to Scout Jobs page (with pagination support)."""
        try:
            # Build URL with page number
            if page_number == 1:
                url = f"{self.base_url}/jobs"
            else:
                url = f"{self.base_url}/jobs?page={page_number}"
            
            self.logger.info(f"Navigating to page {page_number}: {url}")
            
            # Try different loading strategies
            try:
                # First try with networkidle (60 seconds)
                self.page.goto(url, wait_until='networkidle', timeout=60000)
                self.logger.info("Page loaded with networkidle strategy")
            except Exception as e:
                self.logger.warning(f"networkidle failed: {e}")
                try:
                    # Fallback to domcontentloaded (30 seconds)
                    self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    self.logger.info("Page loaded with domcontentloaded strategy")
                except Exception as e2:
                    self.logger.warning(f"domcontentloaded failed: {e2}")
                    # Last resort - just load without waiting
                    self.page.goto(url, timeout=20000)
                    self.logger.info("Page loaded with basic strategy")
            
            # Wait for basic page structure
            try:
                self.page.wait_for_selector('body', timeout=10000)
                self.logger.info("Body element found")
            except:
                self.logger.warning("Body element not found, but continuing")
            
            self.human_delay(3, 6)  # Give extra time for dynamic content
            
            # Log page title for debugging
            try:
                page_title = self.page.title()
                self.logger.info(f"Page title: {page_title}")
            except:
                pass
            
            # Check page content
            try:
                page_content = self.page.content()[:500]  # First 500 chars
                self.logger.debug(f"Page content sample: {page_content}")
            except:
                pass
            
            return True  # Continue even if some checks fail
            
        except Exception as e:
            self.logger.error(f"Error navigating to jobs page: {e}")
            return False
    
    def find_job_elements(self):
        """Find job card elements on the current page."""
        self.logger.info("Searching for job elements...")
        
        # Based on the actual HTML structure provided, Scout Jobs uses specific selectors
        selectors = [
            # Primary selector - each job is in a div with class "row-fluid search-result"
            'div.row-fluid.search-result',
            '.search-result',
            'div[class*="search-result"]',
            # Fallback selectors
            'div.row-fluid',
            'div:has-text("Save Job")',
            '.result-content',
            'article',
            'section'
        ]
        
        for selector in selectors:
            try:
                self.logger.debug(f"Trying selector: {selector}")
                elements = self.page.query_selector_all(selector)
                if elements:
                    # Filter out elements that are too small (likely not job cards)
                    valid_elements = []
                    for element in elements:
                        try:
                            # Check if element has substantial text content
                            text_content = element.text_content()
                            if text_content and len(text_content.strip()) > 50:
                                valid_elements.append(element)
                        except:
                            continue
                    
                    if valid_elements:
                        self.logger.info(f"Found {len(valid_elements)} valid job elements using selector: {selector}")
                        return valid_elements
                    else:
                        self.logger.debug(f"Found {len(elements)} elements but none with sufficient content: {selector}")
                else:
                    self.logger.debug(f"No elements found with selector: {selector}")
            except Exception as e:
                self.logger.error(f"Error with selector {selector}: {e}")
                continue
        
        self.logger.warning("No job elements found with any selector")
        return []
    
    def extract_text_by_selectors(self, element, selectors):
        """Try multiple selectors to extract text."""
        for selector in selectors:
            try:
                text_element = element.query_selector(selector)
                if text_element:
                    text = text_element.text_content() or text_element.get_attribute("title") or ""
                    if text and text.strip():
                        return text.strip()
            except:
                continue
        return ""
    
    def extract_job_data(self, job_element):
        """Extract job data from a single job element based on Scout Jobs HTML structure."""
        try:
            # Based on the actual HTML structure:
            # <h3 class="result-title">Job Title</h3>
            # <h4 class="group-name">Company Name</h4>
            # <p class="result-meta">Location</p>
            # <div class="short_descr">Description</div>
            
            # Extract job title from h3.result-title
            title = self.extract_text_by_selectors(job_element, [
                "h3.result-title",
                ".result-title",
                "h3",
                "[class*='result-title']"
            ])
            
            # Extract company name from h4.group-name
            company = self.extract_text_by_selectors(job_element, [
                "h4.group-name",
                ".group-name", 
                "h4",
                "[class*='group-name']"
            ])
            
            # Extract location from .result-meta
            location = self.extract_text_by_selectors(job_element, [
                ".result-meta",
                "p.result-meta",
                "[class*='result-meta']"
            ])
            
            # Extract URL from the main link (a href)
            url = ""
            try:
                # The main link wraps the content
                link_element = job_element.query_selector("a[href*='/job/']")
                if link_element:
                    href = link_element.get_attribute("href")
                    if href:
                        url = urljoin(self.base_url, href) if not href.startswith("http") else href
                
                # Fallback: any link in the job element
                if not url:
                    link_element = job_element.query_selector("a")
                    if link_element:
                        href = link_element.get_attribute("href")
                        if href and '/job/' in href:
                            url = urljoin(self.base_url, href) if not href.startswith("http") else href
                            
            except Exception as e:
                self.logger.debug(f"Error extracting URL: {e}")
                url = ""
            
            # Extract description from .short_descr
            description = self.extract_text_by_selectors(job_element, [
                ".short_descr",
                "div.short_descr",
                "[class*='short_descr']",
                ".description",
                "p"  # Fallback to any paragraph
            ])
            
            # Extract benefits/additional info from ul/li elements
            benefits = ""
            try:
                ul_elements = job_element.query_selector_all("ul li")
                if ul_elements:
                    benefit_list = []
                    for li in ul_elements:
                        benefit_text = li.text_content().strip()
                        if benefit_text:
                            benefit_list.append(benefit_text)
                    if benefit_list:
                        benefits = " | ".join(benefit_list)
            except:
                pass
            
            # Combine description and benefits
            if benefits and description:
                description = f"{description}\n\nBenefits: {benefits}"
            elif benefits and not description:
                description = f"Benefits: {benefits}"
            
            # Extract salary (not visible in this example but may exist in other jobs)
            salary = self.extract_text_by_selectors(job_element, [
                ".salary",
                "[class*='salary']",
                "[class*='pay']",
                "[class*='wage']",
                ".compensation"
            ])
            
            # No specific date field visible in the HTML structure
            posted_date = ""
            
            # Debug logging for extracted fields
            self.logger.debug(f"Extracted - Title: '{title}', Company: '{company}', Location: '{location}', URL: '{url}'")
            
            # Additional debug: log the element's HTML structure if extraction fails
            if not title or not company:
                try:
                    element_html = job_element.inner_html()[:300]  # First 300 chars
                    self.logger.debug(f"Element HTML sample: {element_html}")
                except:
                    pass
            
            # Skip if missing essential data
            if not title or not company:
                self.logger.warning(f"Skipping job: missing title ('{title}') or company ('{company}')")
                return None
            
            # Clean and prepare data
            title = self.clean_text(title)
            company = self.clean_text(company)
            location = self.clean_text(location) if location else "Australia"
            salary = self.clean_text(salary) if salary else ""
            description = self.clean_text(description) if description else ""
            posted_date = self.clean_text(posted_date) if posted_date else ""
            
            # Detect job type from content (will be updated later from full description)
            detected_job_type = self.detect_job_type(job_element, title, description)
            
            # Note: Full description will be extracted separately to avoid element staleness
            
            return {
                'title': title,
                'company_name': company,
                'location': location,
                'description': description,
                'external_url': url,
                'salary_text': salary,
                'job_type': detected_job_type,
                'posted_date': posted_date,
                'external_source': 'scoutjobs.com.au',
                'country': 'Australia'
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
    def extract_full_job_description(self, job_url):
        """Extract clean formatted text content from the job detail page."""
        if not job_url:
            return ""
        
        try:
            self.logger.debug(f"Visiting job page for clean text content: {job_url}")
            
            # Navigate to job detail page
            self.page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(1, 2)
            
            # Target the specific job detail container
            job_detail_container = self.page.query_selector('div.span7.preview-main.job-detail-main[itemscope][itemtype="http://schema.org/JobPosting"]')
            
            if not job_detail_container:
                # Fallback to less specific selectors
                job_detail_container = self.page.query_selector('.job-detail-main') or self.page.query_selector('.preview-main')
            
            if not job_detail_container:
                self.logger.debug("Job detail container not found")
                return ""
            
            # Extract structured text content
            try:
                formatted_text = self.extract_structured_text_content(job_detail_container)
                
                if formatted_text:
                    self.logger.debug("Successfully extracted structured text content")
                    return formatted_text.strip()
                else:
                    self.logger.debug("No structured text content found")
                    return ""
                
            except Exception as e:
                self.logger.debug(f"Error extracting structured text: {e}")
                return ""
            
        except Exception as e:
            self.logger.debug(f"Error extracting clean text content from {job_url}: {e}")
            return ""
    
    def extract_structured_text_content(self, container):
        """Extract and format text content in a structured way."""
        import re
        result_parts = []
        
        try:
            # 1. Extract company logo alt text (company name)
            logo_img = container.query_selector('img.job-logo')
            if logo_img:
                company_name = logo_img.get_attribute('alt')
                if company_name:
                    result_parts.append(company_name.strip())
            
            # 2. Extract job title
            job_title = container.query_selector('h1.job-title')
            if job_title:
                title_text = job_title.text_content().strip()
                if title_text:
                    result_parts.append(title_text)
            
            # 3. Extract company name from group-name
            company_info = container.query_selector('h4.group-name span[itemprop="name"]')
            if company_info:
                company_text = company_info.text_content().strip()
                if company_text:
                    result_parts.append(company_text)
            
            # 4. Extract metadata (Date Listed, Location, Salary, etc.) with proper formatting
            meta_items = container.query_selector_all('.job-meta ul li')
            for item in meta_items:
                try:
                    label_elem = item.query_selector('.l')
                    value_elem = item.query_selector('.val')
                    
                    if label_elem and value_elem:
                        label = label_elem.text_content().strip()
                        value = value_elem.text_content().strip()
                        
                        # Clean up the value text - remove excessive whitespace and newlines
                        value = re.sub(r'\s+', ' ', value)
                        value = value.replace('\n', ' ').replace('\r', ' ')
                        
                        if label and value:
                            result_parts.append(f"{label} {value}")
                except:
                    continue
            
            # Add an empty line after metadata for better readability
            if result_parts:
                result_parts.append("")
            
            # 5. Extract short description
            short_descr = container.query_selector('.short-descr')
            if short_descr:
                short_text = short_descr.text_content().strip()
                if short_text:
                    # Clean the short description text
                    short_text = re.sub(r'\s+', ' ', short_text)
                    result_parts.append(short_text)
                    result_parts.append("")  # Add spacing after short description
            
            # 6. Extract bullet points (benefits/highlights) with better formatting
            bullet_points = []
            bullet_lists = container.query_selector_all('ul')
            for ul in bullet_lists:
                # Skip the metadata ul
                if 'job-meta' in (ul.get_attribute('class') or ''):
                    continue
                
                # Get items from this list
                items = ul.query_selector_all('li')
                for li in items:
                    li_text = li.text_content().strip()
                    # Clean the text
                    li_text = re.sub(r'\s+', ' ', li_text)
                    
                    # Skip metadata items and empty items
                    if (li_text and len(li_text) > 3 and 
                        not any(skip in li_text.lower() for skip in ['date listed:', 'location:', 'salary:', 'industry:', 'position:', 'work type:'])):
                        bullet_points.append(f"• {li_text}")
            
            # Add bullet points if any were found
            if bullet_points:
                result_parts.extend(bullet_points)
                result_parts.append("")  # Add spacing after bullet points
            
            # 7. Extract detailed description with better paragraph formatting
            detail_descr = container.query_selector('.detail-descr')
            if detail_descr:
                detail_text = detail_descr.text_content().strip()
                
                if detail_text:
                    # Replace HTML entities
                    detail_text = detail_text.replace('&nbsp;', ' ')
                    detail_text = detail_text.replace('&amp;', '&')
                    detail_text = detail_text.replace('&lt;', '<')
                    detail_text = detail_text.replace('&gt;', '>')
                    
                    # Split into paragraphs and clean each one
                    paragraphs = re.split(r'\n\s*\n|\r\n\s*\r\n', detail_text)
                    cleaned_paragraphs = []
                    
                    for para in paragraphs:
                        # Clean excessive whitespace but preserve sentence structure
                        para = re.sub(r'\s+', ' ', para.strip())
                        
                        # Only include substantial content
                        if para and len(para) > 10:
                            cleaned_paragraphs.append(para)
                    
                    # Join paragraphs with proper spacing
                    if cleaned_paragraphs:
                        result_parts.append('\n\n'.join(cleaned_paragraphs))
                        result_parts.append("")  # Add spacing after description
            
            # 8. Extract Apply Now button text
            apply_btn = container.query_selector('a.btn')
            if apply_btn:
                btn_text = apply_btn.text_content().strip()
                if btn_text:
                    result_parts.append(btn_text)
            
            # Combine all parts and clean up final formatting
            final_text = '\n'.join(result_parts)
            
            # Clean up excessive empty lines
            final_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', final_text)
            final_text = re.sub(r'^\n+|\n+$', '', final_text)
            
            return final_text
            
        except Exception as e:
            self.logger.debug(f"Error in extract_structured_text_content: {e}")
            return ""
    
    def extract_job_type_from_description(self, description_text):
        """Extract job type from the full job description metadata."""
        if not description_text:
            return 'full_time'
        
        # Look for "Work Type:" in the description
        import re
        work_type_pattern = r'Work Type:\s*([^\n]+)'
        match = re.search(work_type_pattern, description_text, re.IGNORECASE)
        
        if match:
            work_type_text = match.group(1).strip().lower()
            
            # Map Scout Jobs work types to our job types
            if 'casual' in work_type_text:
                return 'casual'
            elif 'part time' in work_type_text or 'part-time' in work_type_text:
                return 'part_time'
            elif 'contract' in work_type_text:
                return 'contract'
            elif 'temporary' in work_type_text or 'temp' in work_type_text:
                return 'temporary'
            elif 'intern' in work_type_text:
                return 'internship'
            elif 'full time' in work_type_text or 'full-time' in work_type_text:
                return 'full_time'
        
        # Fallback to original detection method
        return 'full_time'
    
    def extract_salary_from_description(self, description_text):
        """Extract salary information from the full job description metadata."""
        if not description_text:
            return None, None, None, None, None
        
        import re
        
        # Initialize default values
        salary_min = None
        salary_max = None
        salary_currency = 'AUD'  # Default for Australian jobs
        salary_type = 'yearly'   # Default assumption
        salary_raw_text = ''
        
        try:
            # Look for "Salary:" line first
            salary_pattern = r'Salary:\s*([^\n]+)'
            salary_match = re.search(salary_pattern, description_text, re.IGNORECASE)
            
            if salary_match:
                salary_text = salary_match.group(1).strip()
                salary_raw_text = salary_text
                
                # Parse different salary formats
                # Format: "75-85k" or "$75k-$85k"
                range_pattern = r'[\$]?(\d+)[\-–](\d+)k'
                range_match = re.search(range_pattern, salary_text, re.IGNORECASE)
                
                if range_match:
                    salary_min = int(range_match.group(1)) * 1000
                    salary_max = int(range_match.group(2)) * 1000
                    salary_type = 'yearly'
                else:
                    # Format: single value like "75k" or "$75,000"
                    single_pattern = r'[\$]?(\d+(?:,\d{3})*)[k]?'
                    single_match = re.search(single_pattern, salary_text)
                    
                    if single_match:
                        amount = int(single_match.group(1).replace(',', ''))
                        # If it ends with 'k', multiply by 1000
                        if 'k' in salary_text.lower():
                            amount *= 1000
                        salary_min = amount
                        salary_max = amount
            
            # Look for structured Min/Max salary data
            min_value_pattern = r'<span itemprop="minValue">(\d+)</span>'
            max_value_pattern = r'<span itemprop="maxValue">(\d+)</span>'
            currency_pattern = r'<span itemprop="currency">([A-Z]{3})</span>'
            unit_pattern = r'<span itemprop="unitText">(\w+)</span>'
            
            min_match = re.search(min_value_pattern, description_text)
            max_match = re.search(max_value_pattern, description_text)
            currency_match = re.search(currency_pattern, description_text)
            unit_match = re.search(unit_pattern, description_text)
            
            # If we have structured data, use it (it's more accurate)
            if min_match and max_match:
                salary_min = int(min_match.group(1))
                salary_max = int(max_match.group(1))
                
                if currency_match:
                    salary_currency = currency_match.group(1)
                
                if unit_match:
                    unit_text = unit_match.group(1).lower()
                    if unit_text == 'year':
                        salary_type = 'yearly'
                    elif unit_text == 'hour':
                        salary_type = 'hourly'
                    elif unit_text == 'month':
                        salary_type = 'monthly'
            
            # If we still don't have raw text, extract from Salary line
            if not salary_raw_text and salary_min and salary_max:
                if salary_type == 'yearly':
                    if salary_min == salary_max:
                        salary_raw_text = f"{salary_currency} {salary_min:,} per year"
                    else:
                        salary_raw_text = f"{salary_currency} {salary_min:,} - {salary_max:,} per year"
                elif salary_type == 'hourly':
                    if salary_min == salary_max:
                        salary_raw_text = f"{salary_currency} {salary_min} per hour"
                    else:
                        salary_raw_text = f"{salary_currency} {salary_min} - {salary_max} per hour"
            
            return salary_min, salary_max, salary_currency, salary_type, salary_raw_text
            
        except Exception as e:
            self.logger.debug(f"Error extracting salary: {e}")
            return None, None, None, None, None
    
    def get_total_pages(self):
        """Get the total number of pages from pagination."""
        try:
            # Look for pagination elements
            pagination = self.page.query_selector('.pagination')
            if not pagination:
                self.logger.info("No pagination found, assuming single page")
                return 1
            
            # Find all page links
            page_links = pagination.query_selector_all('li a')
            max_page = 1
            
            for link in page_links:
                try:
                    href = link.get_attribute('href')
                    if href and 'page=' in href:
                        # Extract page number from URL
                        import re
                        page_match = re.search(r'page=(\d+)', href)
                        if page_match:
                            page_num = int(page_match.group(1))
                            max_page = max(max_page, page_num)
                    
                    # Also check link text for page numbers
                    text = link.text_content().strip()
                    if text.isdigit():
                        page_num = int(text)
                        max_page = max(max_page, page_num)
                        
                except Exception as e:
                    continue
            
            self.logger.info(f"Found {max_page} total pages")
            return max_page
            
        except Exception as e:
            self.logger.warning(f"Error detecting pagination: {e}")
            return 1
    
    def has_next_page(self):
        """Check if there's a next page available."""
        try:
            pagination = self.page.query_selector('.pagination')
            if not pagination:
                return False
            
            # Look for "Next" link that's not disabled
            next_link = pagination.query_selector('a.icn-pag-next')
            if next_link:
                # Check if it's not disabled
                classes = next_link.get_attribute('class') or ''
                return 'disabled' not in classes
            
            return False
            
        except Exception as e:
            self.logger.debug(f"Error checking next page: {e}")
            return False
    
    def clean_description_text(self, text):
        """Clean and format job description text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common navigation/UI elements
        text = re.sub(r'(Apply now|Apply for this job|Back to search|Save job|Share|Print|Save This Search|Refine Search)', '', text, flags=re.IGNORECASE)
        
        # Remove footer/header elements
        text = re.sub(r'(Terms of use|Privacy policy|Cookie policy|Contact us|Scout Jobs|Broadsheet Media)', '', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def detect_job_type(self, job_element, job_title="", job_description=""):
        """Detect job type from Scout Jobs listing based on text indicators."""
        
        # Collect all text from the job element
        try:
            element_text = job_element.text_content().lower()
        except:
            element_text = ""
        
        # Combine all available text for analysis
        combined_text = f"{job_title} {job_description} {element_text}".lower()
        
        # Define job type patterns based on Scout Jobs terminology
        job_type_patterns = {
            'casual': [
                'casual', 'casual position', 'casual role', 'casual work',
                'ad hoc', 'as needed', 'on call', 'when required',
                'zero hours', 'flexible casual', 'casual staff'
            ],
            'part_time': [
                'part time', 'part-time', 'parttime', 'part time position',
                'hours per week', '20 hours', '25 hours', '30 hours',
                'flexible hours', 'reduced hours', 'part-time role'
            ],
            'contract': [
                'contract', 'contractor', 'fixed term', 'temporary contract',
                'contract position', 'contract role', '6 month contract',
                '12 month contract', 'fixed-term', 'temp contract'
            ],
            'temporary': [
                'temporary', 'temp', 'interim', 'temporary position',
                'short term', 'temp role', 'cover position',
                'maternity cover', 'temporary assignment'
            ],
            'internship': [
                'internship', 'intern', 'graduate program', 'traineeship',
                'apprenticeship', 'graduate role', 'junior trainee',
                'student position', 'work experience'
            ]
        }
        
        # Check for each job type pattern
        for job_type, patterns in job_type_patterns.items():
            for pattern in patterns:
                if pattern in combined_text:
                    self.logger.debug(f"Detected job type '{job_type}' from pattern '{pattern}'")
                    return job_type
        
        # Default to full_time if no specific type detected
        return 'full_time'
    
    def truncate_description(self, description):
        """Truncate description to 250-300 characters with smart word boundaries."""
        if not description or len(description) <= 300:
            return description
            
        # Find a good cut point around 250-300 chars to avoid cutting mid-word
        cut_point = 250
        for i in range(250, min(300, len(description))):
            if description[i] in [' ', '.', ',', '!', '?', ';']:
                cut_point = i
                break
        return description[:cut_point] + "..."
    
    def clean_text(self, text):
        """Clean and normalize text data."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common prefixes/suffixes
        text = re.sub(r'^(Job Title:|Company:|Location:)', '', text, flags=re.IGNORECASE)
        
        return text
    
    def parse_salary(self, salary_text):
        """Parse Australian salary information from text."""
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        
        # Australian salary patterns
        patterns = [
            r'\$\s*(\d+(?:,\d+)*)\s*-\s*\$\s*(\d+(?:,\d+)*)',  # $50,000 - $80,000
            r'(\d+(?:,\d+)*)\s*-\s*(\d+(?:,\d+)*)',           # 50,000 - 80,000
            r'\$\s*(\d+(?:,\d+)*)',                            # $50,000
            r'(\d+(?:,\d+)*)'                                  # 50,000
        ]
        
        for pattern in patterns:
            match = re.search(pattern, salary_text)
            if match:
                try:
                    if len(match.groups()) == 2:
                        min_sal = Decimal(match.group(1).replace(',', ''))
                        max_sal = Decimal(match.group(2).replace(',', ''))
                    else:
                        min_sal = max_sal = Decimal(match.group(1).replace(',', ''))
                    
                    # Determine salary type
                    salary_type = 'yearly'
                    if any(word in salary_text.lower() for word in ['hour', 'hr', 'hourly']):
                        salary_type = 'hourly'
                    elif any(word in salary_text.lower() for word in ['month', 'monthly']):
                        salary_type = 'monthly'
                    elif any(word in salary_text.lower() for word in ['week', 'weekly']):
                        salary_type = 'weekly'
                    elif any(word in salary_text.lower() for word in ['day', 'daily']):
                        salary_type = 'daily'
                    
                    return min_sal, max_sal, 'AUD', salary_type
                except:
                    continue
        
        return None, None, 'AUD', 'yearly'
    
    def get_or_create_company(self, company_name):
        """Get or create company object."""
        try:
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'slug': slugify(company_name),
                    'description': f'Company profile for {company_name}',
                    'company_size': 'medium'
                }
            )
            return company
        except Exception as e:
            self.logger.error(f"Error creating company {company_name}: {e}")
            return None
    
    def get_or_create_location(self, location_name):
        """Get or create location object for Australia."""
        try:
            if not location_name or location_name.lower() == 'unknown':
                return None
                
            # Clean location name
            location_name = location_name.strip()
            
            location, created = Location.objects.get_or_create(
                name=location_name,
                defaults={
                    'city': location_name,
                    'country': 'Australia'
                }
            )
            return location
        except Exception as e:
            self.logger.error(f"Error creating location {location_name}: {e}")
            return None
    
    def save_job_to_database_sync(self, job_data):
        """Save job to database (synchronous version for thread execution)."""
        try:
            with transaction.atomic():
                # Check for duplicates by URL
                if job_data.get('external_url'):
                    existing_job = JobPosting.objects.filter(
                        external_url=job_data['external_url']
                    ).first()
                    
                    if existing_job:
                        self.duplicates_found += 1
                        self.logger.debug(f"Duplicate job found (URL): {job_data['title']}")
                        return False
                
                # Alternative duplicate check by title + company
                existing_job = JobPosting.objects.filter(
                    title=job_data['title'],
                    company__name=job_data['company_name'],
                    external_source='scoutjobs.com.au'
                ).first()
                
                if existing_job:
                    self.duplicates_found += 1
                    self.logger.debug(f"Duplicate job found (title+company): {job_data['title']}")
                    return False
                
                # Get or create company
                company = self.get_or_create_company(job_data['company_name'])
                if not company:
                    self.logger.error(f"Failed to create company: {job_data['company_name']}")
                    return False
                
                # Get or create location
                location = self.get_or_create_location(job_data['location'])
                
                # Parse salary information
                salary_min, salary_max, salary_currency, salary_type = self.parse_salary(job_data.get('salary_text', ''))
                
                # Categorize job using your existing service [[memory:6698010]]
                job_category = JobCategorizationService.categorize_job(
                    job_data['title'], 
                    job_data.get('description', '')
                )
                
                # Generate tags using your existing service
                tags_list = JobCategorizationService.get_job_keywords(
                    job_data['title'], 
                    job_data.get('description', '')
                )
                tags_str = ','.join(tags_list[:10])  # Limit to 10 tags
                
                # Create unique slug
                base_slug = slugify(job_data['title'])
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'],
                    slug=unique_slug,
                    description=job_data.get('description', ''),
                    company=company,
                    location=location,
                    posted_by=self.bot_user,
                    job_category=job_category,
                    job_type=job_data.get('job_type', 'full_time'),
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=salary_currency,
                    salary_type=salary_type,
                    salary_raw_text=job_data.get('salary_text', ''),
                    external_source=job_data['external_source'],
                    external_url=job_data.get('external_url', ''),
                    status='active',
                    tags=tags_str,
                    posted_ago=job_data.get('posted_date', ''),
                    additional_info={
                        'scraper_version': 'ScoutJobs-Australia-1.0',
                        'country': job_data['country']
                    }
                )
                
                self.jobs_saved += 1
                location_str = f" - {location.name}" if location else ""
                self.logger.info(f"SAVED: {job_data['title']} at {job_data['company_name']}{location_str}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving job: {e}")
            self.errors_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job to database using thread to avoid async context issues."""
        def run_in_thread():
            return self.save_job_to_database_sync(job_data)
        
        # Use ThreadPoolExecutor to run database operation in separate thread
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_in_thread)
            return future.result()
    
    def scrape_page(self):
        """Scrape a single page of job results (assumes page is already loaded)."""
        try:
            # Find job elements (page should already be loaded by caller)
            job_elements = self.find_job_elements()
            
            if not job_elements:
                self.logger.warning("No job elements found on page")
                return []
            
            self.logger.info(f"Found {len(job_elements)} job elements on page")
            
            # Extract job data in two phases to avoid element staleness
            jobs_data = []
            
            # Phase 1: Extract basic data from all job elements
            for i, element in enumerate(job_elements):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    # Try to refresh the element to avoid staleness
                    try:
                        # Re-find elements to avoid staleness issues
                        fresh_elements = self.page.query_selector_all('div.row-fluid.search-result')
                        if i < len(fresh_elements):
                            element = fresh_elements[i]
                        else:
                            self.logger.warning(f"Element {i+1} not found in refreshed list")
                            continue
                    except Exception as refresh_error:
                        self.logger.debug(f"Could not refresh element {i+1}: {refresh_error}")
                        # Continue with original element
                    
                    job_data = self.extract_job_data(element)
                    
                    if job_data:
                        jobs_data.append(job_data)
                        self.jobs_scraped += 1
                        self.logger.info(f"✅ Extracted job {i+1}: {job_data['title']} at {job_data['company_name']}")
                    else:
                        self.logger.warning(f"❌ Failed to extract job data from element {i+1}")
                    
                    # Human delay between job extractions
                    self.human_delay(0.3, 1.0)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1}: {e}")
                    self.errors_count += 1
                    continue
            
            # Phase 2: Extract full descriptions separately
            self.logger.info(f"Extracting full descriptions for {len(jobs_data)} jobs...")
            for i, job_data in enumerate(jobs_data):
                if job_data.get('external_url'):
                    try:
                        full_description = self.extract_full_job_description(job_data['external_url'])
                        if full_description and len(full_description) > len(job_data.get('description', '')):
                            job_data['description'] = full_description
                            
                            # Extract accurate job type from full description metadata
                            accurate_job_type = self.extract_job_type_from_description(full_description)
                            job_data['job_type'] = accurate_job_type
                            
                            # Extract salary information from full description metadata
                            salary_min, salary_max, salary_currency, salary_type, salary_raw_text = self.extract_salary_from_description(full_description)
                            
                            if salary_min or salary_max:
                                job_data['salary_min'] = salary_min
                                job_data['salary_max'] = salary_max
                                job_data['salary_currency'] = salary_currency
                                job_data['salary_type'] = salary_type
                                job_data['salary_text'] = salary_raw_text
                                
                                self.logger.info(f"📄 Enhanced description for: {job_data['title']} (Job Type: {accurate_job_type}, Salary: {salary_raw_text})")
                            else:
                                self.logger.info(f"📄 Enhanced description for: {job_data['title']} (Job Type: {accurate_job_type})")
                        
                        # Go back to main page after each job detail visit
                        self.page.goto(f"{self.base_url}/jobs", wait_until='domcontentloaded', timeout=30000)
                        self.human_delay(1, 2)
                        
                    except Exception as e:
                        self.logger.debug(f"Could not get full description for {job_data['title']}: {e}")
                        continue
            
            return jobs_data
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {e}")
            self.errors_count += 1
            return []
    
    def run_scraping(self):
        """Main scraping orchestrator for Scout Jobs Australia."""
        start_time = datetime.now()
        
        self.logger.info("Starting Scout Jobs Australia job scraping...")
        self.logger.info(f"Target: {self.job_limit or 'unlimited'} jobs")
        self.logger.info("Scraping from main jobs page: https://scoutjobs.com.au/jobs")
        
        try:
            # Setup browser
            self.setup_browser()
            
            # Scrape multiple pages with pagination
            self.logger.info("\n--- Scraping Scout Jobs with Pagination ---")
            
            page_number = 1
            all_jobs_data = []
            
            while True:
                try:
                    # Check if we've reached the job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    self.logger.info(f"\n--- Scraping Page {page_number} ---")
                    
                    # Navigate to the current page
                    if not self.navigate_to_jobs_page(page_number):
                        self.logger.error(f"Failed to navigate to page {page_number}")
                        break
                    
                    # Get total pages on first page
                    if page_number == 1:
                        total_pages = self.get_total_pages()
                        self.logger.info(f"Total pages detected: {total_pages}")
                    
                    # Scrape current page
                    jobs_data = self.scrape_page()
                    
                    if not jobs_data:
                        self.logger.warning(f"No jobs found on page {page_number}")
                        break
                    
                    # Add to all jobs
                    all_jobs_data.extend(jobs_data)
                    
                    self.logger.info(f"Page {page_number}: Found {len(jobs_data)} jobs")
                    
                    # Check if we should continue to next page
                    if not self.has_next_page():
                        self.logger.info("No more pages available")
                        break
                    
                    # Check if we've reached job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    page_number += 1
                    
                    # Add delay between pages
                    self.human_delay(2, 4)
                    
                except Exception as e:
                    self.logger.error(f"Error scraping page {page_number}: {e}")
                    self.errors_count += 1
                    break
            
            # Save all collected jobs to database
            self.logger.info(f"\n--- Saving {len(all_jobs_data)} jobs to database ---")
            for job_data in all_jobs_data:
                if self.job_limit and self.jobs_saved >= self.job_limit:
                    self.logger.info(f"Reached save limit: {self.job_limit}")
                    break
                
                self.save_job_to_database(job_data)
                
                # Variable delay between saves
                save_delay = random.uniform(0.5, 2.0)
                time.sleep(save_delay)
            
        except Exception as e:
            self.logger.error(f"Error during scraping setup: {e}")
            self.errors_count += 1
        
        finally:
            # Clean up
            self.close_browser()
        
        # Print summary
        self.print_summary(start_time)
        
        return {
            'jobs_scraped': self.jobs_scraped,
            'jobs_saved': self.jobs_saved,
            'duplicates_found': self.duplicates_found,
            'errors_count': self.errors_count,
            'duration': datetime.now() - start_time
        }
    
    def print_summary(self, start_time):
        """Print scraping summary."""
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "="*80)
        print("SCOUT JOBS AUSTRALIA SCRAPING COMPLETED!")
        print("="*80)
        print(f"Duration: {duration}")
        print(f"Jobs scraped: {self.jobs_scraped}")
        print(f"Jobs saved: {self.jobs_saved}")
        print(f"Duplicates skipped: {self.duplicates_found}")
        print(f"Errors encountered: {self.errors_count}")
        
        if self.jobs_scraped > 0:
            success_rate = (self.jobs_saved / self.jobs_scraped) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        # Database statistics - run in thread to avoid async context issues
        def get_db_stats():
            try:
                return JobPosting.objects.filter(external_source='scoutjobs.com.au').count()
            except:
                return "unavailable"
        
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(get_db_stats)
                total_scout_jobs = future.result(timeout=5)
                print(f"Total Scout Jobs Australia jobs in database: {total_scout_jobs}")
        except Exception as e:
            print(f"Total Scout Jobs Australia jobs in database: unavailable")
        
        print("="*80)


def main():
    """Main function."""
    print("Professional Scout Jobs Australia Job Scraper (Playwright)")
    print("="*60)
    print("Advanced job scraper for creative industries")
    print("Specialized for retail, hospitality, advertising, design & media")
    print("Optimized for Australian job market using Playwright")
    print("="*60)
    
    # Parse command line arguments
    job_limit = None
    
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
            print(f"Job limit set to: {job_limit}")
        except ValueError:
            print("Warning: Invalid job limit. Using unlimited.")
    
    # Initialize and run scraper
    scraper = ScoutJobsAustraliaJobScraper(job_limit=job_limit)
    scraper.run_scraping()


if __name__ == "__main__":
    main()
