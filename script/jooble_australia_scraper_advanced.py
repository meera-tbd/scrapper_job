#!/usr/bin/env python3
"""
Professional Jooble Australia Job Scraper using Playwright
==========================================================

Advanced Playwright-based scraper for Jooble Australia (au.jooble.org) that integrates with 
your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian-specific optimization
- No pagination support (as requested)

Usage:
    python jooble_australia_scraper_advanced.py [job_limit]
    
Examples:
    python jooble_australia_scraper_advanced.py 50    # Scrape 50 Australian jobs
    python jooble_australia_scraper_advanced.py       # Scrape all available jobs on first page
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
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from django.utils import timezone
from playwright.sync_api import sync_playwright

# Import your existing models and services
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

User = get_user_model()


class JoobleAustraliaJobScraper:
    """Professional Jooble Australia job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://au.jooble.org"
        self.search_url = "https://au.jooble.org/SearchResult"
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
        
        # Common Australian job search terms for dynamic behavior
        self.search_terms = [
            "software engineer", "data analyst", "marketing manager", "nurse", 
            "teacher", "accountant", "project manager", "business analyst",
            "developer", "consultant", "coordinator", "administrator",
            "customer service", "sales representative", "finance", "hr"
        ]
        
        # Australian locations for search diversity
        self.locations = [
            "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", 
            "Canberra", "Gold Coast", "Newcastle", "Wollongong", "Darwin"
        ]
    
    def setup_logging(self):
        """Setup logging configuration."""
        file_handler = logging.FileHandler('jooble_australia_scraper.log', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user, created = User.objects.get_or_create(
                username='jooble_australia_bot',
                defaults={
                    'email': 'jooble.australia.bot@jobscraper.local',
                    'first_name': 'Jooble Australia',
                    'last_name': 'Scraper Bot',
                    'is_active': False
                }
            )
            if created:
                self.logger.info("Created new bot user for Jooble Australia scraping")
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
        self.logger.info("Setting up Playwright browser for Jooble Australia...")
        
        playwright = sync_playwright().start()
        
        # Browser configuration for anti-detection
        self.browser = playwright.chromium.launch(
            headless=False,  # Visible browser for better success rate
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
                '--disable-blink-features=AutomationControlled'
            ]
        )
        
        # Create context with Australian settings
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
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
    
    def navigate_to_search(self, search_term="", location=""):
        """Navigate to Jooble Australia search page."""
        try:
            # Start with base search URL - simpler approach
            search_url = self.search_url
            
            self.logger.info(f"Navigating to: {search_url}")
            
            # Navigate to URL with more lenient settings
            self.page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for page to load completely
            self.page.wait_for_selector('body', timeout=10000)
            self.human_delay(2, 4)
            
            # If we have search parameters, use the search form instead of URL params
            if search_term:
                try:
                    # Look for search input field
                    search_input_selectors = [
                        'input[name="q"]',
                        'input[id="q"]', 
                        'input[placeholder*="job"]',
                        'input[placeholder*="keyword"]',
                        'input[type="search"]',
                        '.search-input input',
                        '#search-input'
                    ]
                    
                    search_input = None
                    for selector in search_input_selectors:
                        search_input = self.page.query_selector(selector)
                        if search_input:
                            break
                    
                    if search_input:
                        self.logger.info(f"Found search input, entering: {search_term}")
                        search_input.clear()
                        search_input.type(search_term, delay=100)
                        self.human_delay(1, 2)
                        
                        # Look for location input
                        if location:
                            location_input_selectors = [
                                'input[name="l"]',
                                'input[id="l"]',
                                'input[placeholder*="location"]',
                                'input[placeholder*="city"]',
                                '.location-input input'
                            ]
                            
                            location_input = None
                            for selector in location_input_selectors:
                                location_input = self.page.query_selector(selector)
                                if location_input:
                                    break
                            
                            if location_input:
                                self.logger.info(f"Found location input, entering: {location}")
                                location_input.clear()
                                location_input.type(location, delay=100)
                                self.human_delay(1, 2)
                        
                        # Submit the search
                        submit_selectors = [
                            'button[type="submit"]',
                            'input[type="submit"]',
                            '.search-button',
                            'button:has-text("Search")',
                            'button:has-text("Find")'
                        ]
                        
                        submit_button = None
                        for selector in submit_selectors:
                            submit_button = self.page.query_selector(selector)
                            if submit_button:
                                break
                        
                        if submit_button:
                            self.logger.info("Submitting search form")
                            submit_button.click()
                            self.page.wait_for_load_state('domcontentloaded', timeout=15000)
                            self.human_delay(3, 5)
                        else:
                            # Try pressing Enter
                            search_input.press('Enter')
                            self.page.wait_for_load_state('domcontentloaded', timeout=15000)
                            self.human_delay(3, 5)
                    else:
                        self.logger.info("No search input found, proceeding with default page")
                        
                except Exception as e:
                    self.logger.warning(f"Error using search form: {e}, proceeding with current page")
            
            # Handle cookie consent if present
            try:
                cookie_selectors = [
                    'button:has-text("Accept")',
                    'button:has-text("Accept all")', 
                    'button:has-text("OK")',
                    '[data-testid="cookie-accept"]',
                    '.cookie-accept',
                    '#cookie-accept'
                ]
                
                for selector in cookie_selectors:
                    cookie_button = self.page.query_selector(selector)
                    if cookie_button:
                        cookie_button.click()
                        self.logger.info("Accepted cookies")
                        self.human_delay(1, 2)
                        break
            except:
                pass
            
            # Random scroll to trigger lazy loading
            if random.choice([True, False]):
                self.page.mouse.wheel(0, random.randint(200, 500))
                self.human_delay(1, 2)
            
            self.logger.info("Successfully loaded search page")
            return True
            
        except Exception as e:
            self.logger.error(f"Error navigating to search: {e}")
            return False
    
    def find_job_elements(self):
        """Find job card elements on the current page."""
        self.logger.info("Searching for job elements...")
        
        # Specific selectors for Jooble Australia based on actual HTML structure
        selectors = [
            # Primary Jooble job card selector
            'li div[data-test-name="_jobCard"]',
            '[data-test-name="_jobCard"]',
            # Fallback selectors
            "li div[id]",  # Job cards have unique IDs
            ".job-item", 
            ".job-card",
            ".job-listing",
            ".result",
            ".search-result",
            "article",
            "div[class*='job']",
            "li[class*='job']",
            ".listing",
            ".card",
            "[class*='card']",
            # Last resort
            ".vacancy",
            ".position"
        ]
        
        for selector in selectors:
            try:
                self.logger.info(f"Trying selector: {selector}")
                elements = self.page.query_selector_all(selector)
                if elements and len(elements) > 2:  # Need at least 3 for valid results
                    self.logger.info(f"Found {len(elements)} elements using selector: {selector}")
                    return elements
                else:
                    self.logger.debug(f"Found {len(elements) if elements else 0} elements with selector: {selector}")
            except Exception as e:
                self.logger.error(f"Error with selector {selector}: {e}")
                continue
        
        self.logger.warning("No job elements found with any selector")
        return []
    
    def extract_job_data(self, job_element):
        """Extract job data from a single job element using Jooble-specific selectors."""
        try:
            # Debug: Log the element to understand its structure
            element_text = job_element.text_content()[:100] if job_element.text_content() else "No text"
            self.logger.debug(f"Processing element with text: {element_text}...")
            
            # Extract job title using Jooble-specific selectors
            title = self.extract_text_by_selectors(job_element, [
                'h2.jA9gFS.dUatPc a',  # Specific Jooble title selector
                'h2 a.job_card_link',  # Job card link in h2
                'a.job_card_link',     # Direct job card link
                'h2 a',                # Any h2 link
                'h1 a', 'h3 a',        # Other heading links
                '.job-title a',
                'a[href*="/away/"]'    # Jooble away links
            ])
            
            # Extract company name using Jooble-specific selectors
            company = self.extract_text_by_selectors(job_element, [
                'p.z6WlhX[data-test-name="_companyName"]',  # Specific Jooble company selector
                'p[data-test-name="_companyName"]',         # Company name test selector
                '.z6WlhX',                                  # Company class
                '.E6E0jY .pXyhD4 p',                       # Company in specific container
                '.company-name',
                '.company'
            ])
            
            # Extract location using Jooble-specific selectors
            job_location = self.extract_text_by_selectors(job_element, [
                '.blapLw .caption.NTRJBV',                  # Specific location selector (with map marker)
                '.blapLw .caption:first-child',             # First caption in location section
                '.NTRJBV',                                  # Location class
                '.blapLw:has(svg[xlink\\:href*="map_marker"]) .caption',  # Location with map marker
                '[class*="location"] .caption',
                '.location',
                '.job-location'
            ])
            
            # Extract salary using Jooble-specific selectors
            salary = self.extract_text_by_selectors(job_element, [
                'p.b97WnG',                                 # Specific Jooble salary selector
                '.QZH8mt p:first-child',                    # First paragraph in job details
                '.salary',
                'p:has-text("$")',                          # Any paragraph containing $
                '[class*="salary"]'
            ])
            
            # Extract job URL using Jooble-specific selectors
            url = ""
            try:
                link_selectors = [
                    'a.job_card_link',                      # Specific Jooble job link
                    'h2 a',                                 # Title link
                    'a[href*="/away/"]',                    # Jooble away links
                    'a[href*="jooble.org"]'                 # Any Jooble link
                ]
                
                for selector in link_selectors:
                    link_element = job_element.query_selector(selector)
                    if link_element:
                        href = link_element.get_attribute("href")
                        if href:
                            if href.startswith("http"):
                                url = href
                            else:
                                url = urljoin(self.base_url, href)
                            break
            except Exception as e:
                self.logger.debug(f"Error extracting URL: {e}")
            
            # Extract basic description using comprehensive Jooble selectors
            # Try to get the full description content from the job card itself
            basic_description = self.extract_text_by_selectors(job_element, [
                '.GEyos4.e9eiOZ',                          # Specific Jooble description selector
                '.QZH8mt .GEyos4',                         # Description in job details
                '.QZH8mt',                                 # Full job details container
                '.job-description',
                '.description',
                '.snippet'
            ])
            
            # If basic description is short, try to get more content from the job card
            if not basic_description or len(basic_description) < 100:
                # Try to extract from the entire job element for more complete content
                try:
                    full_element_text = job_element.inner_text()
                    # Extract everything after the title/company but exclude salary/location
                    lines = full_element_text.split('\n')
                    description_lines = []
                    skip_next = False
                    
                    for line in lines:
                        line = line.strip()
                        # Skip empty lines, titles, companies, locations, salaries
                        if (not line or 
                            any(skip_word in line.lower() for skip_word in [title.lower(), company.lower()]) or
                            line.startswith('$') or 
                            any(loc_word in line.lower() for loc_word in ['melbourne', 'sydney', 'brisbane', 'adelaide', 'perth', 'nsw', 'vic', 'qld', 'sa', 'wa']) or
                            len(line) < 10):
                            continue
                            
                        # Look for description content
                        if (len(line) > 20 and 
                            any(desc_word in line.lower() for desc_word in 
                                ['looking for', 'seeking', 'position', 'role', 'responsibilities', 
                                 'duties', 'requirements', 'experience', 'skills', 'we are', 'about'])):
                            description_lines.append(line)
                    
                    if description_lines:
                        enhanced_description = '\n'.join(description_lines)
                        if len(enhanced_description) > len(basic_description):
                            basic_description = enhanced_description
                            self.logger.debug(f"Enhanced basic description from job card ({len(basic_description)} chars)")
                
                except Exception as e:
                    self.logger.debug(f"Error enhancing basic description: {e}")
            
            # Use basic description (which contains the actual job content from listing cards)
            # Skip detail page extraction due to Cloudflare protection on Jooble
            description = basic_description
            
            # Try to extract full description from detail pages only if basic description seems truncated
            if url and basic_description and (basic_description.endswith('...') or len(basic_description) < 200):
                try:
                    self.logger.debug(f"Basic description seems truncated ({len(basic_description)} chars), attempting detail page extraction")
                    full_description = self.extract_full_job_description(url)
                    
                    # Check if we got actual job content (not security challenge)
                    if (full_description and 
                        'verify you are human' not in full_description.lower() and
                        'security' not in full_description.lower() and
                        len(full_description) > len(basic_description)):
                        
                        description = full_description
                        self.logger.info(f"✅ SUCCESS: Enhanced with FULL description ({len(full_description)} chars) vs basic ({len(basic_description)} chars)")
                    else:
                        self.logger.debug(f"⚠️ Detail page blocked by security - using basic description ({len(basic_description)} chars)")
                        
                except Exception as e:
                    self.logger.debug(f"Detail page extraction failed: {e}")
            else:
                self.logger.debug(f"Using basic description from job card ({len(basic_description)} chars)")
            
            # Extract job type from tags
            job_type_text = self.extract_text_by_selectors(job_element, [
                '[data-name="full_time"]',                  # Specific job type tag
                '[data-name="part_time"]',
                '[data-name="contract"]',
                '.K8ZLnh.tag',                              # Job tags
                '[data-test-name="_jobTag"]',               # Job tag test selector
                '.job-type'
            ])
            
            # Extract posting date using Jooble-specific selectors
            date_text = self.extract_text_by_selectors(job_element, [
                '.blapLw:has(svg[xlink\\:href*="clock"]) .caption',  # Time with clock icon
                '.caption.Vk-5Da',                          # Time caption class
                '.blapLw .caption:last-child',              # Last caption (usually time)
                '.date',
                '.time'
            ])
            
            # Debug logging
            self.logger.debug(f"Raw extracted - Title: '{title}', Company: '{company}', Location: '{job_location}', Salary: '{salary}'")
            
            # Clean and validate data with proper length restrictions
            title = self.clean_text(title)[:200] if title else ""                    # CharField(max_length=200)
            company = self.clean_text(company)[:100] if company else ""              # Reasonable company name length
            job_location = self.clean_text(job_location)[:100] if job_location else ""  # Reasonable location length
            salary = self.clean_text(salary)[:200] if salary else ""                 # CharField(max_length=200) for salary_raw_text
            description = self.clean_description_text(description) if description else ""  # TextField - handled in clean method (max 5000 chars)
            date_text = self.clean_text(date_text)[:50] if date_text else ""         # CharField(max_length=50) for posted_ago
            job_type_text = self.clean_text(job_type_text)[:50] if job_type_text else ""  # Reasonable job type length
            
            # Skip if no essential data found
            if not title:
                self.logger.warning("Skipping element - no job title found")
                return None
            
            # Use fallback for company if missing
            if not company:
                company = "Unknown Company"
            
            # Use fallback for location if missing
            if not job_location:
                job_location = "Australia"
            
            # Detect job type from available information
            detected_job_type = self.detect_job_type(job_element, title, description, job_type_text)
            
            # Final debug logging
            self.logger.debug(f"Final extracted - Title: '{title}', Company: '{company}', Location: '{job_location}', Salary: '{salary}'")
            
            return {
                'title': title,
                'company_name': company,
                'location': job_location,
                'description': description,
                'external_url': url,
                'salary_text': salary,
                'job_type': detected_job_type,
                'external_source': 'jooble.org.au',
                'posted_ago': date_text,
                'date_posted': self.parse_date(date_text),
                'country': 'Australia'
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    
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
    
    def extract_full_job_description(self, job_url):
        """Extract full job description from individual job detail page."""
        if not job_url:
            return ""
        
        try:
            self.logger.debug(f"Fetching full description from: {job_url[:80]}...")
            
            # Create a new page context to avoid conflicts
            detail_page = self.context.new_page()
            
            # Navigate to job detail page with more aggressive settings
            detail_page.goto(job_url, wait_until='domcontentloaded', timeout=30000)
            self.human_delay(3, 6)  # Give much more time for content to load
            
            # Wait for content to fully load
            try:
                detail_page.wait_for_load_state('networkidle', timeout=15000)
            except:
                pass
            
            # Multiple scroll attempts to trigger lazy loading
            for _ in range(3):
                detail_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                self.human_delay(1, 2)
                detail_page.evaluate("window.scrollTo(0, 0)")
                self.human_delay(1, 2)
            
            # Try to click any "show more" or "expand" buttons
            try:
                expand_buttons = detail_page.query_selector_all('button, a, span')
                for btn in expand_buttons[:5]:  # Check first 5 elements
                    btn_text = btn.inner_text().lower()
                    if any(word in btn_text for word in ['show more', 'read more', 'expand', 'view more', 'see more']):
                        btn.click()
                        self.human_delay(1, 2)
                        self.logger.debug(f"Clicked expand button: {btn_text}")
                        break
            except:
                pass
            
            # Try to find the full description using comprehensive selectors
            description_selectors = [
                # Your specific selectors from the HTML structure
                '.col.fw-light.font_3.text-align-justify.text-break',  # Exact selector from your HTML
                '.row.mb-3.mt-2 .col.fw-light.font_3.text-align-justify.text-break',  # Full path
                '.row .col.fw-light',                                   # Shorter path
                '.mb-3.mt-2 .col',                                      # Container with description
                
                # Generic job description selectors
                '.job-description',
                '.description',
                '.job-content',
                '.job-details',
                '.content',
                
                # Content area selectors
                '.container .row .col',                                 # Bootstrap container structure
                '.container-fluid .col',
                'main .col',
                'main .container .col',
                
                # Fallback selectors for different layouts
                '.job-posting-content',
                '.vacancy-description',
                '.position-details',
                '[class*="description"]',
                '[class*="content"]',
                '[class*="details"]',
                
                # Broad selectors
                'main',
                '.main',
                '.container',
                'article'
            ]
            
            full_description = ""
            
            for selector in description_selectors:
                try:
                    desc_element = detail_page.query_selector(selector)
                    if desc_element:
                        desc_text = desc_element.inner_text().strip()
                        if desc_text and len(desc_text) > 100:  # Lowered threshold to catch more content
                            full_description = desc_text
                            self.logger.debug(f"Found description using selector: {selector} ({len(desc_text)} chars)")
                            break
                except Exception as e:
                    self.logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            # If still no description found, try a comprehensive content extraction approach
            if not full_description:
                try:
                    # Wait even more for dynamic content
                    detail_page.wait_for_timeout(5000)
                    
                    # Log the page title and URL to verify we're on the right page
                    page_title = detail_page.title()
                    current_url = detail_page.url
                    self.logger.debug(f"Job detail page - Title: {page_title[:100]}")
                    self.logger.debug(f"Current URL: {current_url[:100]}")
                    
                    # Try to extract the entire page content and find job-related text
                    page_content = detail_page.content()
                    self.logger.debug(f"Page content length: {len(page_content)} characters")
                    
                    # Get all text elements and analyze them comprehensively
                    all_elements = detail_page.query_selector_all('*')
                    candidates = []
                    
                    for element in all_elements:
                        try:
                            text = element.inner_text().strip()
                            # Look for text that contains job description keywords
                            if (text and len(text) > 150 and 
                                any(keyword in text.lower() for keyword in 
                                    ['duties and responsibilities', 'responsibilities', 'duties', 'requirements', 
                                     'experience', 'skills', 'position', 'role', 'candidate', 'about the role', 
                                     'job description', 'qualifications', 'working', 'manoeuvring', 'loading',
                                     'unloading', 'vehicles', 'lifting', 'tipping', 'observing safety',
                                     'quality checks', 'mygration', 'pty ltd', 'recruitment', 'based in', 
                                     'full-time', 'part-time', 'what you will', 'you will be', 'key responsibilities'])):
                                
                                # Additional check for detailed content
                                if ('•' in text or '*' in text or 
                                    text.count('.') > 3 or 
                                    any(detailed_word in text.lower() for detailed_word in 
                                        ['manoeuvring vehicles', 'loading and unloading', 'safety requirements',
                                         'quality checks', 'immediate start', 'salary range', 'closure date'])):
                                    candidates.append(text)
                                    self.logger.debug(f"Found detailed candidate ({len(text)} chars): {text[:150]}...")
                                
                        except:
                            continue
                    
                    # Pick the best candidate - prioritize longest with bullet points or detailed content
                    if candidates:
                        # Sort by length and content quality
                        candidates.sort(key=lambda x: len(x), reverse=True)
                        
                        # Prefer descriptions with bullet points or structured content
                        for candidate in candidates:
                            if ('•' in candidate and len(candidate) > 300) or \
                               ('Duties and responsibilities' in candidate) or \
                               (candidate.count('.') > 5 and len(candidate) > 400):
                                full_description = candidate
                                self.logger.info(f"🎯 Found DETAILED job description ({len(full_description)} chars)")
                                break
                        
                        # If no structured content found, use the longest candidate
                        if not full_description and candidates:
                            full_description = candidates[0]
                            self.logger.debug(f"Using longest candidate description ({len(full_description)} chars)")
                        
                except Exception as e:
                    self.logger.error(f"Comprehensive content analysis failed: {e}")
            
            # Final fallback - get the largest text block on the page
            if not full_description:
                try:
                    all_text_elements = detail_page.query_selector_all('div, p, section')
                    largest_text = ""
                    for element in all_text_elements:
                        try:
                            text = element.inner_text().strip()
                            if len(text) > len(largest_text) and len(text) > 200:
                                largest_text = text
                        except:
                            continue
                    
                    if largest_text:
                        full_description = largest_text
                        self.logger.debug(f"Using largest text block as fallback ({len(largest_text)} chars)")
                except Exception as e:
                    self.logger.debug(f"Fallback extraction failed: {e}")
            
            # Clean up the description
            if full_description:
                full_description = self.clean_description_text(full_description)
            
            # Close the detail page
            detail_page.close()
            
            return full_description
            
        except Exception as e:
            self.logger.error(f"Error extracting full description from {job_url}: {e}")
            try:
                detail_page.close()
            except:
                pass
            return ""
    
    def clean_description_text(self, text):
        """Clean and format job description text while preserving ALL content."""
        if not text:
            return ""
        
        # Clean up whitespace but preserve paragraph structure and bullet points
        text = re.sub(r'\r\n|\r|\n', '\n', text)  # Normalize line endings
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)  # Remove excessive blank lines but keep paragraphs
        text = re.sub(r'[ \t]+', ' ', text)       # Normalize spaces/tabs
        text = text.strip()
        
        # Remove only specific navigation/UI elements but be VERY conservative
        ui_elements_to_remove = [
            r'^Apply now\s*$',
            r'^Apply for this job\s*$', 
            r'^Back to search\s*$',
            r'^Save job\s*$',
            r'^Share\s*$',
            r'^Print\s*$',
            r'^Report\s*$',
            r'^Terms of use\s*$',
            r'^Privacy policy\s*$',
            r'^Cookie policy\s*$',
            r'^Contact us\s*$'
        ]
        
        for pattern in ui_elements_to_remove:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
        
        # DO NOT truncate - preserve the full description no matter the length
        # This ensures we get complete job descriptions with all duties and responsibilities
        
        return text.strip()
    
    def detect_job_type(self, job_element, job_title="", job_description="", job_type_text=""):
        """Detect job type from job listing based on text indicators."""
        
        # Collect all text from the job element
        try:
            element_text = job_element.text_content().lower()
        except:
            element_text = ""
        
        # Combine all available text for analysis
        combined_text = f"{job_title} {job_description} {job_type_text} {element_text}".lower()
        
        # Define job type patterns
        job_type_patterns = {
            'casual': [
                'casual', 'casual position', 'casual role', 'casual work',
                'ad hoc', 'as needed', 'on call', 'when required'
            ],
            'part_time': [
                'part time', 'part-time', 'parttime', 'part time position',
                'hours per week', '20 hours', '25 hours', '30 hours'
            ],
            'contract': [
                'contract', 'contractor', 'fixed term', 'temporary contract',
                'contract position', 'contract role', 'fixed-term'
            ],
            'temporary': [
                'temporary', 'temp', 'interim', 'temporary position',
                'short term', 'temp role', 'cover position'
            ],
            'internship': [
                'internship', 'intern', 'graduate program', 'traineeship',
                'apprenticeship', 'graduate role', 'student position'
            ],
            'freelance': [
                'freelance', 'freelancer', 'independent contractor',
                'self employed', 'consultant', 'project based'
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
    
    def parse_date(self, date_text):
        """Parse relative date strings into datetime objects."""
        if not date_text:
            return timezone.now()
            
        date_text = date_text.lower().strip()
        now = timezone.now()
        
        # Handle "today" and "yesterday"
        if 'today' in date_text or 'just posted' in date_text:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        elif 'yesterday' in date_text:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Extract number and unit from strings like "2 days ago"
        match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_text)
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
        
        return now.replace(hour=9, minute=0, second=0, microsecond=0)
    
    def clean_text(self, text):
        """Clean and normalize text data."""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common prefixes/suffixes
        text = re.sub(r'^(Job Title:|Company:|Location:)', '', text, flags=re.IGNORECASE)
        
        return text
    
    def parse_location(self, location_string):
        """Parse location string into normalized location data."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Australian state abbreviations
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
        
        # Split by comma first
        parts = [part.strip() for part in location_string.split(',')]
        
        city = ""
        state = ""
        country = "Australia"
        
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
            # Close any existing connections to ensure fresh connection
            connections.close_all()
            
            with transaction.atomic():
                # Check for duplicates by URL (only if URL exists)
                if job_data.get('external_url') and job_data['external_url'].strip():
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
                    external_source='jooble.org.au'
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
                
                # Parse and get or create location
                location_name, city, state, country = self.parse_location(job_data['location'])
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
                
                # Parse salary information
                salary_min, salary_max, salary_currency, salary_type = self.parse_salary(job_data.get('salary_text', ''))
                
                # Categorize job using your existing service
                job_category = JobCategorizationService.categorize_job(
                    job_data['title'], 
                    job_data.get('description', '')
                )
                
                # Create unique slug with timestamp for uniqueness when no URL
                import time
                base_slug = slugify(job_data['title'])[:200]  # Limit base slug length
                if not job_data.get('external_url') or not job_data.get('external_url').strip():
                    # Add timestamp suffix for jobs without URLs to ensure uniqueness
                    timestamp_suffix = str(int(time.time()))[-6:]  # Last 6 digits of timestamp
                    unique_slug = f"{base_slug}-{timestamp_suffix}"[:250]  # Ensure slug is not too long
                else:
                    unique_slug = base_slug[:250]  # Ensure slug is not too long
                    
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"[:250]  # Ensure slug is not too long
                    counter += 1
                
                # Create job posting with proper field length handling
                # Debug: Log field lengths to identify the issue
                debug_external_url = job_data.get('external_url') if job_data.get('external_url') and job_data.get('external_url').strip() else f"https://au.jooble.org/job-{unique_slug}"
                self.logger.debug(f"Field lengths - Title: {len(job_data['title'])}, External URL: {len(debug_external_url)}, Slug: {len(unique_slug)}")
                
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200] if job_data['title'] else '',  # CharField(200)
                    slug=unique_slug,
                    description=job_data.get('description', ''),  # TextField - no limit
                    company=company,
                    location=location_obj,
                    posted_by=self.bot_user,
                    job_category=job_category,
                    job_type=job_data.get('job_type', 'full_time'),
                    experience_level=job_data.get('experience_level', '')[:100] if job_data.get('experience_level') else '',  # CharField(100)
                    work_mode=job_data.get('work_mode', '')[:50] if job_data.get('work_mode') else '',  # CharField(50)
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=salary_currency,
                    salary_type=salary_type,
                    salary_raw_text=job_data.get('salary_text', '')[:200] if job_data.get('salary_text') else '',  # CharField(200)
                    external_source=job_data['external_source'][:100] if job_data['external_source'] else 'jooble.org.au',  # CharField(100)
                    external_url=(job_data.get('external_url') if job_data.get('external_url') and job_data.get('external_url').strip() else f"https://au.jooble.org/job-{unique_slug}")[:200],  # Ensure URL is not too long
                    external_id=job_data.get('external_id', '')[:100] if job_data.get('external_id') else '',  # CharField(100)
                    status='active',
                    posted_ago=job_data.get('posted_ago', '')[:50] if job_data.get('posted_ago') else '',  # CharField(50)
                    date_posted=job_data.get('date_posted'),
                    tags=job_data.get('tags', ''),  # TextField - no limit
                    additional_info={
                        'external_source': job_data['external_source'],
                        'posted_ago': job_data.get('posted_ago', ''),
                        'country': job_data['country'],
                        'scraper_version': 'Jooble-Australia-Playwright-1.0'
                    }
                )
                
                self.jobs_saved += 1
                location_str = f" - {location_obj.name}" if location_obj else ""
                self.logger.info(f"SAVED: {job_data['title']} at {job_data['company_name']}{location_str}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving job: {e}")
            self.errors_count += 1
            return False
    
    def save_job_to_database(self, job_data):
        """Save job to database using thread to avoid async context issues."""
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.save_job_to_database_sync, job_data)
            try:
                return future.result(timeout=30)
            except concurrent.futures.TimeoutError:
                self.logger.error("Database save operation timed out")
                self.errors_count += 1
                return False
    
    def scrape_page(self):
        """Scrape jobs from the current page (no pagination)."""
        try:
            # Find job elements
            job_elements = self.find_job_elements()
            
            if not job_elements:
                self.logger.warning("No job elements found on page")
                return []
            
            self.logger.info(f"Found {len(job_elements)} job elements on page")
            
            # Extract job data
            jobs_data = []
            for i, element in enumerate(job_elements):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    job_data = self.extract_job_data(element)
                    
                    if job_data:
                        jobs_data.append(job_data)
                        self.jobs_scraped += 1
                        self.logger.info(f"Extracted job {i+1}: {job_data['title']} at {job_data['company_name']}")
                    else:
                        self.logger.debug(f"Skipped invalid job {i+1}")
                    
                    # Human delay between job extractions
                    self.human_delay(0.5, 1.5)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1}: {e}")
                    self.errors_count += 1
                    continue
            
            return jobs_data
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {e}")
            self.errors_count += 1
            return []
    
    def run_scraping(self):
        """Main scraping orchestrator for Jooble Australia (single page, no pagination)."""
        start_time = datetime.now()
        
        self.logger.info("Starting Jooble Australia job scraping...")
        self.logger.info(f"Target: {self.job_limit or 'unlimited'} jobs from first page only")
        self.logger.info("Note: Pagination NOT implemented as per request")
        
        try:
            # Setup browser
            self.setup_browser()
            
            # Start with simple approach - no search terms to avoid complexity
            search_term = ""
            location = ""
            
            self.logger.info("Starting with basic job search (no specific terms for better compatibility)")
            
            # Navigate to search page
            if not self.navigate_to_search(search_term, location):
                self.logger.error("Failed to navigate to search page")
                return
            
            # Scrape jobs from the current page
            jobs_data = self.scrape_page()
            
            # Save jobs to database
            for job_data in jobs_data:
                if self.job_limit and self.jobs_saved >= self.job_limit:
                    self.logger.info(f"Reached save limit: {self.job_limit}")
                    break
                
                self.save_job_to_database(job_data)
                
                # Variable delay between saves
                save_delay = random.uniform(0.5, 2.0)
                time.sleep(save_delay)
            
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
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
        print("JOOBLE AUSTRALIA SCRAPING COMPLETED!")
        print("="*80)
        print(f"Duration: {duration}")
        print(f"Jobs scraped: {self.jobs_scraped}")
        print(f"Jobs saved: {self.jobs_saved}")
        print(f"Duplicates skipped: {self.duplicates_found}")
        print(f"Errors encountered: {self.errors_count}")
        
        if self.jobs_scraped > 0:
            success_rate = (self.jobs_saved / self.jobs_scraped) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        # Database statistics
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(lambda: JobPosting.objects.filter(external_source='jooble.org.au').count())
                total_jooble_jobs = future.result(timeout=10)
                print(f"Total Jooble Australia jobs in database: {total_jooble_jobs}")
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
        
        print("="*80)
        print("Note: This scraper only processes the first page (pagination not implemented)")
        print("="*80)


def main():
    """Main function."""
    print("Professional Jooble Australia Job Scraper (Playwright)")
    print("="*60)
    print("Advanced job scraper with professional database structure")
    print("Optimized for Australian job market using Playwright")
    print("NOTE: Single page scraping only (no pagination)")
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
    scraper = JoobleAustraliaJobScraper(job_limit=job_limit)
    scraper.run_scraping()


if __name__ == "__main__":
    main()
