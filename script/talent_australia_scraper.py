#!/usr/bin/env python3
"""
Professional Talent.com Australia Job Scraper using Playwright
==============================================================

Advanced Playwright-based scraper for Talent.com Australia that integrates with 
your existing seek_scraper_project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection with pagination support
- Comprehensive error handling and logging
- Australian-specific optimization
- Multi-page pagination support for thorough job collection

Usage:
    python talent_australia_scraper.py [job_limit]
    
Examples:
    python talent_australia_scraper.py 20    # Scrape 20 Australian jobs
    python talent_australia_scraper.py       # Scrape all available jobs
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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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


class TalentAustraliaJobScraper:
    """Professional Talent.com Australia job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://au.talent.com"
        self.job_limit = job_limit
        self.jobs_scraped = 0
        self.jobs_saved = 0
        self.duplicates_found = 0
        self.errors_count = 0
        self.pages_scraped = 0
        
        # Browser instances
        self.browser = None
        self.context = None
        self.page = None
        
        # Setup logging
        self.setup_logging()
        
        # Get or create bot user
        self.bot_user = self.get_or_create_bot_user()
        
        # Dynamic search term generation - more human-like
        self.base_roles = ["manager", "assistant", "coordinator", "specialist", "developer", "analyst", "engineer", "consultant", "advisor", "executive", "supervisor", "officer"]
        self.tech_prefixes = ["software", "web", "mobile", "cloud", "data", "ai", "machine learning", "cyber security", "network", "database", "digital", "it"]
        self.business_prefixes = ["business", "financial", "marketing", "project", "product", "operations", "sales", "hr", "customer", "office", "admin"]
        self.general_prefixes = ["senior", "junior", "lead", "principal", "head of", "assistant", "graduate", "trainee", "entry level"]
        
        # Dynamic Australian location generation - includes suburbs and regions
        self.major_cities = ["sydney", "melbourne", "brisbane", "perth", "adelaide", "canberra", "darwin", "hobart"]
        self.sydney_areas = ["sydney", "parramatta", "chatswood", "manly", "bondi", "surry hills", "pyrmont", "north sydney", "bankstown", "penrith"]
        self.melbourne_areas = ["melbourne", "richmond", "fitzroy", "st kilda", "south yarra", "carlton", "brunswick", "collingwood", "geelong", "ballarat"]
        self.brisbane_areas = ["brisbane", "south bank", "fortitude valley", "new farm", "paddington", "west end", "kangaroo point", "gold coast", "sunshine coast"]
        self.perth_areas = ["perth", "fremantle", "subiaco", "nedlands", "cottesloe", "northbridge", "leederville", "joondalup", "rockingham"]
        
        # Used search combinations tracking for anti-detection
        self.used_combinations = set()
        self.last_search_time = None
    
    def setup_logging(self):
        """Setup logging configuration."""
        # Configure logging with UTF-8 encoding for compatibility
        file_handler = logging.FileHandler('talent_australia_scraper.log', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Set up formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  # Back to INFO level
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user, created = User.objects.get_or_create(
                username='talent_australia_bot',
                defaults={
                    'email': 'talent.australia.bot@jobscraper.local',
                    'first_name': 'Talent Australia',
                    'last_name': 'Scraper Bot'
                }
            )
            if created:
                self.logger.info("Created new bot user for Talent Australia scraping")
            return user
        except Exception as e:
            self.logger.error(f"Error creating bot user: {e}")
            return None
    
    def human_delay(self, min_delay=2, max_delay=5):
        """Add human-like delays to avoid detection."""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def generate_dynamic_search_term(self):
        """Generate a human-like, dynamic search term that varies each time."""
        
        # Different patterns of search term generation
        patterns = [
            # Simple role searches (most human-like)
            lambda: random.choice(self.base_roles),
            
            # Tech role combinations
            lambda: f"{random.choice(self.tech_prefixes)} {random.choice(self.base_roles)}",
            
            # Business role combinations  
            lambda: f"{random.choice(self.business_prefixes)} {random.choice(self.base_roles)}",
            
            # Seniority + role combinations
            lambda: f"{random.choice(self.general_prefixes)} {random.choice(self.base_roles)}",
            
            # Popular job titles that real people search for
            lambda: random.choice([
                "software engineer", "data scientist", "business analyst", "project manager",
                "marketing coordinator", "sales representative", "customer service", "accountant",
                "nurse", "teacher", "graphic designer", "web developer", "admin assistant",
                "finance manager", "hr advisor", "operations coordinator", "support specialist",
                "remote", "part time", "full time", "contract", "casual", "graduate"
            ]),
            
            # Sometimes use skills-based searches (like real people do)
            lambda: random.choice([
                "python", "javascript", "react", "aws", "sql", "excel", "salesforce",
                "adobe", "marketing", "accounting", "nursing", "teaching", "design",
                "management", "leadership", "communication", "analysis"
            ]),
            
            # Industry-based searches
            lambda: random.choice([
                "healthcare", "education", "finance", "technology",
                "retail", "hospitality", "construction", "government",
                "engineering", "consulting", "media", "transport"
            ])
        ]
        
        # Select a random pattern and generate term
        pattern = random.choice(patterns)
        search_term = pattern().lower().strip()
        
        # Occasionally add location-specific modifiers (like real job seekers)
        if random.random() < 0.15:  # 15% chance
            modifiers = ["remote", "work from home", "part time", "full time", "contract", "casual"]
            search_term += f" {random.choice(modifiers)}"
        
        return search_term
    
    def generate_dynamic_location(self):
        """Generate a human-like, dynamic location that includes suburbs and regions."""
        
        location_strategies = [
            # Major cities (most common)
            lambda: random.choice(self.major_cities),
            
            # Sydney areas
            lambda: random.choice(self.sydney_areas) if random.random() < 0.3 else "sydney",
            
            # Melbourne areas  
            lambda: random.choice(self.melbourne_areas) if random.random() < 0.3 else "melbourne",
            
            # Brisbane areas
            lambda: random.choice(self.brisbane_areas) if random.random() < 0.25 else "brisbane",
            
            # Perth areas
            lambda: random.choice(self.perth_areas) if random.random() < 0.25 else "perth",
            
            # Sometimes search without location (like real people do)
            lambda: "" if random.random() < 0.2 else random.choice(self.major_cities),
            
            # State-based searches
            lambda: random.choice(["NSW", "VIC", "QLD", "WA", "SA", "ACT", "NT", "TAS"]),
            
            # Regional areas (less common but more human-like)
            lambda: random.choice([
                "Gold Coast", "Sunshine Coast", "Newcastle", "Wollongong", "Geelong",
                "Ballarat", "Bendigo", "Albury", "Wagga Wagga", "Cairns", "Townsville",
                "Mackay", "Rockhampton", "Toowoomba", "Bunbury", "Albany", "Geraldton"
            ])
        ]
        
        strategy = random.choice(location_strategies)
        return strategy()
    
    def get_intelligent_search_combination(self):
        """Get an intelligent search combination that avoids repetition and appears human."""
        
        max_attempts = 50  # Prevent infinite loops
        attempts = 0
        
        while attempts < max_attempts:
            # Mix of location-only searches and term+location searches
            if random.random() < 0.4:  # 40% chance of location-only search
                search_term = ""  # Empty search term for location-only
                location = self.generate_dynamic_location()
            else:
                search_term = self.generate_dynamic_search_term()
                location = self.generate_dynamic_location()
            
            # Create combination key
            combination_key = f"{search_term}|{location}"
            
            # Check if we've used this combination recently
            if combination_key not in self.used_combinations:
                # Add to used combinations (keep only recent ones to allow eventual reuse)
                self.used_combinations.add(combination_key)
                
                # Keep only last 100 combinations to allow eventual reuse
                if len(self.used_combinations) > 100:
                    # Remove oldest combinations (this is approximate, but good enough)
                    oldest_combinations = list(self.used_combinations)[:20]
                    for old_combo in oldest_combinations:
                        self.used_combinations.discard(old_combo)
                
                return search_term, location
            
            attempts += 1
        
        # If we've exhausted unique combinations, clear the cache and start fresh
        self.used_combinations.clear()
        
        # Generate a fresh combination
        if random.random() < 0.4:
            search_term = ""
            location = self.generate_dynamic_location()
        else:
            search_term = self.generate_dynamic_search_term()
            location = self.generate_dynamic_location()
            
        self.used_combinations.add(f"{search_term}|{location}")
        
        return search_term, location
    
    def add_human_search_behavior(self):
        """Add realistic human search behavior between searches."""
        
        # Simulate real user behavior - longer pauses between different searches
        if self.last_search_time:
            time_since_last = datetime.now() - self.last_search_time
            
            # If we just did a search, wait longer (like a real person)
            if time_since_last.total_seconds() < 30:
                extra_delay = random.uniform(15, 45)  # 15-45 second pause
                self.logger.info(f"Adding human behavior delay: {extra_delay:.1f} seconds")
                time.sleep(extra_delay)
        
        # Sometimes simulate "browsing" behavior
        if random.random() < 0.3:  # 30% chance
            self.logger.info("Simulating human browsing behavior...")
            # Scroll randomly
            try:
                scroll_amount = random.randint(100, 800)
                self.page.mouse.wheel(0, scroll_amount)
                self.human_delay(2, 5)
                
                # Sometimes scroll back up
                if random.random() < 0.4:
                    self.page.mouse.wheel(0, -random.randint(50, 300))
                    self.human_delay(1, 3)
            except:
                pass
        
        self.last_search_time = datetime.now()
    
    def extract_text_by_selectors(self, element, selectors):
        """Extract text from element using static selectors (first match wins)."""
        for selector in selectors:
            try:
                target_element = element.query_selector(selector)
                if target_element:
                    text = target_element.text_content().strip()
                    if text:
                        return text
            except Exception as e:
                self.logger.debug(f"Selector {selector} failed: {e}")
                continue
        return ""
    
    def extract_job_url(self, job_element):
        """Extract job URL using static selectors."""
        try:
            # Static URL extraction patterns
            url_selectors = [
                "a[href*='/view?id=']",
                "a[href*='view']",
                "a[href*='job']",
                "a[href*='details']",
                "a[href]"
            ]
            
            for selector in url_selectors:
                link_element = job_element.query_selector(selector)
                if link_element:
                    href = link_element.get_attribute('href')
                    if href:
                        # Make URL absolute if needed
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        return href
            
            return ""
            
        except Exception as e:
            self.logger.debug(f"Error extracting URL: {e}")
            return ""
    
    def setup_browser(self):
        """Setup Playwright browser with stealth configuration."""
        self.logger.info("Setting up Playwright browser for Australia...")
        
        playwright = sync_playwright().start()
        
        # Browser configuration for anti-detection
        self.browser = playwright.chromium.launch(
            headless=True,  # Visible browser for better success rate
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
    
    def navigate_to_search(self, search_term, location="Australia"):
        """Navigate to Talent.com Australia search page."""
        try:
            # Construct search URL for Australia
            if search_term.strip():
                # Search with both term and location
                search_url = f"{self.base_url}/jobs?q={search_term.replace(' ', '+')}&l={location.replace(' ', '+')}"
                self.logger.info(f"Navigating to search: '{search_term}' in '{location}'")
            else:
                # Location-only search (no search term)
                search_url = f"{self.base_url}/jobs?l={location.replace(' ', '+')}"
                self.logger.info(f"Navigating to location-only search: '{location}'")
            
            self.logger.info(f"URL: {search_url}")
            
            # Navigate to URL
            self.page.goto(search_url, wait_until='networkidle', timeout=60000)
            
            # Wait for page to load completely
            self.page.wait_for_selector('body', timeout=10000)
            self.human_delay(2, 4)
            
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
        """Find job card elements on the current page using correct selectors."""
        self.logger.info("Searching for job elements with corrected selectors...")
        
        # Corrected selectors based on website debugging
        selectors = [
            "main section",  # Primary selector - found 21 elements (perfect for ~20 jobs per page)
            "[data-testid*='card']",  # Secondary selector - also found 21 elements
            "section[data-testid*='jobcard-container']",  # Fallback
            "[data-testid*='jobcard-container']"  # Fallback
        ]
        
        for selector in selectors:
            try:
                self.logger.info(f"Trying selector: {selector}")
                elements = self.page.query_selector_all(selector)
                if elements:
                    # Validate these are actually job cards by checking content
                    valid_job_elements = []
                    for element in elements:
                        try:
                            text = element.text_content().lower()
                            # Check for job-related keywords
                            if any(keyword in text for keyword in ['remote', 'australia', 'promoted', 'company', 'ago']):
                                # Additional check: must contain some substantial text
                                if len(text.strip()) > 50:
                                    valid_job_elements.append(element)
                        except:
                            continue
                    
                    if valid_job_elements:
                        self.logger.info(f"Found {len(valid_job_elements)} valid job elements using selector: {selector}")
                        return valid_job_elements
                    else:
                        self.logger.debug(f"Found {len(elements)} elements but none are valid job cards with selector: {selector}")
                else:
                    self.logger.debug(f"No elements found with selector: {selector}")
            except Exception as e:
                self.logger.error(f"Error with selector {selector}: {e}")
                continue
        
        self.logger.warning("No job elements found with any selector")
        return []
    
    def extract_job_data(self, job_element, search_term, location):
        """Extract job data from a single job element using static selectors."""
        try:
            # Corrected selector-based field extraction based on debugging
            title = self.extract_text_by_selectors(job_element, [
                "h2",  # Primary selector - debugging found 22 h2 elements with job titles
                "h3",
                "[data-testid*='title']",
                "a[href*='view'] h2",
                "a h2"
            ])
            
            company = self.extract_text_by_selectors(job_element, [
                "span[color='#691F74']",
                "[data-testid='JobCardContainer'] span:nth-child(2)",
                "span.sc-4cea4a13-10.sc-4cea4a13-12",
                "a[href*='company'] span"
            ])
            
            job_location = self.extract_text_by_selectors(job_element, [
                "span[color='#222222']",
                "[data-testid='JobCardContainer'] span:nth-child(3)",
                "span[class*='location']",
                "div[class*='location'] span"
            ])
            
            # Static URL extraction
            url = self.extract_job_url(job_element)
            
            # Static field extraction for other fields
            salary = self.extract_text_by_selectors(job_element, [
                "span[class*='salary']",
                "div[class*='salary']",
                "span:has-text('$')"
            ])
            
            posted_date = self.extract_text_by_selectors(job_element, [
                "span[color='#676767']",
                "span:has-text('Last updated')",
                "span:has-text('ago')",
                "time"
            ])
            
            job_type_info = self.extract_text_by_selectors(job_element, [
                "div.sc-367d3bae-0.detFHD span",
                "span[class*='job-type']",
                "div[class*='type'] span"
            ])
            
            # Static description extraction
            basic_description = self.extract_text_by_selectors(job_element, [
                "div[class*='description']",
                "p",
                "span[class*='summary']"
            ])
            
            # Skip if missing essential data
            if not title or not company:
                return None
            
            # Clean and prepare data
            title = self.clean_text(title)
            company = self.clean_text(company)
            job_location = self.clean_text(job_location) if job_location else location or "Australia"
            salary = self.clean_text(salary) if salary else ""
            posted_date = self.clean_text(posted_date) if posted_date else ""
            
            # Use basic description from job card initially
            # Full description will be extracted later to avoid navigation issues during the loop
            description = self.clean_text(basic_description) if basic_description else ""
            
            # Detect job type from all available information including job_type_info
            detected_job_type = self.detect_job_type(job_element, title, description, job_type_info)
            
            return {
                'title': title,
                'company_name': company,
                'location': job_location,
                'description': description,
                'external_url': url,
                'salary_text': salary,
                'posted_date': posted_date,
                'job_type': detected_job_type,
                'external_source': 'au.talent.com',
                'search_term': search_term,
                'scraped_location': location,
                'country': 'Australia'
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting job data: {e}")
            return None
    

    
    def extract_full_job_description(self, job_url):
        """Extract full job description by visiting the individual job page."""
        if not job_url:
            return ""
        
        try:
            self.logger.debug(f"Visiting job page for full description: {job_url}")
            
            # Store the current search results page URL to return to it later
            current_search_url = self.page.url
            
            # Navigate to job detail page
            self.page.goto(job_url, wait_until='networkidle', timeout=30000)
            self.human_delay(1, 3)
            
            # Static hardcoded description selectors
            description_selectors = [
                'div.sc-4cea4a13-10.sc-4cea4a13-11.sc-6cde2aa1-10',
                'div.sc-6cde2aa1-10',
                '[class*="sc-6cde2aa1-10"]',
                'div[class*="job-description"]',
                'section[class*="description"]',
                'div[data-testid*="description"]',
                'main div[class*="content"]',
                'article div'
            ]
            
            full_description = ""
            
            for selector in description_selectors:
                try:
                    description_element = self.page.query_selector(selector)
                    if description_element:
                        # Get the innerHTML to preserve formatting but extract text
                        desc_html = description_element.inner_html()
                        desc_text = description_element.text_content().strip()
                        
                        # Clean up the description text but keep full content
                        desc_text = self.clean_description_text(desc_text)
                        
                        # Remove "Job description" header if it exists at the beginning
                        if desc_text.lower().startswith('job description'):
                            desc_text = desc_text[len('job description'):].strip()
                        
                        if len(desc_text) > 50:  # Must be substantial content
                            full_description = desc_text
                            self.logger.debug(f"Found full description using selector: {selector}")
                            self.logger.debug(f"Description length: {len(desc_text)} characters")
                            break
                except Exception as selector_error:
                    self.logger.debug(f"Selector {selector} failed: {selector_error}")
                    continue
            
            # If no specific description found, try to get main content
            if not full_description:
                try:
                    # Try to find any div that contains substantial text content
                    main_content = self.page.query_selector('main, article, .main, [role="main"]')
                    if main_content:
                        content_text = main_content.text_content().strip()
                        if "job description" in content_text.lower():
                            # Extract only the job description part
                            parts = content_text.lower().split("job description")
                            if len(parts) > 1:
                                full_description = parts[1].strip()
                except:
                    pass
            
            # Return to the search results page to maintain pagination context
            try:
                if current_search_url and 'jobs?' in current_search_url:
                    self.page.goto(current_search_url, wait_until='networkidle', timeout=30000)
                    self.human_delay(1, 2)
                    self.logger.debug(f"Returned to search results page: {current_search_url}")
            except Exception as nav_error:
                self.logger.error(f"Error returning to search page: {nav_error}")
            
            # Return full description without any truncation
            return full_description if full_description else ""
            
        except Exception as e:
            self.logger.error(f"Error extracting full description from {job_url}: {e}")
            # Try to return to search page even on error
            try:
                if 'current_search_url' in locals() and current_search_url and 'jobs?' in current_search_url:
                    self.page.goto(current_search_url, wait_until='networkidle', timeout=30000)
                    self.human_delay(1, 2)
            except:
                pass
            return ""
    
    def clean_description_text(self, text):
        """Clean and format job description text while preserving structure."""
        if not text:
            return ""
        
        # Clean up excessive whitespace but preserve line breaks
        text = re.sub(r'\n\s*\n', '\n\n', text)  # Preserve paragraph breaks
        text = re.sub(r'[ \t]+', ' ', text)  # Normalize spaces and tabs
        text = text.strip()
        
        # Remove common navigation/UI elements
        text = re.sub(r'(Apply now|Apply for this job|Back to search|Save job|Share|Print|Apply online)', '', text, flags=re.IGNORECASE)
        
        # Remove footer/header elements
        text = re.sub(r'(Terms of use|Privacy policy|Cookie policy|Contact us|Sign in|Register)', '', text, flags=re.IGNORECASE)
        
        # Remove "Job description" header if at the beginning
        if text.lower().startswith('job description'):
            text = text[len('job description'):].strip()
        
        # Clean up any remaining artifacts
        text = re.sub(r'\s+', ' ', text)  # Final cleanup of whitespace
        
        return text.strip()
    
    def detect_job_type(self, job_element, job_title="", job_description="", job_type_info=""):
        """Detect job type from Talent.com job listing based on text indicators."""
        
        # Collect all text from the job element
        try:
            element_text = job_element.text_content().lower()
        except:
            element_text = ""
        
        # Combine all available text for analysis including job type info
        combined_text = f"{job_title} {job_description} {job_type_info} {element_text}".lower()
        
        self.logger.debug(f"Analyzing job type for: {job_title}")
        self.logger.debug(f"Combined text sample: {combined_text[:200]}...")
        
        # Define job type patterns based on common terminology
        # Order matters - check more specific patterns first
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
                '12 month contract', 'fixed-term', 'temp contract',
                'contract work', 'contracting'
            ],
            'temporary': [
                'temporary', 'temp', 'interim', 'temporary position',
                'short term', 'temp role', 'cover position',
                'maternity cover', 'temporary assignment', 'temp work'
            ],
            'internship': [
                'internship', 'intern', 'graduate program', 'traineeship',
                'apprenticeship', 'graduate role', 'junior trainee',
                'student position', 'work experience', 'cadetship'
            ],
            'freelance': [
                'freelance', 'freelancer', 'independent contractor',
                'self employed', 'consultant', 'remote freelance',
                'project based', 'gig work'
            ]
        }
        
        # Check for each job type pattern (order matters!)
        for job_type, patterns in job_type_patterns.items():
            for pattern in patterns:
                if pattern in combined_text:
                    self.logger.info(f"✅ Detected job type '{job_type}' from pattern '{pattern}' in: {job_title}")
                    return job_type
        
        # Look for remote work indicators (can be combined with other types)
        remote_patterns = ['remote', 'work from home', 'wfh', 'home based', 'telecommute']
        is_remote = any(pattern in combined_text for pattern in remote_patterns)
        
        # Default to full_time if no specific type detected
        detected_type = 'full_time'
        
        # If remote is detected, we might want to note it but still classify the base type
        if is_remote:
            self.logger.debug(f"Remote work detected in addition to {detected_type}")
        
        self.logger.info(f"🔍 Defaulting to '{detected_type}' for: {job_title}")
        return detected_type
    

    
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
    
    def parse_posted_date(self, date_text):
        """Parse posted date information."""
        if not date_text:
            return None
        
        # Common patterns for relative dates
        date_text = date_text.lower().strip()
        now = datetime.now()
        
        if 'today' in date_text:
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
        
        return None
    
    def get_or_create_company(self, company_name):
        """Get or create company object with better duplicate handling."""
        try:
            # Keep company name as is without truncation
            
            # First try to find existing company by name
            try:
                company = Company.objects.get(name=company_name)
                return company
            except Company.DoesNotExist:
                pass
            
            # Create unique slug for new company
            base_slug = slugify(company_name)
            unique_slug = base_slug
            counter = 1
            
            while Company.objects.filter(slug=unique_slug).exists():
                unique_slug = f"{base_slug}-{counter}"
                counter += 1
            
            # Keep slug as is without length restriction
            
            company = Company.objects.create(
                name=company_name,
                slug=unique_slug,
                description=f'Company profile for {company_name}',
                company_size='medium'
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
                    external_source='au.talent.com'
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
                
                # Parse posted date
                posted_date = self.parse_posted_date(job_data.get('posted_date', ''))
                
                # Categorize job using your existing service
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
                
                # Create unique slug to avoid duplicates
                base_slug = slugify(job_data['title'])
                company_part = slugify(company.name)  # Keep full company part
                unique_slug = f"{base_slug}-{company_part}"
                
                # Handle slug length and uniqueness
                counter = 1
                original_slug = unique_slug
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{original_slug}-{counter}"
                    counter += 1
                
                # Keep slug as is without length restriction
                
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
                    date_posted=posted_date,
                    posted_ago=job_data.get('posted_date', ''),
                    status='active',
                    tags=tags_str,
                    additional_info={
                        'search_term': job_data['search_term'],
                        'scraped_location': job_data['scraped_location'],
                        'scraper_version': 'Playwright-Australia-Talent-1.0',
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
    
    def detect_pagination(self):
        """Detect if pagination is available and return next page element."""
        try:
            self.logger.debug("Starting pagination detection with corrected selectors...")
            
            current_url = self.page.url
            self.logger.debug(f"Current URL: {current_url}")
            
            # Based on debugging, we found pagination links with 'a[href*="&p="]'
            # The debugging found 2 pagination elements
            next_page_selectors = [
                'a[href*="&p="]',  # Primary selector found during debugging
                'a[href*="?p="]',  # Alternative URL structure
                'a:has-text("2")',  # Page number 2
                'a:has-text("3")',  # Page number 3
                'a:has-text("Next")',
                'a:has-text(">")',
                'a[aria-label*="Next"]',
                'button:has-text("Next")',
                'button:has-text(">")'
            ]
            
            # Extract current page number from URL if present
            current_page = 1
            if '&p=' in current_url:
                try:
                    current_page = int(current_url.split('&p=')[1].split('&')[0])
                except:
                    pass
            elif '?p=' in current_url:
                try:
                    current_page = int(current_url.split('?p=')[1].split('&')[0])
                except:
                    pass
            
            self.logger.debug(f"Current page detected as: {current_page}")
            
            for selector in next_page_selectors:
                try:
                    self.logger.debug(f"Trying pagination selector: {selector}")
                    elements = self.page.query_selector_all(selector)
                    
                    for element in elements:
                        if not element:
                            continue
                            
                        href = element.get_attribute('href')
                        title = element.get_attribute('title')
                        text = element.text_content().strip()
                        classes = element.get_attribute('class') or ''
                        
                        self.logger.debug(f"Found element - href: {href}, title: {title}, text: '{text}', classes: {classes}")
                        
                        if not href:
                            continue
                        
                        # Skip if it's the current page indicator
                        if 'gSKnXL' in classes:
                            self.logger.debug("Skipping current page indicator")
                            continue
                            
                        # Check if this link goes to a higher page number
                        target_page = None
                        if '&p=' in href:
                            try:
                                target_page = int(href.split('&p=')[1].split('&')[0])
                            except:
                                pass
                        elif '?p=' in href:
                            try:
                                target_page = int(href.split('?p=')[1].split('&')[0])
                            except:
                                pass
                        
                        # If we found a page number and it's higher than current, use it
                        if target_page and target_page > current_page:
                            self.logger.debug(f"Found next page element! Target page: {target_page}, Current: {current_page}")
                            self.logger.debug(f"Using selector: {selector}")
                            self.logger.debug(f"Next page href: {href}")
                            return element
                        
                        # If no page number detected but href contains pagination, might be next page
                        elif ('&p=' in href or '?p=' in href) and target_page != current_page:
                            self.logger.debug(f"Found potential next page element with href: {href}")
                            return element
                            
                except Exception as e:
                    self.logger.debug(f"Error with pagination selector {selector}: {e}")
                    continue
            
            # Log all links for debugging
            all_links = self.page.query_selector_all('a[href]')
            pagination_links = [link for link in all_links if link.get_attribute('href') and ('&p=' in link.get_attribute('href') or '?p=' in link.get_attribute('href'))]
            
            self.logger.debug(f"Found {len(pagination_links)} total pagination links:")
            for link in pagination_links[:5]:  # Log first 5 only
                href = link.get_attribute('href')
                text = link.text_content().strip()
                self.logger.debug(f"  - '{text}' -> {href}")
            
            self.logger.debug("No next page element found after exhaustive search")
            return None
            
        except Exception as e:
            self.logger.error(f"Error detecting pagination: {e}")
            return None
    
    def navigate_to_next_page(self):
        """Navigate to the next page of job results."""
        try:
            next_element = self.detect_pagination()
            
            if not next_element:
                self.logger.info("No next page available")
                return False
            
            # Get the href before clicking
            next_href = next_element.get_attribute('href')
            self.logger.info(f"Navigating to next page: {next_href}")
            
            # Scroll to the pagination element
            next_element.scroll_into_view_if_needed()
            self.human_delay(1, 2)
            
            # Click the next page element
            next_element.click()
            
            # Wait for the new page to load
            self.page.wait_for_load_state('domcontentloaded', timeout=30000)
            self.human_delay(3, 5)
            
            # Verify we're on a new page by checking URL change
            current_url = self.page.url
            if next_href and next_href in current_url:
                self.logger.info("Successfully navigated to next page")
                return True
            else:
                self.logger.warning("Page navigation may have failed - URL didn't change as expected")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {e}")
            return False
    
    def scrape_jobs_from_current_page(self, search_term, location, page_num=1):
        """Scrape jobs from the current page without navigation."""
        try:
            self.logger.info(f"Scraping jobs from page {page_num}...")
            
            # Find job elements
            job_elements = self.find_job_elements()
            
            if not job_elements:
                self.logger.warning(f"No job elements found on page {page_num}")
                return []
            
            self.logger.info(f"Found {len(job_elements)} job elements on page {page_num}")
            
            # Extract job data
            jobs_data = []
            for i, element in enumerate(job_elements):
                try:
                    # Check job limit
                    if self.job_limit and self.jobs_scraped >= self.job_limit:
                        self.logger.info(f"Reached job limit: {self.job_limit}")
                        break
                    
                    job_data = self.extract_job_data(element, search_term, location)
                    
                    if job_data:
                        jobs_data.append(job_data)
                        self.jobs_scraped += 1
                        self.logger.info(f"Page {page_num}, Job {i+1}: {job_data['title']} at {job_data['company_name']}")
                    else:
                        self.logger.debug(f"Skipped invalid job {i+1} on page {page_num}")
                    
                    # Human delay between job extractions
                    self.human_delay(0.2, 0.8)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1} on page {page_num}: {e}")
                    self.errors_count += 1
                    continue
            
            # Now extract full descriptions for all jobs (after collecting basic data)
            # This prevents navigation issues during the main job extraction loop
            self.logger.info(f"Extracting full descriptions for {len(jobs_data)} jobs from page {page_num}...")
            for i, job_data in enumerate(jobs_data):
                try:
                    if job_data.get('external_url'):
                        self.logger.debug(f"Getting full description for job {i+1}: {job_data['title']}")
                        full_description = self.extract_full_job_description(job_data['external_url'])
                        if full_description and len(full_description) > len(job_data.get('description', '')):
                            job_data['description'] = full_description
                            self.logger.debug(f"Updated description for {job_data['title']} ({len(full_description)} chars)")
                        else:
                            self.logger.debug(f"Keeping basic description for {job_data['title']}")
                    
                    # Small delay between description extractions
                    self.human_delay(0.5, 1.0)
                    
                except Exception as e:
                    self.logger.error(f"Error extracting full description for job {i+1}: {e}")
                    continue
            
            return jobs_data
            
        except Exception as e:
            self.logger.error(f"Error scraping page {page_num}: {e}")
            self.errors_count += 1
            return []
    
    def scrape_page(self, search_term, location="Australia", max_pages=5):
        """Scrape multiple pages of job results with pagination support."""
        try:
            # Navigate to first search page
            if not self.navigate_to_search(search_term, location):
                return []
            
            all_jobs_data = []
            current_page = 1
            
            while current_page <= max_pages:
                self.logger.info(f"Processing page {current_page}/{max_pages}...")
                
                # Scrape jobs from current page
                page_jobs = self.scrape_jobs_from_current_page(search_term, location, current_page)
                
                if page_jobs:
                    all_jobs_data.extend(page_jobs)
                    self.pages_scraped += 1
                    self.logger.info(f"Page {current_page} completed: {len(page_jobs)} jobs extracted")
                else:
                    self.logger.warning(f"No jobs found on page {current_page}")
                    self.pages_scraped += 1
                
                # Check if we should continue
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    self.logger.info(f"Reached job limit: {self.job_limit}")
                    break
                
                # Try to go to next page
                if current_page < max_pages:
                    if self.navigate_to_next_page():
                        current_page += 1
                        # Add delay between pages to be more human-like
                        self.human_delay(2, 5)
                    else:
                        self.logger.info("No more pages available or reached end of pagination")
                        break
                else:
                    self.logger.info(f"Reached maximum pages limit: {max_pages}")
                    break
            
            self.logger.info(f"Pagination completed. Total jobs from all pages: {len(all_jobs_data)}")
            return all_jobs_data
            
        except Exception as e:
            self.logger.error(f"Error during pagination scraping: {e}")
            self.errors_count += 1
            return []
    
    def run_scraping(self, max_searches=5, max_pages_per_search=3):
        """Main scraping orchestrator for Talent.com Australia with pagination support."""
        start_time = datetime.now()
        
        self.logger.info("Starting Talent.com Australia job scraping with FIXED pagination support...")
        self.logger.info(f"Target: {self.job_limit or 'unlimited'} jobs")
        self.logger.info(f"Max pages to scrape: {max_pages_per_search}")
        self.logger.info("Using FIXED URL with proper pagination handling - NO search term changes")
        
        try:
            # Setup browser
            self.setup_browser()
            
            # Use the specific URL instead of generating search combinations
            fixed_url = "https://au.talent.com/jobs?l=Australia&id=7563d299b20e"
            self.logger.info(f"Using fixed URL: {fixed_url}")
            
            # Navigate directly to the specific URL
            self.logger.info("Navigating to the fixed URL...")
            self.page.goto(fixed_url, wait_until='networkidle', timeout=60000)
            
            # Wait for page to load completely
            self.page.wait_for_selector('body', timeout=10000)
            self.human_delay(2, 4)
            
            self.logger.info("Successfully loaded the fixed URL page")
            
            # Scrape with pagination from the fixed URL
            all_jobs_data = []
            current_page = 1
            
            while current_page <= max_pages_per_search:
                self.logger.info(f"\n--- Processing page {current_page}/{max_pages_per_search} ---")
                
                # Scrape jobs from current page
                page_jobs = self.scrape_jobs_from_current_page("Fixed URL Scraper", "Australia", current_page)
                
                if page_jobs:
                    all_jobs_data.extend(page_jobs)
                    self.pages_scraped += 1
                    self.logger.info(f"Page {current_page} completed: {len(page_jobs)} jobs extracted")
                else:
                    self.logger.warning(f"No jobs found on page {current_page}")
                    self.pages_scraped += 1
                
                # Check if we should continue
                if self.job_limit and self.jobs_scraped >= self.job_limit:
                    self.logger.info(f"Reached job limit: {self.job_limit}")
                    break
                
                # Try to go to next page
                if current_page < max_pages_per_search:
                    if self.navigate_to_next_page():
                        current_page += 1
                        # Add delay between pages
                        self.human_delay(3, 6)
                    else:
                        self.logger.info("No more pages available or reached end of pagination")
                        break
                else:
                    self.logger.info(f"Reached maximum pages limit: {max_pages_per_search}")
                    break
            
            # Save jobs to database
            self.logger.info(f"Saving {len(all_jobs_data)} jobs to database...")
            for job_data in all_jobs_data:
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
            'pages_scraped': self.pages_scraped,
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
        print("TALENT.COM AUSTRALIA SCRAPING COMPLETED! (FIXED PAGINATION)")
        print("="*80)
        print(f"Duration: {duration}")
        print(f"Pages scraped: {self.pages_scraped}")
        print(f"Jobs scraped: {self.jobs_scraped}")
        print(f"Jobs saved: {self.jobs_saved}")
        print(f"Duplicates skipped: {self.duplicates_found}")
        print(f"Errors encountered: {self.errors_count}")
        
        if self.jobs_scraped > 0:
            success_rate = (self.jobs_saved / self.jobs_scraped) * 100
            print(f"Success rate: {success_rate:.1f}%")
        
        # Database statistics (skip to avoid async context issues)
        print(f"Note: Database statistics skipped to avoid async context issues")
        
        print("="*80)


def main():
    """Main function."""
    print("Professional Talent.com Australia Job Scraper (Playwright)")
    print("="*60)
    print("Advanced job scraper with professional database structure")
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
    scraper = TalentAustraliaJobScraper(job_limit=job_limit)
    # Fixed: Use more pages since we're only doing one URL now
    scraper.run_scraping(max_searches=1, max_pages_per_search=10)


if __name__ == "__main__":
    main()


def run(job_limit=None, max_pages=10):
    """Automation entrypoint for Talent.com Australia scraper.

    Runs the scraper without CLI args and returns a summary dictionary.
    """
    try:
        scraper = TalentAustraliaJobScraper(job_limit=job_limit)
        summary = scraper.run_scraping(max_searches=1, max_pages_per_search=max_pages)
        summary = summary or {}
        summary.update({'success': True})
        return summary
    except Exception as e:
        try:
            logging.getLogger(__name__).error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }

