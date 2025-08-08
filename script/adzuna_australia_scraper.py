#!/usr/bin/env python3
"""
Professional Adzuna Australia Job Scraper using Playwright
==========================================================

Advanced Playwright-based scraper for Adzuna Australia that integrates with 
your existing seek_scraper_project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Australian-specific optimization

Usage:
    python adzuna_australia_scraper.py [job_limit]
    
Examples:
    python adzuna_australia_scraper.py 10    # Scrape 10 Australian jobs
    python adzuna_australia_scraper.py       # Scrape all available jobs
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


class AdzunaAustraliaJobScraper:
    """Professional Adzuna Australia job scraper using Playwright."""
    
    def __init__(self, job_limit=None):
        """Initialize the scraper with optional job limit."""
        self.base_url = "https://www.adzuna.com.au"
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
        
        # Dynamic search term generation - more human-like
        self.base_roles = ["engineer", "developer", "analyst", "manager", "specialist", "coordinator", "consultant", "advisor", "executive", "lead"]
        self.tech_prefixes = ["software", "web", "mobile", "cloud", "data", "ai", "machine learning", "cyber security", "network", "database"]
        self.business_prefixes = ["business", "financial", "marketing", "digital", "project", "product", "operations", "sales", "hr", "customer"]
        self.general_prefixes = ["senior", "junior", "lead", "principal", "head of", "assistant", "graduate", "trainee"]
        
        # Dynamic Australian location generation - includes suburbs and regions
        self.major_cities = ["sydney", "melbourne", "brisbane", "perth", "adelaide", "canberra", "darwin", "hobart"]
        self.sydney_areas = ["sydney cbd", "parramatta", "chatswood", "manly", "bondi", "surry hills", "pyrmont", "north sydney"]
        self.melbourne_areas = ["melbourne cbd", "richmond", "fitzroy", "st kilda", "south yarra", "carlton", "brunswick", "collingwood"]
        self.brisbane_areas = ["brisbane cbd", "south bank", "fortitude valley", "new farm", "paddington", "west end", "kangaroo point"]
        self.perth_areas = ["perth cbd", "fremantle", "subiaco", "nedlands", "cottesloe", "northbridge", "leederville"]
        
        # Used search combinations tracking for anti-detection
        self.used_combinations = set()
        self.last_search_time = None
    
    def setup_logging(self):
        """Setup logging configuration."""
        # Configure logging with UTF-8 encoding for compatibility
        file_handler = logging.FileHandler('adzuna_australia_scraper.log', encoding='utf-8')
        console_handler = logging.StreamHandler(sys.stdout)
        
        # Set up formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_or_create_bot_user(self):
        """Get or create a bot user for job posting attribution."""
        try:
            user, created = User.objects.get_or_create(
                username='adzuna_australia_bot',
                defaults={
                    'email': 'adzuna.australia.bot@jobscraper.local',
                    'first_name': 'Adzuna Australia',
                    'last_name': 'Scraper Bot'
                }
            )
            if created:
                self.logger.info("Created new bot user for Adzuna Australia scraping")
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
                "finance manager", "hr advisor", "operations coordinator", "support specialist"
            ]),
            
            # Sometimes use skills-based searches (like real people do)
            lambda: random.choice([
                "python", "javascript", "react", "aws", "sql", "excel", "salesforce",
                "adobe", "marketing", "accounting", "nursing", "teaching", "design"
            ]),
            
            # Industry-based searches
            lambda: random.choice([
                "healthcare jobs", "education jobs", "finance jobs", "technology jobs",
                "retail jobs", "hospitality jobs", "construction jobs", "government jobs"
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
            lambda: random.choice(["nsw", "vic", "qld", "wa", "sa", "act", "nt", "tas"]),
            
            # Regional areas (less common but more human-like)
            lambda: random.choice([
                "gold coast", "sunshine coast", "newcastle", "wollongong", "geelong",
                "ballarat", "bendigo", "albury", "wagga wagga", "cairns", "townsville",
                "mackay", "rockhampton", "toowoomba", "bunbury", "albany", "geraldton"
            ])
        ]
        
        strategy = random.choice(location_strategies)
        return strategy()
    
    def get_intelligent_search_combination(self):
        """Get an intelligent search combination that avoids repetition and appears human."""
        
        max_attempts = 50  # Prevent infinite loops
        attempts = 0
        
        while attempts < max_attempts:
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
    
    def setup_browser(self):
        """Setup Playwright browser with stealth configuration."""
        self.logger.info("Setting up Playwright browser for Australia...")
        
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
    
    def navigate_to_search(self, search_term, location=""):
        """Navigate to Adzuna Australia search page."""
        try:
            # Construct search URL for Australia
            search_url = f"{self.base_url}/search?q={search_term.replace(' ', '+')}"
            if location:
                search_url += f"&w={location.replace(' ', '+')}"
            
            self.logger.info(f"Navigating to: {search_url}")
            
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
        """Find job card elements on the current page."""
        self.logger.info("Searching for job elements...")
        
        # Enhanced selectors for Adzuna Australia
        selectors = [
            "[data-testid='job-card']",
            ".job-card", 
            ".job-listing",
            ".result",
            ".search-result",
            "[class*='job-result']",
            "[class*='job-item']", 
            "[class*='result-item']",
            "article",
            "div[class*='job']",
            "li[class*='job']",
            ".listing",
            "[data-testid='jobCard']",
            ".card",
            "[class*='card']"
        ]
        
        for selector in selectors:
            try:
                self.logger.info(f"Trying selector: {selector}")
                elements = self.page.query_selector_all(selector)
                if elements:
                    self.logger.info(f"Found {len(elements)} elements using selector: {selector}")
                    return elements
                else:
                    self.logger.debug(f"No elements found with selector: {selector}")
            except Exception as e:
                self.logger.error(f"Error with selector {selector}: {e}")
                continue
        
        self.logger.warning("No job elements found with any selector")
        return []
    
    def extract_job_data(self, job_element, search_term, location):
        """Extract job data from a single job element."""
        try:
            # Extract title
            title = self.extract_text_by_selectors(job_element, [
                "h2 a", "h3 a", "h1 a",
                "[data-testid='job-title']",
                ".job-title",
                "a[class*='title']",
                "span[class*='title']",
                ".title"
            ])
            
            # Extract company
            company = self.extract_text_by_selectors(job_element, [
                "[data-testid='job-company']",
                ".company-name",
                ".company",
                "span[class*='company']",
                "div[class*='company']",
                "[class*='employer']"
            ])
            
            # Extract location  
            job_location = self.extract_text_by_selectors(job_element, [
                "[data-testid='job-location']",
                ".location",
                ".job-location",
                "span[class*='location']",
                "[class*='location']"
            ])
            
            # Extract URL
            url = ""
            try:
                link_element = job_element.query_selector("a")
                if link_element:
                    url = link_element.get_attribute("href")
                    if url and not url.startswith("http"):
                        url = urljoin(self.base_url, url)
            except:
                pass
            
            # Extract salary
            salary = self.extract_text_by_selectors(job_element, [
                ".salary",
                "[class*='salary']",
                "[data-testid='job-salary']",
                "[class*='pay']",
                "[class*='wage']"
            ])
            
            # Extract basic description from listing
            basic_description = self.extract_text_by_selectors(job_element, [
                ".job-description",
                ".description", 
                "[class*='description']",
                ".job-snippet",
                ".snippet",
                ".summary",
                ".job-summary",
                ".role-summary",
                ".teaser",
                ".preview",
                ".excerpt",
                "p",
                ".content",
                "[data-testid='job-description']",
                "[data-testid='description']",
                "[data-testid='summary']"
            ])
            
            # Skip if missing essential data
            if not title or not company:
                return None
            
            # Clean and prepare data
            title = self.clean_text(title)
            company = self.clean_text(company)
            job_location = self.clean_text(job_location) if job_location else location or "Australia"
            salary = self.clean_text(salary) if salary else ""
            
            # Try to get full description from job detail page if URL is available
            description = ""
            if url:
                try:
                    description = self.extract_full_job_description(url)
                    if description:
                        self.logger.debug(f"Extracted full description ({len(description)} chars) for: {title}")
                    else:
                        # Fallback to basic description if full extraction fails
                        description = self.clean_text(basic_description) if basic_description else ""
                        description = self.truncate_description(description)
                        self.logger.debug(f"Using basic description for: {title}")
                except Exception as e:
                    self.logger.error(f"Error getting full description for {title}: {e}")
                    description = self.clean_text(basic_description) if basic_description else ""
                    description = self.truncate_description(description)
            else:
                description = self.clean_text(basic_description) if basic_description else ""
                description = self.truncate_description(description)
            
            # Detect job type from all available information
            detected_job_type = self.detect_job_type(job_element, title, description)
            
            return {
                'title': title,
                'company_name': company,
                'location': job_location,
                'description': description,
                'external_url': url,
                'salary_text': salary,
                'job_type': detected_job_type,  # Now uses intelligent detection
                'external_source': 'adzuna.com.au',
                'search_term': search_term,
                'scraped_location': location,
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
        """Extract full job description by visiting the individual job page."""
        if not job_url:
            return ""
        
        try:
            self.logger.debug(f"Visiting job page for full description: {job_url}")
            
            # Navigate to job detail page
            self.page.goto(job_url, wait_until='networkidle', timeout=30000)
            self.human_delay(1, 3)
            
            # Enhanced description selectors for Adzuna job detail pages
            description_selectors = [
                '.job-description',
                '.description',
                '.job-details',
                '.role-description',
                '.position-description',
                '.vacancy-description',
                '.job-content',
                '.content',
                '.details',
                '.summary',
                '.job-summary',
                '.role-summary',
                '[data-testid="job-description"]',
                '[data-testid="description"]',
                '[data-testid="job-details"]',
                '[class*="description"]',
                '[class*="details"]',
                '[class*="content"]',
                'main .content',
                'article .content',
                '.main-content',
                '#job-description',
                '#description',
                '.job-info',
                '.job-detail',
                '.position-details'
            ]
            
            full_description = ""
            
            for selector in description_selectors:
                try:
                    description_element = self.page.query_selector(selector)
                    if description_element:
                        desc_text = description_element.text_content().strip()
                        
                        # Clean up the description text
                        desc_text = self.clean_description_text(desc_text)
                        
                        if len(desc_text) > 50:  # Must be substantial content
                            full_description = desc_text
                            self.logger.debug(f"Found full description using selector: {selector}")
                            break
                except:
                    continue
            
            # If no specific description found, try to get main content
            if not full_description:
                try:
                    main_content = self.page.query_selector('main, article, .main')
                    if main_content:
                        full_description = self.clean_description_text(main_content.text_content())
                except:
                    pass
            
            # Use helper method to truncate description consistently
            return self.truncate_description(full_description)
            
        except Exception as e:
            self.logger.error(f"Error extracting full description from {job_url}: {e}")
            return ""
    
    def clean_description_text(self, text):
        """Clean and format job description text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common navigation/UI elements
        text = re.sub(r'(Apply now|Apply for this job|Back to search|Save job|Share|Print)', '', text, flags=re.IGNORECASE)
        
        # Remove footer/header elements
        text = re.sub(r'(Terms of use|Privacy policy|Cookie policy|Contact us)', '', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def detect_job_type(self, job_element, job_title="", job_description=""):
        """Detect job type from Adzuna job listing based on text indicators."""
        
        # Collect all text from the job element
        try:
            element_text = job_element.text_content().lower()
        except:
            element_text = ""
        
        # Combine all available text for analysis
        combined_text = f"{job_title} {job_description} {element_text}".lower()
        
        self.logger.debug(f"Analyzing job type for: {job_title}")
        self.logger.debug(f"Combined text sample: {combined_text[:200]}...")
        
        # Define job type patterns based on Adzuna's common terminology
        # Order matters - check more specific patterns first
        job_type_patterns = {
            'casual': [
                'casual', 'casual position', 'casual role', 'casual work',
                'ad hoc', 'as needed', 'on call', 'when required',
                'zero hours', 'flexible casual', 'casual staff',
                'casual nurses', 'casual registered'
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
                'project based', 'gig work', 'contractor'
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
    
    def truncate_description(self, description):
        """Truncate description to 200-300 characters with smart word boundaries."""
        if not description or len(description) <= 300:
            return description
            
        # Find a good cut point around 250-300 chars to avoid cutting mid-word
        cut_point = 250
        # Try to cut at a space or punctuation to avoid breaking words
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
                    external_source='adzuna.com.au'
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
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'],
                    slug=slugify(job_data['title']),
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
                    additional_info={
                        'search_term': job_data['search_term'],
                        'scraped_location': job_data['scraped_location'],
                        'scraper_version': 'Playwright-Australia-1.0',
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
    
    def scrape_page(self, search_term, location=""):
        """Scrape a single page of job results."""
        try:
            # Navigate to search page
            if not self.navigate_to_search(search_term, location):
                return []
            
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
                    
                    job_data = self.extract_job_data(element, search_term, location)
                    
                    if job_data:
                        jobs_data.append(job_data)
                        self.jobs_scraped += 1
                        self.logger.info(f"Extracted job {i+1}: {job_data['title']} at {job_data['company_name']}")
                    else:
                        self.logger.debug(f"Skipped invalid job {i+1}")
                    
                    # Human delay between job extractions
                    self.human_delay(0.2, 0.8)
                    
                except Exception as e:
                    self.logger.error(f"Error processing job {i+1}: {e}")
                    self.errors_count += 1
                    continue
            
            return jobs_data
            
        except Exception as e:
            self.logger.error(f"Error scraping page: {e}")
            self.errors_count += 1
            return []
    
    def run_scraping(self, max_searches=5):
        """Main scraping orchestrator for Adzuna Australia with dynamic human-like search behavior."""
        start_time = datetime.now()
        
        self.logger.info("Starting Adzuna Australia job scraping with intelligent search generation...")
        self.logger.info(f"Target: {self.job_limit or 'unlimited'} jobs")
        self.logger.info(f"Max searches: {max_searches}")
        self.logger.info("Using dynamic search terms and locations for human-like behavior")
        
        try:
            # Setup browser
            self.setup_browser()
            
            # Generate dynamic search combinations - no static patterns!
            search_combinations = []
            for i in range(max_searches):
                search_term, location = self.get_intelligent_search_combination()
                search_combinations.append((search_term, location))
                self.logger.info(f"Generated search {i+1}: '{search_term}' in '{location or 'Australia'}'")
            
            # Scrape jobs with human-like behavior
            for i, (search_term, location) in enumerate(search_combinations, 1):
                if self.job_limit and (self.jobs_saved >= self.job_limit or self.jobs_scraped >= self.job_limit):
                    self.logger.info(f"Reached job limit: {self.job_limit}")
                    break
                
                self.logger.info(f"\n--- Search {i}/{len(search_combinations)}: '{search_term}' in '{location or 'Australia'}' ---")
                
                # Add human-like behavior before each search
                self.add_human_search_behavior()
                
                try:
                    # Scrape current search
                    jobs_data = self.scrape_page(search_term, location)
                    
                    # Save jobs to database with human-like patterns
                    for job_data in jobs_data:
                        if self.job_limit and self.jobs_saved >= self.job_limit:
                            self.logger.info(f"Reached save limit: {self.job_limit}")
                            break
                        
                        self.save_job_to_database(job_data)
                        
                        # Variable delay between saves (more human-like)
                        save_delay = random.uniform(0.5, 2.0)
                        time.sleep(save_delay)
                    
                except Exception as e:
                    self.logger.error(f"Error in search {i}: {e}")
                    self.errors_count += 1
                    
                    # Even on error, behave human-like (don't rush to next search)
                    error_pause = random.uniform(10, 30)
                    self.logger.info(f"Error occurred, pausing {error_pause:.1f}s before continuing...")
                    time.sleep(error_pause)
                    continue
                
                # Intelligent delay between searches - varies based on success
                if i < len(search_combinations):
                    if jobs_data:  # If we found jobs, take a longer break (like a real person would)
                        delay = random.uniform(20, 60)
                        self.logger.info(f"Search successful, taking {delay:.1f}s break before next search")
                    else:  # If no jobs found, shorter break
                        delay = random.uniform(10, 25)
                        self.logger.info(f"No jobs found, taking {delay:.1f}s break before trying different search")
                    
                    time.sleep(delay)
            
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
        print("ADZUNA AUSTRALIA SCRAPING COMPLETED!")
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
            total_adzuna_jobs = JobPosting.objects.filter(external_source='adzuna.com.au').count()
            print(f"Total Adzuna Australia jobs in database: {total_adzuna_jobs}")
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
        
        print("="*80)


def main():
    """Main function."""
    print("Professional Adzuna Australia Job Scraper (Playwright)")
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
    scraper = AdzunaAustraliaJobScraper(job_limit=job_limit)
    scraper.run_scraping(max_searches=5)


if __name__ == "__main__":
    main()
