#!/usr/bin/env python3
"""
Professional APS Jobs Australia Scraper using Playwright

This script scrapes job listings from apsjobs.gov.au using a robust approach
that handles Salesforce Lightning components, "Load More" pagination, and comprehensive data extraction.

Features:
- Professional database structure integration
- Comprehensive job categorization for government positions
- Real browser automation with "Load More" button handling
- Complete pagination handling with dynamic Lightning component detection
- Advanced salary and location extraction from job cards and detail pages
- Robust error handling and logging with government site respect
- APS classification level mapping (APS1-6, EL1-2, SES1-3)
- Duplicate prevention and database reset options

Usage:
    python apsjobs_australia_scraper_advanced.py [max_jobs] [options]

Examples:
    python apsjobs_australia_scraper_advanced.py 30
    python apsjobs_australia_scraper_advanced.py 50 --visible
    python apsjobs_australia_scraper_advanced.py 100 --reset
"""

import os
import sys
import re
import time
import random
import uuid
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs
import logging
from decimal import Decimal
import threading
import json
import asyncio

# Set up Django environment BEFORE any Django imports
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ.setdefault('DJANGO_ALLOW_ASYNC_UNSAFE', 'true')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging with UTF-8 encoding support
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('apsjobs_scraper_advanced.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class APSJobsAustraliaScraper:
    """Professional scraper for apsjobs.gov.au job listings."""
    
    def __init__(self, max_jobs=None, headless=True):
        """
        Initialize the scraper.
        
        Args:
            max_jobs (int): Maximum number of jobs to scrape
            headless (bool): Whether to run browser in headless mode
        """
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.apsjobs.gov.au"
        self.search_url = f"{self.base_url}/s/job-search"
        self.scraped_jobs = []
        self.processed_urls = set()
        
        # APS-specific job categories mapped to Django model choices
        self.aps_categories = {
            'technology': ['it', 'technology', 'software', 'developer', 'programmer', 'system', 'cyber', 'digital', 'data'],
            'finance': ['finance', 'financial', 'accounting', 'budget', 'treasury', 'revenue', 'audit', 'procurement'],
            'healthcare': ['health', 'medical', 'clinical', 'nursing', 'therapeutic', 'dental', 'pharmacy'],
            'education': ['education', 'training', 'research', 'academic', 'learning', 'university', 'school'],
            'legal': ['legal', 'lawyer', 'solicitor', 'compliance', 'regulatory', 'policy', 'legislation'],
            'hr': ['human resources', 'hr', 'recruitment', 'people', 'workforce', 'industrial relations'],
            'other': ['administration', 'administrative', 'clerical', 'executive', 'management', 'operations',
                     'communications', 'media', 'diplomatic', 'consular', 'security', 'intelligence',
                     'defence', 'immigration', 'customs', 'border', 'transport', 'infrastructure',
                     'environment', 'agriculture', 'resources', 'energy', 'social services']
        }
        
        # APS Classification levels for experience mapping
        self.aps_levels = {
            'APS1': 'entry_level',
            'APS2': 'entry_level',
            'APS3': 'junior',
            'APS4': 'junior',
            'APS5': 'mid_level',
            'APS6': 'mid_level',
            'EL1': 'senior',
            'EL2': 'senior',
            'SES1': 'executive',
            'SES2': 'executive',
            'SES3': 'executive'
        }
        
        # Statistics tracking
        self.stats = {
            'total_found': 0,
            'successfully_scraped': 0,
            'duplicates_skipped': 0,
            'errors_encountered': 0,
            'pages_processed': 0
        }
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='apsjobs_scraper_system',
                defaults={
                    'email': 'system@apsjobsscraper.com',
                    'first_name': 'APS Jobs',
                    'last_name': 'Scraper'
                }
            )
            if created:
                logger.info("Created new system user for APS Jobs scraper")
            return user
        except Exception as e:
            logger.error(f"Failed to create system user: {e}")
            # Fallback to first available user
            return User.objects.first()

    def human_delay(self, min_seconds=2, max_seconds=6):
        """Add human-like delay between actions - conservative for government sites."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def setup_stealth_browser(self, playwright):
        """Set up browser with advanced stealth configuration for government sites."""
        browser = playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
        )
        
        context = browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-AU',
            timezone_id='Australia/Canberra'
        )
        
        # Advanced stealth injection for government sites
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-AU', 'en'] });
            window.chrome = { runtime: {} };
            delete navigator.__proto__.webdriver;
        """)
        
        return browser, context

    def navigate_to_search_page(self, page):
        """Navigate to APS Jobs search page and handle initial loading."""
        try:
            logger.info(f"[NAVIGATE] Loading APS Jobs search page: {self.search_url}")
            page.goto(self.search_url, wait_until='networkidle', timeout=90000)
            
            # Handle cookie consent if present
            try:
                cookie_selectors = [
                    "button:has-text('Accept')",
                    "button:has-text('Agree')",
                    ".cookie-accept",
                    "[id*='cookie']",
                    "[class*='cookie'] button"
                ]
                
                for selector in cookie_selectors:
                    cookie_btn = page.query_selector(selector)
                    if cookie_btn:
                        cookie_btn.click(timeout=3000)
                        self.human_delay(1, 2)
                        break
            except Exception:
                pass

            # Wait for Salesforce Lightning components to load
            logger.info("[WAIT] Waiting for Salesforce Lightning components to load...")
            self.human_delay(8, 12)
            
            # Try to trigger search to load job results
            self.trigger_job_search(page)
            
            # Additional wait for dynamic content
            logger.info("[WAIT] Waiting for job listings to render...")
            self.human_delay(5, 8)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to navigate to search page: {e}")
            return False

    def trigger_job_search(self, page):
        """Trigger job search to load results."""
        try:
            # Try multiple search trigger approaches
            search_selectors = [
                "a.search__block--button",
                ".search__block--button",
                "button[type='submit']",
                "input[type='submit']",
                ".button__full",
                "[aria-label*='Search']",
                "button:has-text('Search')",
                ".search-button"
            ]
            
            search_triggered = False
            for selector in search_selectors:
                try:
                    search_btn = page.query_selector(selector)
                    if search_btn and search_btn.is_visible():
                        logger.info(f"[SEARCH] Triggering search with selector: {selector}")
                        search_btn.click()
                        self.human_delay(8, 12)
                        search_triggered = True
                        break
                except Exception:
                    continue
            
            if not search_triggered:
                logger.info("[WARNING] Could not find search button, checking for existing results")
                
        except Exception as e:
            logger.warning(f"Could not trigger search: {e}")

    def find_job_elements(self, page):
        """Find job elements using multiple selectors for Salesforce Lightning."""
        # Comprehensive list of selectors for APS Jobs structure
        selectors_to_try = [
            "c-aps_-vacancy-feed article",
            "c-aps_-vacancy-feed div.slds-card",
            "c-aps-vacancy-card",
            "[data-record-id]",
            "div[class*='job_listing__card']",
            ".job_listing__card",
            "div[class*='vacancy']",
            "div[class*='position']",
            ".slds-card",
            "lightning-card",
            "article.slds-card",
            "div.slds-grid.slds-wrap",
            "article",
            "div[class*='job']",
            ".job-listing",
            ".job-card",
            ".job-item",
            ".opportunity",
            "[data-testid*='job']",
            ".search-result",
            ".result-item",
            "div[class*='card']",
            ".card"
        ]
        
        for selector in selectors_to_try:
            try:
                logger.info(f"[SEARCH] Trying job selector: {selector}")
                page.wait_for_selector(selector, timeout=8000)
                job_elements = page.query_selector_all(selector)
                if job_elements:
                    logger.info(f"[FOUND] Found {len(job_elements)} jobs using selector: {selector}")
                    return job_elements
            except Exception as e:
                logger.debug(f"[FAILED] Selector {selector} failed: {str(e)[:50]}...")
                continue
        
        return []

    def extract_job_data_from_cards(self, page):
        """Extract job data directly from job listing cards on the search page."""
        job_data_list = []
        
        try:
            # First try to find job elements (articles)
            job_elements = self.find_job_elements(page)
            
            if not job_elements:
                logger.warning("No job elements found, trying alternative approaches...")
                # Save debug page
                with open("apsjobs_debug_page.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                logger.info("[DEBUG] Page source saved to apsjobs_debug_page.html for debugging")
                return []
            
            logger.info(f"[EXTRACT] Processing {len(job_elements)} job cards...")
            
            # Extract data from each job card
            for i, element in enumerate(job_elements):
                try:
                    job_data = self.extract_single_card_data(element, i + 1)
                    if job_data:
                        job_data_list.append(job_data)
                        logger.info(f"[SUCCESS] Extracted job {i + 1}: {job_data.get('title', 'No title')[:50]}...")
                    else:
                        logger.warning(f"[SKIP] Failed to extract job {i + 1}")
                        
                except Exception as e:
                    logger.error(f"Error extracting data from job card {i + 1}: {e}")
                    continue
            
            logger.info(f"[RESULT] Successfully extracted {len(job_data_list)} jobs from cards")
            return job_data_list
            
        except Exception as e:
            logger.error(f"Failed to extract job data from cards: {e}")
            return []

    def extract_single_card_data(self, card_element, card_number):
        """Extract data from a single job listing card."""
        try:
            # Extract job title from h3 tag in header
            title_element = card_element.query_selector(".job_listing__card--header h3")
            title = title_element.inner_text().strip() if title_element else None
            
            if not title:
                logger.warning(f"[CARD {card_number}] No title found")
                return None
            
            # Extract company from header
            company_element = card_element.query_selector(".job_listing__card--header p.content__label.content__text--highlight")
            company = company_element.inner_text().strip() if company_element else "Government Agency"
            
            # Extract salary from lightning-formatted-number elements
            salary = self.extract_salary_from_card(card_element)
            
            # Extract job URL
            job_url = self.extract_url_from_card(card_element)
            
            # Extract job type and location from footer
            footer_data = self.extract_footer_data_from_card(card_element)
            
            # Categorize job
            job_category = self.categorize_aps_job(title, "")
            
            # Extract APS classification from title
            aps_classification = self.extract_aps_classification_from_text(title)
            
            # Build job data structure (without description for now)
            job_data = {
                'title': title,
                'company_name': company,
                'location': footer_data.get('location', 'Not specified'),
                'description': None,  # Will be filled later from detail page
                'salary_text': salary.get('raw_text', 'Not specified'),
                'salary_min': salary.get('min_amount'),
                'salary_max': salary.get('max_amount'),
                'salary_currency': 'AUD',
                'salary_type': 'yearly',
                'job_type': footer_data.get('job_type', 'full_time'),
                'posted_date': None,
                'external_url': job_url,
                'external_source': 'apsjobs.gov.au',
                'job_category': job_category,
                'experience_level': self.map_aps_level_to_experience(aps_classification),
                'keywords': [],
                'additional_info': {
                    'aps_classification': aps_classification,
                    'security_clearance': None,
                    'scraper_version': 'hybrid_extraction_v1.0',
                    'government_job': True,
                    'extracted_from': 'card_plus_detail_page'
                }
            }
            
            return job_data
            
        except Exception as e:
            logger.error(f"[CARD {card_number}] Error extracting card data: {e}")
            return None

    def extract_salary_from_card(self, card_element):
        """Extract salary from lightning-formatted-number elements in the card."""
        salary_info = {
            'raw_text': '',
            'min_amount': None,
            'max_amount': None,
            'currency': 'AUD',
            'type': 'yearly'
        }
        
        try:
            # Look for salary paragraph with lightning-formatted-number elements
            salary_paragraph = card_element.query_selector(".job_listing__card--header p.content__label.content__label--quiet")
            
            if salary_paragraph:
                # Extract lightning-formatted-number elements
                lightning_numbers = salary_paragraph.query_selector_all("lightning-formatted-number")
                
                if lightning_numbers and len(lightning_numbers) >= 2:
                    # Extract the two salary amounts
                    min_salary = lightning_numbers[0].inner_text().strip().replace(',', '')
                    max_salary = lightning_numbers[1].inner_text().strip().replace(',', '')
                    
                    salary_info['raw_text'] = f"${min_salary} to ${max_salary}"
                    salary_info['min_amount'] = int(min_salary) if min_salary.isdigit() else None
                    salary_info['max_amount'] = int(max_salary) if max_salary.isdigit() else None
                    
                elif lightning_numbers and len(lightning_numbers) == 1:
                    # Single salary amount
                    salary_amount = lightning_numbers[0].inner_text().strip().replace(',', '')
                    salary_info['raw_text'] = f"${salary_amount}"
                    salary_info['min_amount'] = int(salary_amount) if salary_amount.isdigit() else None
                    salary_info['max_amount'] = salary_info['min_amount']
                
                else:
                    # Fallback: try to extract salary from paragraph text
                    salary_text = salary_paragraph.inner_text().strip()
                    if '$' in salary_text:
                        salary_info['raw_text'] = salary_text
                        # Try to extract numbers
                        numbers = re.findall(r'[\d,]+', salary_text)
                        if numbers:
                            amounts = [int(n.replace(',', '')) for n in numbers if n.replace(',', '').isdigit()]
                            if len(amounts) >= 2:
                                salary_info['min_amount'] = min(amounts)
                                salary_info['max_amount'] = max(amounts)
                            elif len(amounts) == 1:
                                salary_info['min_amount'] = amounts[0]
                                salary_info['max_amount'] = amounts[0]
                                
        except Exception as e:
            logger.debug(f"Error extracting salary from card: {e}")
        
        return salary_info

    def extract_footer_data_from_card(self, card_element):
        """Extract job type and location from footer section."""
        footer_data = {'job_type': 'full_time', 'location': 'Not specified'}
        
        try:
            # Find footer element
            footer = card_element.query_selector("footer.job_listing__card--footer")
            if not footer:
                return footer_data
            
            # Extract all footer divs
            footer_divs = footer.query_selector_all("div")
            
            for div in footer_divs:
                try:
                    # Get the label to identify what this div contains
                    label_element = div.query_selector("p.content__label.content__text--quiet")
                    if not label_element:
                        continue
                        
                    label_text = label_element.inner_text().strip().lower()
                    
                    # Get the value (second p element)
                    value_element = div.query_selector("p:not(.content__label)")
                    if not value_element:
                        continue
                        
                    value_text = value_element.inner_text().strip()
                    
                    # Map based on label
                    if 'opportunity type' in label_text:
                        footer_data['job_type'] = self.normalize_aps_job_type(value_text)
                    elif 'location' in label_text:
                        footer_data['location'] = value_text
                        
                except Exception as e:
                    logger.debug(f"Error processing footer div: {e}")
                    continue
                    
        except Exception as e:
            logger.debug(f"Error extracting footer data: {e}")
        
        return footer_data

    def normalize_aps_job_type(self, job_type_text):
        """Normalize APS job type to standard format."""
        if not job_type_text:
            return 'full_time'
            
        job_type_lower = job_type_text.lower()
        
        # Map APS job types to standard types
        if 'full-time' in job_type_lower and 'ongoing' in job_type_lower:
            return 'full_time'
        elif 'full-time' in job_type_lower and 'non-ongoing' in job_type_lower:
            return 'contract'
        elif 'part-time' in job_type_lower:
            return 'part_time'
        elif 'casual' in job_type_lower:
            return 'casual'
        elif 'temporary' in job_type_lower:
            return 'temporary'
        elif 'contract' in job_type_lower:
            return 'contract'
        else:
            return 'full_time'  # Default for APS

    def extract_url_from_card(self, card_element):
        """Extract job URL from card link."""
        try:
            # Look for the main job link in header
            link_element = card_element.query_selector(".job_listing__card--header a")
            if link_element:
                href = link_element.get_attribute("href")
                if href:
                    if href.startswith("./"):
                        return f"{self.base_url}/s/{href[2:]}"
                    elif href.startswith("/"):
                        return f"{self.base_url}{href}"
                    elif href.startswith("http"):
                        return href
                    else:
                        return f"{self.base_url}/s/{href}"
        except Exception as e:
            logger.debug(f"Error extracting URL from card: {e}")
        
        return f"{self.base_url}/s/job-search"  # Fallback URL

    def extract_aps_classification_from_text(self, text):
        """Extract APS classification from title text."""
        if not text:
            return None
            
        # Look for APS classification patterns in title
        patterns = [
            r'APS\s*[1-6]',
            r'EL\s*[12]', 
            r'SES\s*[123]',
            r'Graduate'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0).strip()
        
        return None

    def extract_description_from_detail_page(self, page, job_url):
        """Extract full job description from the detail page using content blocks."""
        try:
            logger.info(f"[DESCRIPTION] Visiting detail page for description: {job_url}")
            page.goto(job_url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            self.human_delay(2, 4)
            
            # Extract description from job_detail__content--block elements (SELECTIVE APPROACH)
            description_text = ""
            
            # Look for content blocks ONLY within the main job detail container
            # Target: parent .job_detail__content -> child .job_detail__content--block
            main_container = page.query_selector("article.job_detail__content")
            content_blocks = []
            
            if main_container:
                # Get content blocks only from within the main job detail article
                content_blocks = main_container.query_selector_all(".job_detail__content--block")
                logger.debug(f"[DESCRIPTION] Found main container with {len(content_blocks)} content blocks")
            else:
                # Fallback: look for content blocks anywhere (previous behavior)
                content_blocks = page.query_selector_all(".job_detail__content--block")
                logger.debug(f"[DESCRIPTION] No main container found, using all content blocks: {len(content_blocks)}")
            
            if content_blocks:
                logger.debug(f"[DESCRIPTION] Found {len(content_blocks)} content blocks")
                
                for i, block in enumerate(content_blocks):
                    try:
                        # Extract ALL content from the block (headings + rich text + everything)
                        block_text = block.inner_text().strip()
                        
                        if block_text and len(block_text) > 20:  # Has substantial content
                            description_text += block_text + "\n\n"
                            logger.debug(f"[DESCRIPTION] Extracted content from block {i + 1}: {len(block_text)} chars")
                        else:
                            logger.debug(f"[DESCRIPTION] Block {i + 1} is empty or too short")
                            
                    except Exception as e:
                        logger.debug(f"Error extracting from content block {i + 1}: {e}")
                        continue
            
            # Fallback to other selectors if no content blocks found
            if not description_text:
                logger.debug("[DESCRIPTION] No content blocks found, trying fallback selectors")
                
                fallback_selectors = [
                    "article.job_detail__content",  # Main article container
                    ".job-description",
                    "[class*='description']",
                    ".job-details", 
                    ".description",
                    ".job-content",
                    ".main-content",
                    "main",
                    "article"
                ]
                
                for selector in fallback_selectors:
                    try:
                        element = page.query_selector(selector)
                        if element:
                            text = element.inner_text().strip()
                            if len(text) > 200:
                                description_text = text
                                logger.debug(f"[DESCRIPTION] Found description using fallback selector: {selector}")
                                break
                    except Exception:
                        continue
            
            # Clean and format the description
            if description_text:
                description_text = self.clean_description_text(description_text)
                logger.info(f"[DESCRIPTION] Extracted description ({len(description_text)} chars)")
                return description_text
            else:
                logger.warning("[DESCRIPTION] No description found on detail page")
                return "No description available"
                
        except Exception as e:
            logger.error(f"[DESCRIPTION] Error extracting description from {job_url}: {e}")
            return "Description extraction failed"

    def extract_job_links_from_page(self, page):
        """Extract job links from current page."""
        job_links = []
        
        try:
            # First try to find job elements
            job_elements = self.find_job_elements(page)
            
            if not job_elements:
                logger.warning("No job elements found, trying alternative approaches...")
                # Save debug page
                with open("apsjobs_debug_page.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                logger.info("[DEBUG] Page source saved to apsjobs_debug_page.html for debugging")
                return []
            
            # Extract links from job elements
            for element in job_elements:
                try:
                    # Look for record ID (Salesforce pattern)
                    record_id = element.get_attribute("data-record-id")
                    if record_id:
                        job_url = f"{self.base_url}/s/job-detail/{record_id}"
                        job_links.append(job_url)
                        continue
                    
                    # Look for traditional links
                    link_selectors = ["h3 a", "h2 a", "a[class*='title']", "a"]
                    for link_selector in link_selectors:
                        link = element.query_selector(link_selector)
                        if link:
                            href = link.get_attribute("href")
                            if href:
                                if href.startswith("http"):
                                    job_url = href
                                elif href.startswith("./"):
                                    job_url = self.base_url + "/s/" + href[2:]
                                elif href.startswith("/"):
                                    job_url = self.base_url + href
                                else:
                                    job_url = f"{self.base_url}/s/{href}"
                                
                                # Validate URL
                                if self.is_valid_job_url(job_url):
                                    job_links.append(job_url)
                                    break
                                    
                except Exception as e:
                    logger.debug(f"Error extracting link from element: {e}")
                    continue
            
            # Remove duplicates while preserving order
            unique_links = []
            seen = set()
            for link in job_links:
                if link not in seen:
                    unique_links.append(link)
                    seen.add(link)
            
            logger.info(f"[EXTRACT] Extracted {len(unique_links)} unique job links from page")
            return unique_links
            
        except Exception as e:
            logger.error(f"Failed to extract job links: {e}")
            return []

    def is_valid_job_url(self, url):
        """Validate if URL is a proper job detail URL."""
        if not url or url == self.base_url:
            return False
        
        invalid_patterns = [
            '/search',
            '/home',
            '/about',
            '/contact',
            'javascript:',
            '#'
        ]
        
        return not any(pattern in url.lower() for pattern in invalid_patterns)

    def load_more_jobs(self, page):
        """Load more jobs using the 'Load More' button (APS Jobs specific)."""
        try:
            # APS Jobs specific "Load More" button selector
            load_more_selectors = [
                "button.button__full.button__full--primary.button__load",  # Exact class match
                "button.button__load",                                     # Simplified class match
                "button:has-text('Load More')",                           # Text-based fallback
                ".button__load",                                          # Class-based fallback
                "button[class*='button__load']",                          # Partial class match
                "[class*='load']"                                         # General fallback
            ]
            
            for selector in load_more_selectors:
                try:
                    load_more_element = page.query_selector(selector)
                    if load_more_element:
                        # Check if element is visible and enabled
                        if load_more_element.is_visible() and load_more_element.is_enabled():
                            logger.info(f"[LOAD MORE] Found load more button with selector: {selector}")
                            
                            # Get current job count before loading more
                            current_jobs = len(page.query_selector_all("c-aps_-vacancy-feed article"))
                            logger.info(f"[LOAD MORE] Current jobs on page: {current_jobs}")
                            
                            # Scroll to element and click
                            load_more_element.scroll_into_view_if_needed()
                            self.human_delay(1, 2)
                            
                            load_more_element.click()
                            logger.info("[LOAD MORE] Clicked load more button, waiting for new jobs...")
                            
                            # Wait for new content to load with multiple checks
                            page.wait_for_load_state('networkidle', timeout=15000)
                            self.human_delay(2, 3)
                            
                            # Give extra time for dynamic content
                            page.wait_for_timeout(2000)
                            
                            # Check if new jobs were loaded
                            new_jobs_count = len(page.query_selector_all("c-aps_-vacancy-feed article"))
                            logger.info(f"[LOAD MORE] Jobs after loading: {new_jobs_count}")
                            
                            if new_jobs_count > current_jobs:
                                logger.info(f"[LOAD MORE] Successfully loaded {new_jobs_count - current_jobs} more jobs")
                                return True
                            else:
                                logger.info("[LOAD MORE] No new jobs loaded, reached end of results")
                                return False
                                
                except Exception as e:
                    logger.debug(f"Load more selector {selector} failed: {e}")
                    continue
            
            logger.info("[LOAD MORE] No load more button found or not clickable")
            return False
            
        except Exception as e:
            logger.warning(f"Could not load more jobs: {e}")
            return False

    def go_to_next_page(self, page):
        """Navigate to next page of results (fallback for traditional pagination)."""
        try:
            # Try APS Jobs specific "Load More" approach first
            if self.load_more_jobs(page):
                return True
            
            # Fallback to traditional pagination selectors
            next_selectors = [
                "a[aria-label='Next']",
                "button[aria-label='Next']",
                "a[rel='next']",
                "a:has-text('Next')",
                "a:has-text('>')",
                "li[class*='next'] a",
                ".pager .next a",
                ".pagination a[rel='next']",
                ".pagination a.next",
                "nav[aria-label*='Pagination'] a[rel='next']"
            ]
            
            for selector in next_selectors:
                try:
                    next_element = page.query_selector(selector)
                    if next_element and not next_element.get_attribute('disabled'):
                        next_element.scroll_into_view_if_needed()
                        self.human_delay(1, 2)
                        next_element.click()
                        page.wait_for_load_state('networkidle', timeout=60000)
                        self.human_delay(3, 5)
                        return True
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            logger.warning(f"Could not navigate to next page: {e}")
            return False

    def collect_all_job_data_from_cards(self, page):
        """Collect job data directly from cards, clicking 'Load More' until all jobs loaded."""
        all_jobs = []
        max_load_attempts = 50  # Prevent infinite loops
        load_attempts = 0
        
        logger.info("[COLLECT] Starting job collection with 'Load More' clicking...")
        
        # First, get initial jobs
        logger.info("[PAGE] Processing initial job listing...")
        initial_jobs = self.extract_job_data_from_cards(page)
        if initial_jobs:
            all_jobs.extend(initial_jobs)
            logger.info(f"[COLLECT] Found {len(initial_jobs)} initial jobs")
        
        # Keep clicking "Load More" until no more jobs or limit reached
        while load_attempts < max_load_attempts and (self.max_jobs is None or len(all_jobs) < self.max_jobs):
            load_attempts += 1
            
            logger.info(f"[LOAD MORE] Attempt {load_attempts} - Current total: {len(all_jobs)} jobs")
            
            # Try to load more jobs
            if self.go_to_next_page(page):
                logger.info("[LOAD MORE] Successfully clicked Load More button")
                
                # Extract new jobs from the updated page
                new_jobs = self.extract_job_data_from_cards(page)
                if new_jobs:
                    # Filter out jobs we already have (by URL)
                    existing_urls = {job['external_url'] for job in all_jobs}
                    truly_new_jobs = [job for job in new_jobs if job['external_url'] not in existing_urls]
                    
                    if truly_new_jobs:
                        all_jobs.extend(truly_new_jobs)
                        logger.info(f"[COLLECT] Added {len(truly_new_jobs)} new jobs (total: {len(all_jobs)})")
                    else:
                        logger.info("[COLLECT] No new unique jobs found, stopping load more")
                        break
                else:
                    logger.warning("[COLLECT] No jobs found after Load More click")
                    break
            else:
                logger.info("[LOAD MORE] No more Load More button found or reached end")
                break
        
        # Update stats
        self.stats['pages_processed'] = load_attempts + 1  # Initial + load more attempts
        
        # Trim to requested limit if we exceeded it
        if self.max_jobs is not None and len(all_jobs) > self.max_jobs:
            logger.info(f"[LIMIT] Trimming {len(all_jobs)} jobs to requested {self.max_jobs}")
            final_jobs = all_jobs[:self.max_jobs]
        else:
            final_jobs = all_jobs
        
        logger.info(f"[RESULT] Collected {len(final_jobs)} jobs after {load_attempts} Load More attempts")
        self.stats['total_found'] = len(final_jobs)
        
        return final_jobs

    def collect_all_job_links(self, page):
        """Collect job links across multiple pages."""
        all_links = []
        max_pages = 20  # Conservative limit for government sites
        pages_processed = 0
        
        while pages_processed < max_pages and (self.max_jobs is None or len(all_links) < self.max_jobs * 2):
            logger.info(f"[PAGE] Processing page {pages_processed + 1}")
            
            # Extract links from current page
            page_links = self.extract_job_links_from_page(page)
            if page_links:
                all_links.extend(page_links)
                logger.info(f"[COLLECT] Total links collected so far: {len(all_links)}")
            else:
                logger.warning("No links found on current page")
                
            pages_processed += 1
            self.stats['pages_processed'] = pages_processed
            
            # Try to go to next page
            if not self.go_to_next_page(page):
                logger.info("[PAGE] No more pages available or pagination failed")
                break
        
        # Remove duplicates
        unique_links = []
        seen = set()
        for link in all_links:
            if link not in seen:
                unique_links.append(link)
                seen.add(link)
        
        logger.info(f"[RESULT] Collected {len(unique_links)} unique job links across {pages_processed} pages")
        self.stats['total_found'] = len(unique_links)
        
        return unique_links if self.max_jobs is None else unique_links[:self.max_jobs * 2]  # Get extra links in case some fail

    def extract_job_data_from_detail_page(self, page, job_url):
        """Extract comprehensive job data from detail page."""
        try:
            logger.info(f"[VISIT] Visiting job detail: {job_url}")
            page.goto(job_url, wait_until='networkidle', timeout=60000)
            
            # Wait for content to load
            self.human_delay(3, 5)
            
            # Extract job title
            title = self.extract_title(page)
            if not title:
                logger.warning(f"No valid title found for {job_url}, skipping job")
                return None
            
            # Extract other data
            company = self.extract_company(page)
            if not company:
                logger.warning(f"No company found for {job_url}, using URL as fallback")
                company = f"Government Agency (via {urlparse(job_url).netloc})"
            location = self.extract_location(page)
            description = self.extract_description(page)
            salary_info = self.extract_salary(page)
            job_type = self.extract_job_type(page, description)
            posted_date = self.extract_posted_date(page)
            aps_classification = self.extract_aps_classification(page, title, description)
            security_clearance = self.extract_security_clearance(page, description)
            
            # Categorize job
            job_category = self.categorize_aps_job(title, description)
            
            # Extract keywords and tags
            keywords = JobCategorizationService.get_job_keywords(title, description)
            aps_keywords = self.extract_aps_keywords(description)
            all_keywords = list(set(keywords + aps_keywords))[:10]
            
            job_data = {
                'title': title,
                'company_name': company,
                'location': location,
                'description': description,
                'salary_text': salary_info['raw_text'],
                'salary_min': salary_info['min_amount'],
                'salary_max': salary_info['max_amount'],
                'salary_currency': salary_info['currency'],
                'salary_type': salary_info['type'],
                'job_type': job_type,
                'posted_date': posted_date,
                'external_url': job_url,
                'external_source': 'apsjobs.gov.au',
                'job_category': job_category,
                'experience_level': self.map_aps_level_to_experience(aps_classification),
                'keywords': all_keywords,
                'additional_info': {
                    'aps_classification': aps_classification,
                    'security_clearance': security_clearance,
                    'scraper_version': 'advanced_v1.0',
                    'government_job': True
                }
            }
            
            logger.info(f"[EXTRACTED] {title} | {company} | {location}")
            return job_data
            
        except Exception as e:
            logger.error(f"Failed to extract job data from {job_url}: {e}")
            return None

    def extract_title(self, page):
        """Extract job title with APS Jobs specific selectors."""
        title_selectors = [
            # APS Jobs specific structure from job detail page
            ".job_listing__card--header h3",
            ".job_listing__card--header a h3",
            "article .job_listing__card--header h3",
            # Salesforce Lightning structure
            ".slds-page-header__title",
            "c-aps_-job-details-header h1",
            # Generic fallbacks
            "h1",
            "h2", 
            "h3",
            ".job-title",
            ".position-title"
        ]
        
        for selector in title_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    # Filter out notification text and generic phrases
                    excluded_phrases = [
                        'view job', 'job details', 'apply now', 
                        'register to receive notifications',
                        'register to receive',
                        'notifications for jobs',
                        'save this search',
                        'sign in'
                    ]
                    if text and len(text) > 5 and not any(phrase in text.lower() for phrase in excluded_phrases):
                        return text
            except Exception:
                continue
        
        # DO NOT use page title fallback as it contains notification text
        # Return None if no proper title found - this will be handled upstream
        return None

    def extract_company(self, page):
        """Extract department/agency name."""
        company_selectors = [
            # APS Jobs specific selectors from the actual HTML structure
            ".job_listing__card--header p.content__label.content__text--highlight",
            ".job_listing__card--header .content__text--highlight",
            "article .job_listing__card--header p.content__text--highlight",
            "p.content__label.content__text--highlight",
            ".content__text--highlight",
            ".content__label",
            # Generic selectors
            ".agency-name",
            ".department",
            "[data-testid='agency']",
            ".employer",
            "[class*='company']",
            "[class*='employer']",
            "[class*='agency']"
        ]
        
        for selector in company_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    if text and text not in ['Company', 'Department', 'Agency']:
                        return text
            except Exception:
                continue
        
        # Return None instead of static fallback - let upstream handle missing data
        return None

    def extract_location(self, page):
        """Extract location with government-specific patterns."""
        location_selectors = [
            # APS Jobs specific footer structure
            ".job_listing__card--footer div:has(p.content__label.content__text--quiet:contains('Location')) p:not(.content__label)",
            "footer .job_listing__card--footer div p:last-child",
            ".content__location",
            ".job_listing__card--header .content__location",
            "p.content__location",
            ".location",
            "[class*='location']",
            ".job-location",
            "[data-testid='location']"
        ]
        
        for selector in location_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    # Clean up location text
                    text = re.sub(r'\s*,?\s*(Australia|NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s*$', '', text, flags=re.IGNORECASE)
                    text = text.strip()
                    if text and text.lower() not in ['location', 'opportunity type']:
                        return text
            except Exception:
                continue
        
        # Try to find location in footer specifically
        try:
            footer_elements = page.query_selector_all('.job_listing__card--footer div')
            for div in footer_elements:
                label = div.query_selector('p.content__label.content__text--quiet')
                if label and 'location' in label.inner_text().lower():
                    location_p = div.query_selector('p:not(.content__label)')
                    if location_p:
                        return location_p.inner_text().strip()
        except Exception:
            pass
        
        return None  # Don't use static fallback

    def extract_description(self, page):
        """Extract job description."""
        description_selectors = [
            ".job-description",
            "[class*='description']",
            ".job-details",
            ".description",
            ".job-content",
            ".main-content",
            "[class*='content']",
            ".job-detail-content",
            "main",
            "article"
        ]
        
        for selector in description_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    if len(text) > 200:
                        return self.clean_description_text(text)
            except Exception:
                continue
        
        # Fallback to full page text
        try:
            body_text = page.inner_text('body')
            if body_text and len(body_text) > 300:
                return self.clean_description_text(body_text)
        except Exception:
            pass
        
        return "No description available"

    def extract_salary(self, page):
        """Extract salary with APS-specific patterns."""
        salary_info = {
            'raw_text': '',
            'min_amount': None,
            'max_amount': None,
            'currency': 'AUD',
            'type': 'yearly'
        }
        
        # First try APS Jobs specific salary selectors
        salary_selectors = [
            # APS Jobs specific salary structure with lightning-formatted-number
            ".job_listing__card--header p.content__label.content__label--quiet",
            "p.content__label.content__label--quiet",
            ".content__label--quiet",
            ".salary",
            "[class*='salary']",
            ".remuneration",
            "[data-testid='salary']"
        ]
        
        for selector in salary_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    text = element.inner_text().strip()
                    # Also check for lightning-formatted-number elements
                    lightning_numbers = element.query_selector_all('lightning-formatted-number')
                    if lightning_numbers:
                        # Extract numbers from lightning-formatted-number elements
                        numbers = [num.inner_text().strip() for num in lightning_numbers]
                        if len(numbers) >= 2:
                            salary_info['raw_text'] = f"${numbers[0]} to ${numbers[1]}"
                            break
                        elif len(numbers) == 1:
                            salary_info['raw_text'] = f"${numbers[0]}"
                            break
                    elif text and '$' in text:
                        salary_info['raw_text'] = text
                        break
            except Exception:
                continue
        
        # Extract from full page content if not found
        if not salary_info['raw_text']:
            try:
                page_content = page.inner_text('body')
                salary_patterns = [
                    r'APS\s*\d+\s*\$[\d,]+\s*-\s*\$[\d,]+',
                    r'EL\s*\d+\s*\$[\d,]+\s*-\s*\$[\d,]+',
                    r'SES\s*\d+\s*\$[\d,]+\s*-\s*\$[\d,]+',
                    r'\$[\d,]+\s*-\s*\$[\d,]+\s*per\s+annum',
                    r'\$[\d,]+\s*-\s*\$[\d,]+\s*p\.a\.',
                    r'\$[\d,]+\s*-\s*\$[\d,]+',
                    r'\$[\d,]+\s*per\s+annum',
                    r'\$[\d,]+\s*p\.a\.'
                ]
                
                for pattern in salary_patterns:
                    match = re.search(pattern, page_content, re.IGNORECASE)
                    if match:
                        potential_salary = match.group(0).strip()
                        # Validate salary range
                        numbers = re.findall(r'[\d,]+', potential_salary)
                        if numbers:
                            first_num = int(numbers[0].replace(',', ''))
                            if 40000 <= first_num <= 500000:
                                salary_info['raw_text'] = potential_salary
                                break
            except Exception:
                pass
        
        # Parse salary amounts
        if salary_info['raw_text']:
            try:
                numbers = re.findall(r'[\d,]+', salary_info['raw_text'])
                if numbers:
                    amounts = [int(n.replace(',', '')) for n in numbers]
                    if len(amounts) >= 2:
                        salary_info['min_amount'] = min(amounts)
                        salary_info['max_amount'] = max(amounts)
                    elif len(amounts) == 1:
                        salary_info['min_amount'] = amounts[0]
                        salary_info['max_amount'] = amounts[0]
            except Exception:
                pass
        
        return salary_info

    def extract_job_type(self, page, description):
        """Extract job type with APS-specific patterns."""
        try:
            # First try to get job type from footer structure
            footer_elements = page.query_selector_all('.job_listing__card--footer div')
            for div in footer_elements:
                label = div.query_selector('p.content__label.content__text--quiet')
                if label and 'opportunity type' in label.inner_text().lower():
                    job_type_p = div.query_selector('p:not(.content__label)')
                    if job_type_p:
                        job_type_text = job_type_p.inner_text().strip()
                        # Map APS job types to standard types
                        if 'full-time' in job_type_text.lower():
                            if 'ongoing' in job_type_text.lower():
                                return 'full_time'
                            elif 'non-ongoing' in job_type_text.lower():
                                return 'contract'
                        elif 'part-time' in job_type_text.lower():
                            return 'part_time'
                        return job_type_text.lower().replace('-', '_').replace(';', '_').replace(',', '_')
            
            # Fallback to content analysis
            content = page.inner_text('body') + " " + description
            content_lower = content.lower()
            
            # APS-specific job type patterns
            if any(term in content_lower for term in ['ongoing', 'permanent']):
                return 'full_time'
            elif any(term in content_lower for term in ['non-ongoing', 'non ongoing', 'fixed term', 'specified term']):
                return 'contract'
            elif 'temporary' in content_lower:
                return 'temporary'
            elif 'casual' in content_lower:
                return 'casual'
            elif 'part' in content_lower and 'time' in content_lower:
                return 'part_time'
            
            return 'full_time'  # Default for APS
            
        except Exception:
            return 'full_time'

    def extract_posted_date(self, page):
        """Extract posted date."""
        date_selectors = [
            ".posted-date",
            "[class*='posted']",
            "[class*='date']",
            "[data-testid*='date']"
        ]
        
        for selector in date_selectors:
            try:
                element = page.query_selector(selector)
                if element:
                    date_text = element.inner_text().strip()
                    return self.parse_date(date_text)
            except Exception:
                continue
        
        return None

    def extract_aps_classification(self, page, title, description):
        """Extract APS classification level."""
        try:
            content = f"{title} {description} {page.inner_text('body')}"
            
            # Look for APS classification patterns
            patterns = [
                r'APS\s*[1-6]',
                r'EL\s*[12]',
                r'SES\s*[123]',
                r'Executive Level [12]',
                r'Senior Executive Service [123]'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    classification = match.group(0).upper()
                    # Normalize format
                    classification = re.sub(r'\s+', '', classification)
                    return classification
            
            return None
            
        except Exception:
            return None

    def extract_security_clearance(self, page, description):
        """Extract security clearance requirements."""
        try:
            content = f"{description} {page.inner_text('body')}".lower()
            
            clearance_patterns = [
                'negative vetting',
                'positive vetting',
                'secret clearance',
                'top secret',
                'baseline clearance',
                'security clearance'
            ]
            
            found_clearances = []
            for pattern in clearance_patterns:
                if pattern in content:
                    found_clearances.append(pattern.title())
            
            return found_clearances
            
        except Exception:
            return []

    def categorize_aps_job(self, title, description):
        """Categorize APS job using government-specific logic."""
        # Use the standard categorization service first
        standard_category = JobCategorizationService.categorize_job(title, description)
        
        # If it's 'other', try APS-specific categorization
        if standard_category == 'other':
            content = f"{title} {description}".lower()
            
            for category, keywords in self.aps_categories.items():
                score = 0
                for keyword in keywords:
                    if keyword in content:
                        score += 1
                        if keyword in title.lower():
                            score += 2  # Extra weight for title matches
                
                if score > 0 and category != 'other':
                    return category
        
        return standard_category

    def extract_aps_keywords(self, content):
        """Extract APS-specific keywords."""
        content_lower = content.lower()
        aps_keywords = []
        
        keyword_categories = {
            'government': ['policy', 'legislation', 'regulation', 'compliance', 'governance'],
            'security': ['clearance', 'vetting', 'security', 'intelligence', 'defence'],
            'administration': ['administrative', 'executive', 'management', 'operations'],
            'service_delivery': ['services', 'delivery', 'client', 'customer', 'citizen']
        }
        
        for category, keywords in keyword_categories.items():
            for keyword in keywords:
                if keyword in content_lower:
                    aps_keywords.append(keyword.title())
        
        return list(set(aps_keywords))

    def map_aps_level_to_experience(self, classification):
        """Map APS classification to experience level."""
        if not classification:
            return 'mid_level'
        
        classification_clean = re.sub(r'\s+', '', classification.upper())
        return self.aps_levels.get(classification_clean, 'mid_level')

    def clean_description_text(self, text):
        """Clean and format description text."""
        if not text:
            return text
        
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', text).strip()
        
        # Remove common unwanted phrases
        unwanted_phrases = [
            "Press space or enter keys to toggle section visibility",
            "Loading",
            "×Sorry to interrupt",
            "CSS Error",
            "Refresh"
        ]
        
        for phrase in unwanted_phrases:
            cleaned = cleaned.replace(phrase, '')
        
        return cleaned.strip()

    def parse_date(self, date_str):
        """Parse date string to datetime object."""
        if not date_str:
            return None
        
        date_str = date_str.strip().lower()
        
        try:
            # Handle relative dates
            if "ago" in date_str:
                if "today" in date_str or "0 day" in date_str:
                    return timezone.now().date()
                elif "yesterday" in date_str or "1 day" in date_str:
                    return (timezone.now() - timedelta(days=1)).date()
                elif "day" in date_str:
                    days = int(re.search(r'(\d+)', date_str).group(1))
                    return (timezone.now() - timedelta(days=days)).date()
                elif "week" in date_str:
                    weeks = int(re.search(r'(\d+)', date_str).group(1))
                    return (timezone.now() - timedelta(weeks=weeks)).date()
                elif "month" in date_str:
                    months = int(re.search(r'(\d+)', date_str).group(1))
                    return (timezone.now() - timedelta(days=months*30)).date()
            
            # Try different date formats
            date_formats = [
                "%d %B %Y",
                "%B %d, %Y",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%Y-%m-%d"
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except:
                    continue
                    
        except Exception:
            pass
        
        return None

    def get_or_create_company(self, company_name):
        """Get or create company record."""
        try:
            company, created = Company.objects.get_or_create(
                name=company_name,
                defaults={
                    'slug': slugify(company_name),
                    'company_size': 'large',  # Government departments are typically large
                    'description': f'{company_name} - Australian Government Department'
                }
            )
            return company
        except Exception as e:
            logger.error(f"Failed to create company {company_name}: {e}")
            return None

    def get_or_create_location(self, location_name):
        """Get or create location record."""
        if not location_name:
            return None
        
        try:
            location, created = Location.objects.get_or_create(
                name=location_name,
                defaults={
                    'city': location_name.split(',')[0].strip() if ',' in location_name else location_name,
                    'country': 'Australia'
                }
            )
            return location
        except Exception as e:
            logger.error(f"Failed to create location {location_name}: {e}")
            return None

    def save_job_to_database(self, job_data):
        """Save job data to database with comprehensive error handling."""
        import threading
        from concurrent.futures import ThreadPoolExecutor
        
        def _save_job_sync():
            try:
                with transaction.atomic():
                    # Check for duplicates (silently skip without warning)
                    if JobPosting.objects.filter(external_url=job_data['external_url']).exists():
                        logger.debug(f"Duplicate job found, skipping: {job_data['external_url']}")
                        self.stats['duplicates_skipped'] += 1
                        return "duplicate"
                    
                    # Get or create related objects
                    company = self.get_or_create_company(job_data['company_name'])
                    if not company:
                        logger.error(f"Failed to create company: {job_data['company_name']}")
                        return False
                    
                    location = self.get_or_create_location(job_data['location'])
                    
                    # Create job posting
                    job_posting = JobPosting.objects.create(
                        title=job_data['title'],
                        description=job_data['description'],
                        company=company,
                        location=location,
                        posted_by=self.system_user,
                        job_category=job_data['job_category'],
                        job_type=job_data['job_type'],
                        experience_level=job_data['experience_level'],
                        salary_min=job_data['salary_min'],
                        salary_max=job_data['salary_max'],
                        salary_currency=job_data['salary_currency'],
                        salary_type=job_data['salary_type'],
                        salary_raw_text=job_data['salary_text'],
                        external_source=job_data['external_source'],
                        external_url=job_data['external_url'],
                        status='active',
                        date_posted=job_data.get('posted_date'),
                        tags=','.join(job_data['keywords']),
                        additional_info=job_data['additional_info']
                    )
                    
                    logger.info(f"[SUCCESS] Saved job: {job_posting.title} at {company.name}")
                    self.stats['successfully_scraped'] += 1
                    return True
                    
            except Exception as e:
                logger.error(f"Failed to save job: {e}")
                self.stats['errors_encountered'] += 1
                return False
        
        # Execute database operation in a separate thread to avoid async context issues
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_save_job_sync)
                return future.result(timeout=30)
        except Exception as e:
            logger.error(f"Thread execution failed: {e}")
            self.stats['errors_encountered'] += 1
            return False

    def run_scraper(self):
        """Main scraper execution method."""
        start_time = datetime.now()
        
        logger.info("[START] Professional APS Jobs Australia Scraper")
        logger.info("=" * 60)
        logger.info(f"Target: {self.max_jobs or 'No limit'} jobs from apsjobs.gov.au")
        logger.info(f"Database: Professional structure with enhanced categorization")
        logger.info(f"Features: Government-focused, respectful crawling, APS classification")
        logger.info("=" * 60)
        
        with sync_playwright() as playwright:
            browser, context = self.setup_stealth_browser(playwright)
            page = context.new_page()
            
            try:
                # Navigate to search page
                if not self.navigate_to_search_page(page):
                    logger.error("Failed to navigate to search page")
                    return
                
                # Collect job data directly from cards (NEW APPROACH)
                logger.info("[COLLECT] Collecting job data from listing cards...")
                job_data_list = self.collect_all_job_data_from_cards(page)
                
                if not job_data_list:
                    logger.error("No job data found on cards")
                    return
                
                logger.info(f"[PROCESS] Processing {len(job_data_list)} jobs from cards...")
                
                # Process each job data - add descriptions from detail pages
                processed_count = 0
                for i, job_data in enumerate(job_data_list):
                    logger.info(f"\n[JOB] Processing job {i + 1}/{len(job_data_list)}")
                    
                    if job_data:
                        # Extract description from detail page (HYBRID APPROACH)
                        if job_data['external_url']:
                            try:
                                description = self.extract_description_from_detail_page(page, job_data['external_url'])
                                job_data['description'] = description
                                
                                # Update keywords now that we have description
                                keywords = JobCategorizationService.get_job_keywords(job_data['title'], description)
                                job_data['keywords'] = keywords[:10]  # Limit to 10 keywords
                                
                                logger.info(f"  [DESCRIPTION] Added description ({len(description)} chars)")
                            except Exception as e:
                                logger.warning(f"  [DESCRIPTION] Failed to get description: {e}")
                                job_data['description'] = f"Job posted on apsjobs.gov.au - {job_data['title']}"
                        
                        # Save to database
                        save_result = self.save_job_to_database(job_data)
                        if save_result == True:
                            processed_count += 1
                            logger.info(f"  [SUCCESS] Successfully saved: {job_data['title']}")
                            logger.info(f"  [LOCATION] Location: {job_data['location']}")
                            logger.info(f"  [COMPANY] Department: {job_data['company_name']}")
                            logger.info(f"  [SALARY] Salary: {job_data['salary_text'] or 'Not specified'}")
                        elif save_result == "duplicate":
                            processed_count += 1  # Count duplicates as processed to avoid warnings
                            logger.debug(f"  [DUPLICATE] Skipped duplicate job: {job_data['title']}")
                        else:
                            logger.warning(f"  [ERROR] Failed to save job: {job_data['title']}")
                    else:
                        logger.info(f"  [ERROR] No job data available")
                    
                    # Human-like delay between jobs (important for government sites)
                    self.human_delay(2, 4)
                
            except Exception as e:
                logger.error(f"Scraper error: {e}")
                self.stats['errors_encountered'] += 1
            
            finally:
                try:
                    browser.close()
                except Exception:
                    pass
        
        # Print final statistics
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("[COMPLETE] APS JOBS SCRAPING COMPLETED!")
        logger.info("=" * 60)
        logger.info(f"[STATS] Pages processed: {self.stats['pages_processed']}")
        logger.info(f"[STATS] Total jobs found: {self.stats['total_found']}")
        logger.info(f"[STATS] Jobs successfully scraped: {self.stats['successfully_scraped']}")
        logger.info(f"[STATS] Duplicate jobs skipped: {self.stats['duplicates_skipped']}")
        logger.info(f"[STATS] Errors encountered: {self.stats['errors_encountered']}")
        logger.info(f"[STATS] Total APS jobs in database: {JobPosting.objects.filter(external_source='apsjobs.gov.au').count()}")
        logger.info(f"[STATS] Scraping duration: {duration}")
        
        if self.stats['successfully_scraped'] > 0:
            success_rate = (self.stats['successfully_scraped'] / max(self.stats['total_found'], 1)) * 100
            logger.info(f"[STATS] Success rate: {success_rate:.1f}%")
        
        logger.info("=" * 60)


def reset_database():
    """Reset/clear all APS Jobs data from database."""
    try:
        deleted_count = JobPosting.objects.filter(external_source='apsjobs.gov.au').count()
        JobPosting.objects.filter(external_source='apsjobs.gov.au').delete()
        logger.info(f"[RESET] Cleared {deleted_count} APS Jobs from database")
        return True
    except Exception as e:
        logger.error(f"[RESET] Failed to clear database: {e}")
        return False

def main():
    """Main function to run the scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='APS Jobs Australia Professional Scraper')
    parser.add_argument('max_jobs', type=int, nargs='?', default=30,
                       help='Maximum number of jobs to scrape (default: 30)')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--visible', action='store_true',
                       help='Run browser in visible mode for debugging')
    parser.add_argument('--reset', action='store_true',
                       help='Clear all existing APS Jobs data before scraping')
    
    args = parser.parse_args()
    
    # Handle database reset if requested
    if args.reset:
        logger.info("[RESET] Clearing existing APS Jobs data...")
        if not reset_database():
            logger.error("[RESET] Failed to reset database, exiting")
            return
    
    # Override headless if visible flag is set
    headless = args.headless and not args.visible
    
    logger.info(f"[START] Starting APS Jobs scraper...")
    logger.info(f"Target jobs: {args.max_jobs}")
    logger.info(f"Headless mode: {headless}")
    if args.reset:
        logger.info("[MODE] Database reset mode - starting fresh")
    
    scraper = APSJobsAustraliaScraper(max_jobs=args.max_jobs, headless=headless)
    scraper.run_scraper()


def run(max_jobs=None, headless=True):
    """Automation entrypoint for APS Jobs scraper.

    Runs the scraper without CLI, returning the internal stats dict for schedulers.
    """
    try:
        scraper = APSJobsAustraliaScraper(max_jobs=max_jobs, headless=headless)
        scraper.run_scraper()
        return {
            'success': True,
            'stats': getattr(scraper, 'stats', {}),
            'message': 'APS Jobs scraping completed'
        }
    except SystemExit as e:
        return {
            'success': int(getattr(e, 'code', 1)) == 0,
            'exit_code': getattr(e, 'code', 1)
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

if __name__ == "__main__":
    main()
