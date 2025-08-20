#!/usr/bin/env python3
"""
Professional Prosple Australia Job Scraper using Playwright
===========================================================

Advanced Playwright-based scraper for Prosple Australia (https://au.prosple.com/search-jobs) 
that integrates with your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Graduate and entry-level job optimization

Features:
- 🎯 Smart job data extraction from Prosple Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional graduate job categorization

Usage:
    python prosple_australia_scraper.py [job_limit]
    
Examples:
    python prosple_australia_scraper.py 20    # Scrape 20 jobs
    python prosple_australia_scraper.py       # Scrape all available jobs
"""

import os
import sys
import django
import time
import random
import logging
import re
import json
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
        logging.FileHandler('prosple_australia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProspleAustraliaScraper:
    """
    Professional scraper for Prosple Australia job listings
    """
    
    def __init__(self, max_jobs=None, headless=True, max_pages=None):
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.headless = headless
        self.base_url = "https://au.prosple.com"
        self.search_url = "https://au.prosple.com/search-jobs"
        self.search_url_with_location = "https://au.prosple.com/search-jobs?keywords=&locations=9692&defaults_applied=1"
        # Updated user agent to latest Chrome version
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        
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
            username='prosple_australia_scraper',
            defaults={'email': 'scraper@prosple.com.au'}
        )
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        logger.info("Prosple Australia Scraper initialized")
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
        
        # Common salary patterns for graduate/entry-level positions
        patterns = [
            r'AUD\s*([\d,]+)\s*-\s*([\d,]+)',      # AUD 68,000 - 80,000
            r'(\$[\d,]+)\s*-\s*(\$[\d,]+)',        # $50,000 - $60,000
            r'(\$[\d,]+)\s*to\s*(\$[\d,]+)',       # $50,000 to $60,000
            r'AUD\s*([\d,]+)\s*\+',                # AUD 50,000+
            r'(\$[\d,]+)\s*\+',                    # $50,000+
            r'AUD\s*([\d,]+)',                     # AUD 50,000
            r'(\$[\d,]+)',                         # $50,000
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # 50-60k
            r'(\d{1,3}(?:,\d{3})*)\s*k',          # 50k
        ]
        
        salary_min = None
        salary_max = None
        salary_type = 'yearly'
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 2:
                        if 'k' in text.lower():
                            salary_min = Decimal(match.group(1).replace(',', '')) * 1000
                            salary_max = Decimal(match.group(2).replace(',', '')) * 1000
                        else:
                            # Handle both AUD and $ prefixes
                            salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                            salary_max = Decimal(match.group(2).replace('$', '').replace(',', ''))
                    else:
                        if 'k' in text.lower():
                            salary_min = Decimal(match.group(1).replace(',', '')) * 1000
                        else:
                            # Handle both AUD and $ prefixes
                            salary_min = Decimal(match.group(1).replace('$', '').replace(',', ''))
                        if '+' in text:
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
            # Clean up date text
            date_text = date_text.strip().replace('Applications close:', '').replace('Closes:', '').strip()
            
            # Try different date formats
            formats = [
                '%d %b %Y',       # 27 Sep 2025
                '%d %B %Y',       # 27 September 2025
                '%d/%m/%Y',       # 27/09/2025
                '%d-%m-%Y',       # 27-09-2025
                '%Y-%m-%d',       # 2025-09-27
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
                description=f'Organization posting graduate and professional jobs on Prosple Australia'
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

    def extract_job_ids_from_analytics(self, api_requests):
        """Extract job IDs from analytics requests"""
        try:
            logger.info("Attempting to extract job IDs from analytics data...")
            
            job_ids = []
            for request in api_requests:
                if 'analytics.google.com' in request and 'content_list=' in request:
                    logger.info(f"Found analytics request with content_list: {request[:200]}...")
                    
                    # Look for content_list in the URL directly
                    if 'ep.content_list=' in request:
                        # Extract content_list value
                        start = request.find('ep.content_list=') + len('ep.content_list=')
                        end = request.find('&', start)
                        if end == -1:
                            end = len(request)
                        
                        content_list = request[start:end]
                        
                        # Split the content list into individual job IDs (URL encoded comma %2C)
                        ids = content_list.split('%2C')
                        for job_id in ids:
                            job_id = job_id.strip()
                            if job_id.isdigit():
                                job_ids.append(job_id)
                        
                        logger.info(f"Found {len(ids)} job IDs in analytics: {ids[:5]}...")
                        break
            
            # Remove duplicates and return
            unique_ids = list(set(job_ids))
            logger.info(f"Extracted {len(unique_ids)} unique job IDs from analytics")
            return unique_ids
            
        except Exception as e:
            logger.error(f"Error extracting job IDs from analytics: {e}")
            return []

    def fetch_job_data_from_api(self, job_ids, page):
        """Fetch job data using the GraphQL API"""
        try:
            logger.info(f"Attempting to fetch job data for {len(job_ids)} jobs from API...")
            
            jobs = []
            for job_id in job_ids[:self.max_jobs if self.max_jobs else len(job_ids)]:
                try:
                    # Construct individual job URL - try different patterns
                    possible_urls = [
                        f"https://au.prosple.com/opportunities/{job_id}",
                        f"https://au.prosple.com/jobs/{job_id}",
                        f"https://au.prosple.com/career-opportunities/{job_id}",
                        f"https://au.prosple.com/graduate-opportunities/{job_id}"
                    ]
                    
                    for job_url in possible_urls:
                        try:
                            logger.info(f"Trying job URL: {job_url}")
                            response = page.goto(job_url, timeout=30000, wait_until='networkidle')
                            
                            if response and response.status == 200:
                                # Extract job details from the page
                                job_details = self.get_job_details(job_url, page)
                                
                                # Get basic job info
                                title_element = page.query_selector('h1, h2, [data-testid="job-title"]')
                                title = title_element.inner_text().strip() if title_element else f"Job {job_id}"
                                
                                if title and len(title) > 2:
                                    job_data = {
                                        'title': title,
                                        'url': job_url,
                                        'id': job_id
                                    }
                                    jobs.append(job_data)
                                    logger.info(f"Successfully extracted job: {title}")
                                    break
                                    
                        except Exception as e:
                            logger.debug(f"URL {job_url} failed: {e}")
                            continue
                    
                    # Add delay between requests
                    self.human_delay(2, 4)
                    
                except Exception as e:
                    logger.error(f"Error processing job ID {job_id}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(jobs)} jobs from API")
            return jobs
            
        except Exception as e:
            logger.error(f"Error fetching job data from API: {e}")
            return []

    def extract_jobs_from_api_response(self, api_data):
        """Extract job data from GraphQL API response"""
        try:
            jobs = []
            logger.info(f"Starting API response extraction. Data type: {type(api_data)}")
            
            # Handle both list and dict API response structures
            opportunities = None
            
            if isinstance(api_data, list):
                # Direct list of opportunities (GraphQL API response)
                opportunities = api_data
                logger.info(f"API response is a direct list with {len(opportunities)} items")
            elif isinstance(api_data, dict):
                # Try different possible paths for the opportunities
                # Common GraphQL response paths
                possible_paths = [
                    ['data', 'searchOpportunities', 'results'],
                    ['data', 'searchOpportunities', 'opportunities'],
                    ['data', 'opportunities'],
                    ['searchOpportunities', 'results'],
                    ['opportunities'],
                    ['results']
                ]
                
                for path in possible_paths:
                    try:
                        temp_data = api_data
                        for key in path:
                            temp_data = temp_data[key]
                        if isinstance(temp_data, list) and len(temp_data) > 0:
                            opportunities = temp_data
                            logger.info(f"Found opportunities using path: {' -> '.join(path)}")
                            break
                    except (KeyError, TypeError):
                        continue
            
            if opportunities:
                logger.info(f"Found {len(opportunities)} opportunities in API response")
                
                # Process each opportunity using the same logic as Next.js extraction
                for opp in opportunities:
                    try:
                        job_data = self.extract_job_from_nextjs_opportunity(opp, {}, None)  # No apollo_state for API responses
                        if job_data:
                            jobs.append(job_data)
                            logger.info(f"Successfully extracted API job: {job_data.get('title', 'Unknown')} at {job_data.get('company', 'Unknown')}")
                    except Exception as e:
                        logger.info(f"Error processing API opportunity: {e}")
                        continue
            else:
                logger.warning("Could not find opportunities in API response")
                logger.info(f"API response keys: {list(api_data.keys()) if isinstance(api_data, dict) else 'Not a dict'}")
                # Print a sample of the API response structure for debugging
                if isinstance(api_data, dict):
                    import json
                    try:
                        # Try to pretty print the structure for better debugging
                        logger.info(f"API response structure:\n{json.dumps(api_data, indent=2)[:1000]}...")
                    except:
                        logger.info(f"API response sample structure: {str(api_data)[:1000]}...")
                elif isinstance(api_data, list):
                    logger.info(f"API response is a list with {len(api_data)} items")
                    if len(api_data) > 0:
                        logger.info(f"First item sample: {str(api_data[0])[:500]}...")
            
            logger.info(f"API extraction completed. Found {len(jobs)} jobs")
            return jobs
            
        except Exception as e:
            logger.warning(f"Error extracting jobs from API response: {e}")
            return []

    def extract_jobs_from_nextjs_data(self, page):
        """Extract job data from Next.js __NEXT_DATA__ script tag"""
        try:
            logger.info("Attempting to extract jobs from Next.js data...")
            
            # Get the __NEXT_DATA__ script content
            next_data_script = page.query_selector('script#__NEXT_DATA__')
            if not next_data_script:
                logger.warning("No __NEXT_DATA__ script found")
                return []
            
            # Parse the JSON data
            json_content = next_data_script.inner_text()
            data = json.loads(json_content)
            logger.info("Successfully parsed Next.js JSON data")
            
            # Extract jobs from the correct path: props.pageProps.initialResult.opportunities
            jobs = []
            
            try:
                opportunities = data['props']['pageProps']['initialResult']['opportunities']
                apollo_state = data['props']['pageProps']['initialApolloState']
                logger.info(f"Found {len(opportunities)} opportunities in Next.js data")
                
                def resolve_ref(ref_obj, apollo_state):
                    """Resolve Apollo references"""
                    if isinstance(ref_obj, dict) and '__ref' in ref_obj:
                        ref_key = ref_obj['__ref']
                        return apollo_state.get(ref_key, {})
                    return ref_obj
                
                for opp in opportunities:
                    try:
                        job_data = self.extract_job_from_nextjs_opportunity(opp, apollo_state, resolve_ref)
                        if job_data:
                            jobs.append(job_data)
                            logger.info(f"SUCCESS: Extracted job: {job_data['title']} at {job_data['company']}")
                    except Exception as e:
                        logger.warning(f"Failed to extract job from opportunity: {e}")
                        continue
            
            except KeyError as e:
                logger.error(f"Expected data structure not found: {e}")
                return []
            
            return jobs
                
        except Exception as e:
            logger.error(f"Error extracting from Next.js data: {e}")
            return []
    
    def extract_job_from_nextjs_opportunity(self, opp, apollo_state, resolve_ref):
        """Extract job data from a single Next.js opportunity object"""
        try:
            job_data = {
                'title': opp.get('title', 'Unknown Title'),
                'company': 'Unknown Company',
                'location': opp.get('locationDescription', 'Unknown Location'),
                'url': None,
                'salary_min': None,
                'salary_max': None,
                'salary_currency': 'AUD',
                'description': opp.get('description', ''),
                'application_deadline': opp.get('applicationsCloseDate', ''),
                'job_type': 'Unknown',
                'remote_available': opp.get('remoteAvailable', False),
                'sponsored': opp.get('sponsored', False),
                'min_vacancies': opp.get('minNumberVacancies'),
                'max_vacancies': opp.get('maxNumberVacancies')
            }
            
            # Get job ID
            job_id = opp.get('id')
            
            # Construct URL using the correct detailPageURL format
            detail_page_url = opp.get('detailPageURL')
            if detail_page_url:
                job_data['url'] = f"https://au.prosple.com{detail_page_url}"
            elif job_id:
                # Fallback to old format (though it may not work)
                job_data['url'] = f"https://au.prosple.com/graduate-opportunities/{job_id}"
            
            # Extract external application URL
            apply_by_url = opp.get('applyByUrl')
            if apply_by_url:
                job_data['external_url'] = apply_by_url
            
            # Extract employer/company
            if 'parentEmployer' in opp:
                employer_ref = opp['parentEmployer']
                
                # Check if employer data is embedded directly in the job
                if isinstance(employer_ref, dict):
                    # Look for company name in different possible fields
                    company_name = None
                    for field in ['title', 'advertiserName', 'name']:
                        if field in employer_ref:
                            company_name = employer_ref[field]
                            break
                    
                    if company_name:
                        job_data['company'] = company_name
                    elif '__ref' in employer_ref:
                        # Try to resolve the reference
                        employer_key = employer_ref['__ref']
                        employer = apollo_state.get(employer_key, {})
                        if employer:
                            for field in ['title', 'advertiserName', 'name']:
                                if field in employer:
                                    job_data['company'] = employer[field]
                                    break
                elif isinstance(employer_ref, str):
                    # Direct employer key reference
                    employer_key = employer_ref if employer_ref.startswith('Employer:') else f"Employer:{employer_ref}"
                    employer = apollo_state.get(employer_key, {})
                    if employer:
                        for field in ['title', 'advertiserName', 'name']:
                            if field in employer:
                                job_data['company'] = employer[field]
                                break
            
            # Extract salary information
            if 'salary' in opp and opp['salary']:
                salary = opp['salary']
                if isinstance(salary, dict):
                    salary_range = salary.get('range', {})
                    if salary_range:
                        job_data['salary_min'] = salary_range.get('minimum')
                        job_data['salary_max'] = salary_range.get('maximum')
                    
                    # Get currency
                    currency = resolve_ref(salary.get('currency', {}), apollo_state)
                    if isinstance(currency, dict):
                        job_data['salary_currency'] = currency.get('label', 'AUD')
                    else:
                        job_data['salary_currency'] = 'AUD'
            
            # Extract locations from physicalLocations
            if 'physicalLocations' in opp:
                locations = []
                for loc_data in opp['physicalLocations']:
                    if isinstance(loc_data, dict) and 'children' in loc_data:
                        # Navigate through nested location structure
                        for state in loc_data.get('children', []):
                            for city in state.get('children', []):
                                locations.append(city.get('label', ''))
                if locations:
                    job_data['location'] = ', '.join(filter(None, locations))
            
            # Extract job type from opportunityTypes
            if 'opportunityTypes' in opp:
                for opp_type in opp['opportunityTypes']:
                    opp_type_resolved = resolve_ref(opp_type, apollo_state)
                    if opp_type_resolved.get('name'):
                        job_data['job_type'] = opp_type_resolved['name']
                        break
            
            # Log what we extracted for debugging
            logger.debug(f"Extracted job data: Title='{job_data['title']}', Company='{job_data['company']}', Location='{job_data['location']}', Salary={job_data['salary_min']}-{job_data['salary_max']} {job_data['salary_currency']}")
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from opportunity: {e}")
            return None
    
    def process_nextjs_jobs(self, nextjs_jobs, page=None):
        """Process and save jobs extracted from Next.js data"""
        try:
            logger.info(f"Processing {len(nextjs_jobs)} complete jobs from Next.js...")
            
            # Limit to max_jobs if specified
            jobs_to_process = nextjs_jobs[:self.max_jobs] if self.max_jobs else nextjs_jobs
            
            for i, job_data in enumerate(jobs_to_process, 1):
                try:
                    logger.info(f"Processing job {i}/{len(jobs_to_process)}: '{job_data['title']}' at {job_data['company']}")
                    logger.info(f"   Location: {job_data['location']}")
                    if job_data['salary_min'] and job_data['salary_max']:
                        logger.info(f"   Salary: {job_data['salary_currency']} {job_data['salary_min']:,} - {job_data['salary_max']:,}")
                    
                    # Update stats
                    if 'processed' not in self.stats:
                        self.stats['processed'] = 0
                    self.stats['processed'] += 1
                    
                    # Save to database with page context for detailed extraction
                    if self.save_job_from_data(job_data, page):
                        if 'saved' not in self.stats:
                            self.stats['saved'] = 0
                        self.stats['saved'] += 1
                        logger.info(f"SUCCESS: Saved complete job: {job_data['title']} at {job_data['company']}")
                    else:
                        if 'duplicates' not in self.stats:
                            self.stats['duplicates'] = 0
                        self.stats['duplicates'] += 1
                        logger.info(f"DUPLICATE: {job_data['title']} at {job_data['company']}")
                        
                    self.human_delay(1, 2)  # Short delay between jobs
                    
                except Exception as e:
                    logger.error(f"Error processing job data: {e}")
                    if 'errors' not in self.stats:
                        self.stats['errors'] = 0
                    self.stats['errors'] += 1
            
            logger.info(f"SUCCESS: Next.js extraction completed - processed {len(jobs_to_process)} jobs with complete data")
            
        except Exception as e:
            logger.error(f"Error processing Next.js jobs: {e}")
            raise

    def find_jobs_recursive(self, data, max_depth=5, current_depth=0):
        """Recursively search for job data in the JSON structure"""
        if current_depth > max_depth:
            return []
        
        if isinstance(data, list):
            # Check if this looks like a job list
            if len(data) > 0 and isinstance(data[0], dict):
                first_item = data[0]
                # Check if it has job-like properties
                job_indicators = ['title', 'company', 'location', 'url', 'id', 'slug']
                if any(key in first_item for key in job_indicators):
                    return data
        
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (list, dict)):
                    result = self.find_jobs_recursive(value, max_depth, current_depth + 1)
                    if result:
                        return result
        
        return []

    def parse_nextjs_jobs(self, jobs_data):
        """Parse job data from Next.js format into standard format"""
        parsed_jobs = []
        
        logger.info(f"Parsing {len(jobs_data)} jobs from Next.js data")
        
        for i, job in enumerate(jobs_data):
            try:
                logger.info(f"Processing job {i+1}: {job}")
                
                if not isinstance(job, dict):
                    logger.warning(f"Job {i+1} is not a dict: {type(job)}")
                    continue
                
                # Extract basic job information with various possible field names
                job_data = {
                    'title': job.get('title', job.get('name', job.get('position', ''))),
                    'company': '',
                    'location': '',
                    'url': job.get('url', job.get('link', job.get('href', ''))),
                    'salary': job.get('salary', job.get('pay', job.get('compensation', ''))),
                    'job_type': job.get('employment_type', job.get('type', job.get('workType', 'full_time'))),
                    'posted_ago': job.get('posted_date', job.get('created_at', job.get('datePosted', '')))
                }
                
                # Handle company - could be string or object
                company = job.get('company', job.get('employer', job.get('organisation', '')))
                if isinstance(company, dict):
                    job_data['company'] = company.get('name', company.get('title', ''))
                else:
                    job_data['company'] = str(company) if company else ''
                
                # Handle location - could be string or object
                location = job.get('location', job.get('city', job.get('state', '')))
                if isinstance(location, dict):
                    city = location.get('city', location.get('name', ''))
                    state = location.get('state', location.get('region', ''))
                    job_data['location'] = f"{city}, {state}".strip(', ') if city or state else 'Australia'
                elif isinstance(location, list) and location:
                    job_data['location'] = ', '.join(str(loc) for loc in location)
                else:
                    job_data['location'] = str(location) if location else 'Australia'
                
                # Handle different URL formats
                if job_data['url'] and not job_data['url'].startswith('http'):
                    if job_data['url'].startswith('/'):
                        job_data['url'] = urljoin(self.base_url, job_data['url'])
                    else:
                        job_data['url'] = f"{self.base_url}/{job_data['url']}"
                
                # Validate essential data
                if job_data['title'] and len(job_data['title']) > 2:
                    parsed_jobs.append(job_data)
                    logger.info(f"Successfully parsed job: {job_data['title']} at {job_data['company']}")
                else:
                    logger.warning(f"Job {i+1} has no valid title: {job_data}")
                
            except Exception as e:
                logger.error(f"Error parsing job {i+1} from JSON: {e}")
                logger.error(f"Job data was: {job}")
                continue
        
        logger.info(f"Successfully parsed {len(parsed_jobs)} jobs from Next.js data")
        return parsed_jobs

    def extract_job_data(self, job_element, page):
        """Extract ONLY title and URL from job listing element"""
        try:
            job_data = {}
            
            # Extract job title and URL using current website structure
            title_element = job_element.query_selector('h2 a, h1 a, h3 a, a[href*="/graduate-opportunities/"], a[href*="/opportunities/"], a[href*="/job"], a[href*="/career"], a[href*="/position"]')
            if not title_element:
                # Additional fallback selectors for current structure
                title_element = job_element.query_selector('a[target="_blank"], section a, div a, a:first-of-type, [role="button"] a')
            
            if title_element:
                job_data['title'] = title_element.inner_text().strip()
                job_data['url'] = title_element.get_attribute('href')
                if job_data['url'] and not job_data['url'].startswith('http'):
                    job_data['url'] = urljoin(self.base_url, job_data['url'])
            else:
                logger.warning("No title element found")
                return None
            
            # Validate essential data
            if not job_data['title'] or len(job_data['title']) < 2:
                logger.warning("Job title too short or empty")
                return None
            
            # Truncate title to avoid database errors
            job_data['title'] = job_data['title'][:200]
            
            logger.info(f"Extracted job title: {job_data['title']}")
            logger.info(f"Job URL: {job_data['url']}")
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data: {e}")
            return None

    def get_job_details_quick(self, job_url, job_id):
        """Get job details quickly using job ID"""
        try:
            logger.info(f"Getting details for job ID: {job_id}")
            
            # Return basic job details based on ID
            job_details = {
                'title': f'Job Opportunity {job_id}',  # Will be updated if we find real title
                'company': 'Unknown Company',
                'location': 'Australia',
                'url': job_url,
                'salary_min': None,
                'salary_max': None,
                'job_type': 'full_time',
                'description': f'Job listing from Prosple Australia - ID: {job_id}',
                'requirements': '',
                'benefits': '',
                'posted_ago': '',
                'application_deadline': ''
            }
            
            # Try to get more details by checking analytics data
            return job_details
            
        except Exception as e:
            logger.error(f"Error getting job details for {job_id}: {e}")
            return None

    def get_job_details(self, job_url, page):
        """Get detailed job information from the job detail page"""
        try:
            page.goto(job_url)
            self.human_delay(2, 4)
            
            # Wait for content to load
            page.wait_for_selector('body, .job-detail, .content', timeout=10000)
            
            job_details = {
                'description': 'Job listing from Prosple Australia.',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'full_time',
                'closing_date': None,
                'industry': '',
                'job_level': 'graduate'
            }
            
            # Extract company name using generic selectors (no hardcoded content)
            company_selectors = [
                'div[class*="masthead"] h2',          # h2 inside masthead-like div
                'div[class*="header"] h2',            # h2 inside header-like div
                'h2:first-of-type',                   # First h2 on page (usually company)
                'header h2',                          # h2 inside header element
                'h1:first-of-type',                   # First h1 (company name)
                '.company-name',                      # Standard class selectors
                '.employer-name', 
                '.job-company',
                '[data-testid*="company"]'            # Data attribute containing "company"
            ]
            
            for selector in company_selectors:
                company_element = page.query_selector(selector)
                if company_element:
                    company_text = company_element.inner_text().strip()
                    if company_text and len(company_text) > 1:
                        job_details['company'] = company_text
                        break
            
            # Extract location using generic selectors (no hardcoded cities)
            location_selectors = [
                'div:has(svg[class*="map"]) p',        # Paragraph near map icon
                'div:has(svg) p:last-child',           # Last paragraph in div with any SVG
                'p:nth-of-type(2)',                    # Second paragraph (often location)
                'p:last-of-type',                      # Last paragraph (might be location)
                '[class*="location"] p',               # Paragraph in location-like class
                '.job-location',                       # Standard class selectors
                '.location',
                '[data-testid="location"]',
                '[data-testid*="location"]'            # Any data attribute with "location"
            ]
            
            for selector in location_selectors:
                location_element = page.query_selector(selector)
                if location_element:
                    location_text = location_element.inner_text().strip()
                    if location_text and len(location_text) > 1:
                        job_details['location'] = location_text
                        break
            
            # Extract salary information using semantic selectors (no static class dependencies)
            salary_selectors = [
                # Primary: salary icon (dollar sign SVG with specific path pattern) -> last span in that li
                'li[datatype="detail"]:has(svg path[d*="M128 24a104 104 0 1 0 104 104A104.11 104.11 0 0 0 128 24m0 192a88 88 0 1 1 88-88a88.1 88.1 0 0 1-88 88m40-68a28 28 0 0 1-28 28h-4v8a8 8 0 0 1-16 0v-8h-16a8 8 0 0 1 0-16h36a12 12 0 0 0 0-24h-24a28 28 0 0 1 0-56h4v-8a8 8 0 0 1 16 0v8h16a8 8 0 0 1 0 16h-36a12 12 0 0 0 0 24h24a28 28 0 0 1 28 28"]) span:last-child',
                # Alternative: li containing hidden "Salary" text -> last span
                'li[datatype="detail"]:has(span[style*="position: absolute"]:contains("Salary")) span:last-child',
                # Backup: Look for specific dollar sign pattern in path
                'li:has(svg path[d*="m40-68a28 28 0 0 1-28 28"]) span:last-child',
                # Generic SVG with currency/dollar patterns -> parent li -> last span
                'li:has(svg[class*="dollar"]) span:last-child',           # SVG with dollar class
                'li:has(svg[class*="currency"]) span:last-child',         # SVG with currency class
                # Generic class-based fallbacks
                '[class*="salary"] span',                                # Span in salary-like class
                '.salary, .job-salary',                                  # Standard class selectors
                '[data-testid="salary"], [data-testid*="salary"]'        # Data attributes with "salary"
            ]
            
            for selector in salary_selectors:
                salary_element = page.query_selector(selector)
                if salary_element:
                    salary_text = salary_element.inner_text().strip()
                    if salary_text:
                        job_details['salary_raw_text'] = salary_text
                        salary_min, salary_max, salary_type, _ = self.extract_salary_info(salary_text)
                        job_details['salary_min'] = salary_min
                        job_details['salary_max'] = salary_max
                        job_details['salary_type'] = salary_type
                        break
            
            # Extract job type using generic selectors (no hardcoded types)
            type_selectors = [
                # Primary: briefcase/work icon with specific path pattern -> last span in that li
                'li[datatype="detail"]:has(svg path[d*="M216 56h-40v-8a24 24 0 0 0-24-24h-48a24 24 0 0 0-24 24v8H40a16 16 0 0 0-16 16v128a16 16 0 0 0 16 16h176a16 16 0 0 0 16-16V72a16 16 0 0 0-16-16"]) span:last-child',
                # Alternative: Look for hidden "Opportunity type" text
                'li[datatype="detail"]:has(span[style*="position: absolute"]:contains("Opportunity type")) span:last-child',
                # Backup: briefcase/work related selectors
                'li:has(svg[class*="briefcase"]) span:last-child', # Span in li with briefcase icon
                'li:has(svg[class*="work"]) span:last-child',       # Span in li with work icon
                # Generic class-based fallbacks
                '[class*="type"] span',                            # Span in type-like class
                '[class*="employment"] span',                      # Span in employment-like class
                '.job-type',                                       # Standard class selectors
                '.employment-type',
                '[data-testid="job-type"]',
                '[data-testid*="type"]'                            # Any data attribute with "type"
            ]
            
            for selector in type_selectors:
                type_element = page.query_selector(selector)
                if type_element:
                    type_text = type_element.inner_text().strip()
                    
                    # Skip if this contains salary information (AUD, $, numbers)
                    if any(indicator in type_text.upper() for indicator in ['AUD', '$', '000', 'YEAR', 'HOUR', 'WEEK', 'MONTH']):
                        continue
                        
                    type_text_lower = type_text.lower()
                    
                    # Dynamic job type mapping based on common keywords (no hardcoded types)
                    job_type_mapping = {
                        'intern': 'internship',
                        'clerkship': 'internship', 
                        'placement': 'internship',
                        'part': 'part_time',
                        'contract': 'contract',
                        'casual': 'casual',
                        'temporary': 'temporary',
                        'temp': 'temporary',
                        'graduate': 'graduate',
                        'grad': 'graduate',
                        'full': 'full_time',
                        'freelance': 'contract',
                        'consultant': 'contract',
                        'permanent': 'full_time'
                    }
                    
                    # Check for job type keywords in extracted text
                    job_details['job_type'] = 'full_time'  # Default
                    for keyword, job_type in job_type_mapping.items():
                        if keyword in type_text_lower:
                            job_details['job_type'] = job_type
                            break
                    
                    # If we found actual job type text, use it and break
                    if type_text and len(type_text) > 2 and job_details['job_type'] != 'full_time':
                        break
                    
                    # If no mapping found but text looks like job type, store original text (truncated)
                    if (job_details['job_type'] == 'full_time' and type_text and len(type_text) > 2 and 
                        not any(k in type_text_lower for k in job_type_mapping.keys())):
                        job_details['job_type'] = type_text[:50]
                        break
            
            # Extract closing date
            date_selectors = [
                '.closing-date',
                '.application-deadline',
                '[data-testid="closing-date"]',
                '.deadline'
            ]
            
            for selector in date_selectors:
                date_element = page.query_selector(selector)
                if date_element:
                    date_text = date_element.inner_text().strip()
                    job_details['closing_date'] = self.parse_closing_date(date_text)
                    if job_details['closing_date']:
                        break
            
            # Extract industry
            industry_selectors = [
                '.industry',
                '.job-industry',
                '[data-testid="industry"]',
                '.sector'
            ]
            
            for selector in industry_selectors:
                industry_element = page.query_selector(selector)
                if industry_element:
                    job_details['industry'] = industry_element.inner_text().strip()
                    break
            
            # Extract job description - based on provided HTML structure
            description = ''
            description_selectors = [
                '[data-testid="raw-html"]',  # Main selector based on provided HTML
                '.sc-c682c328-0',  # Alternative class-based selector
                '.job-description',  # Fallback selectors
                '.job-detail-description',
                '.description',
                '.job-content',
                '.job-summary',
                '.role-description',
                '[data-testid="job-description"]',
                'main .content',
                '.job-detail .content'
            ]
            
            for selector in description_selectors:
                try:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        desc_text = desc_element.inner_text().strip()
                        if len(desc_text) > 100:
                            description = desc_text
                            logger.info(f"Found description using selector: {selector}")
                            break
                except Exception as e:
                    continue
            
            # If no description found, try to get main content
            if not description:
                try:
                    main_content = page.query_selector('main, .main, #main, .container, .content-area')
                    if main_content:
                        full_text = main_content.inner_text().strip()
                        # Filter out navigation and header content
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                        content_lines = []
                        
                        for line in lines:
                            if len(line) > 20 and not any(keyword in line.lower() for keyword in 
                                ['navigation', 'menu', 'header', 'footer', 'search', 'filter']):
                                content_lines.append(line)
                        
                        if content_lines:
                            description = '\n'.join(content_lines[:50])  # Limit to first 50 meaningful lines
                            logger.info("Extracted description from main content area")
                except Exception as e:
                    logger.warning(f"Error extracting from main content: {e}")
            
            if description and len(description) > 50:
                job_details['description'] = description
            else:
                # Enhanced fallback description
                fallback_parts = [f"Position: {job_details.get('title', 'Graduate Position')}"]
                fallback_parts.append(f"Company: {job_details['company']}")
                fallback_parts.append(f"Location: {job_details['location']}")
                
                if job_details['industry']:
                    fallback_parts.append(f"Industry: {job_details['industry']}")
                if job_details['salary_raw_text']:
                    fallback_parts.append(f"Salary: {job_details['salary_raw_text']}")
                
                fallback_parts.append("This is a graduate and professional opportunity posted on Prosple Australia.")
                fallback_parts.append(f"For full job details, visit: {job_url}")
                
                job_details['description'] = '\n'.join(fallback_parts)
                logger.warning(f"Using enhanced fallback description for {job_url}")
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Could not get job details for {job_url}: {e}")
            return {
                'description': 'Graduate opportunity from Prosple Australia',
                'company': 'Unknown Company',
                'location': 'Australia',
                'salary_min': None,
                'salary_max': None,
                'salary_type': 'yearly',
                'salary_raw_text': '',
                'job_type': 'graduate',
                'closing_date': None,
                'industry': '',
                'job_level': 'graduate'
            }

    def categorize_job(self, title, description, company_name):
        """Categorize job using the categorization service"""
        try:
            category = self.categorization_service.categorize_job(title, description)
            
            # Map to specific graduate/entry-level categories if applicable
            title_lower = title.lower()
            desc_lower = description.lower()
            
            # Graduate-specific categorizations
            if any(term in title_lower for term in ['graduate', 'entry level', 'junior', 'trainee']):
                if any(term in title_lower for term in ['analyst', 'data', 'research']):
                    return 'analyst'
                elif any(term in title_lower for term in ['engineer', 'software', 'developer']):
                    return 'engineering'
                elif any(term in title_lower for term in ['consultant', 'advisory']):
                    return 'consulting'
                elif any(term in title_lower for term in ['marketing', 'communications']):
                    return 'marketing'
                elif any(term in title_lower for term in ['finance', 'accounting']):
                    return 'finance'
                elif any(term in title_lower for term in ['hr', 'human resources']):
                    return 'human_resources'
            
            return category
            
        except Exception as e:
            logger.warning(f"Error categorizing job: {e}")
            return 'other'

    def save_job_from_data(self, job_data, page):
        """Save job to database from JSON data with proper error handling"""
        try:
            with transaction.atomic():
                # Enhanced duplicate detection using multiple fields
                if not job_data.get('url'):
                    logger.warning(f"No URL for job: {job_data.get('title', 'Unknown')}")
                    return False
                
                # Check for duplicates by URL first
                logger.info(f"CHECKING FOR DUPLICATE with URL: {job_data['url']}")
                existing_job = JobPosting.objects.filter(external_url=job_data['url']).first()
                if existing_job:
                    logger.info(f"DUPLICATE FOUND - Job exists in database: {existing_job.title} (ID: {existing_job.id})")
                    logger.info(f"   Existing URL: {existing_job.external_url}")
                    logger.info(f"   Scraped at: {existing_job.scraped_at}")
                    self.stats['duplicate_jobs'] += 1
                    return False
                else:
                    logger.info(f"NO DUPLICATE FOUND - Job is new!")
                
                # Additional duplicate check by title + company
                if job_data.get('title') and job_data.get('company'):
                    logger.info(f"SECONDARY CHECK - Title+Company: '{job_data['title']}' at '{job_data['company']}'")
                    existing_job_by_title = JobPosting.objects.filter(
                        title__iexact=job_data['title'],
                        company__name__iexact=job_data['company'],
                        external_source='prosple.com.au'
                    ).first()
                    if existing_job_by_title:
                        logger.info(f"DUPLICATE FOUND by Title+Company: {existing_job_by_title.title} (ID: {existing_job_by_title.id})")
                        logger.info(f"   Company: {existing_job_by_title.company.name}")
                        logger.info(f"   External Source: {existing_job_by_title.external_source}")
                        self.stats['duplicate_jobs'] += 1
                        return False
                    else:
                        logger.info(f"NO TITLE+COMPANY DUPLICATE - Job is new!")
                
                # Extract detailed job information from the individual job page
                job_details = None
                if job_data.get('url'):
                    try:
                        # Always use the existing page to avoid async/sync conflicts
                        if page is not None:
                            job_details = self.get_job_details(job_data['url'], page)
                        if job_details:
                            logger.info(f"✅ EXTRACTED FULL DETAILS from detail page for: {job_data['title']}")
                        else:
                            logger.warning("No page context available for detailed extraction")
                            job_details = None
                        
                    except Exception as e:
                        logger.warning(f"Error getting job details from page: {e}")
                        job_details = None
                
                # If job_details extraction failed, use JSON data as fallback
                if not job_details:
                    logger.info(f"Using Next.js JSON data as fallback for: {job_data['title']}")
                    job_details = {
                        'description': job_data.get('description', ''),
                        'company': job_data.get('company', 'Unknown Company'),
                        'location': job_data.get('location', 'Australia'),
                        'salary_min': job_data.get('salary_min'),
                        'salary_max': job_data.get('salary_max'),
                        'salary_type': 'yearly',
                        'salary_raw_text': '',
                        'job_type': job_data.get('job_type', 'graduate'),
                        'closing_date': None,
                        'industry': '',
                        'job_level': 'graduate'
                    }
                else:
                    # Use data from detail page but supplement with Next.js data where needed
                    if not job_details.get('company') or job_details['company'] == 'Unknown Company':
                        job_details['company'] = job_data.get('company', 'Unknown Company')
                    if not job_details.get('location') or job_details['location'] == 'Australia':
                        job_details['location'] = job_data.get('location', 'Australia')
                    if not job_details.get('salary_min') and job_data.get('salary_min'):
                        job_details['salary_min'] = job_data.get('salary_min')
                        job_details['salary_max'] = job_data.get('salary_max')
                    if not job_details.get('job_type') or job_details['job_type'] == 'full_time':
                        # Try to get job type from Next.js data if detail page didn't find it
                        if job_data.get('job_type'):
                            job_details['job_type'] = job_data.get('job_type')
                        else:
                            job_details['job_type'] = 'graduate'  # Default for prosple
                
                # Build salary raw text from min/max if available
                if job_data.get('salary_min') and job_data.get('salary_max'):
                    currency = job_data.get('salary_currency', 'AUD')
                    job_details['salary_raw_text'] = f"{currency} {job_data['salary_min']:,} - {job_data['salary_max']:,}"
                
                # Enhanced description handling - prefer detail page description over JSON data
                if job_details.get('description') and len(job_details['description']) > 200:
                    # We got a good description from the detail page, keep it
                    logger.info(f"Using full description from detail page ({len(job_details['description'])} chars)")
                else:
                    # Build enhanced description from available data
                    description_parts = []
                    if job_data.get('description') and len(job_data.get('description', '')) > 50:
                        description_parts.append(job_data['description'])
                    else:
                        # Build description from available data
                        description_parts.append(f"Position: {job_data.get('title', 'Graduate Position')}")
                        description_parts.append(f"Company: {job_data.get('company', 'Unknown Company')}")
                        description_parts.append(f"Location: {job_data.get('location', 'Australia')}")
                        
                        if job_details.get('salary_raw_text'):
                            description_parts.append(f"Salary: {job_details['salary_raw_text']}")
                        
                        if job_data.get('application_deadline'):
                            description_parts.append(f"Application Deadline: {job_data['application_deadline']}")
                        
                        if job_data.get('remote_available'):
                            description_parts.append("Remote work available")
                        
                        description_parts.append("This is a graduate and professional opportunity posted on Prosple Australia.")
                        description_parts.append(f"For full job details, visit: {job_data.get('url', 'https://au.prosple.com')}")
                    
                    job_details['description'] = '\n'.join(description_parts)
                    logger.info(f"Built fallback description ({len(job_details['description'])} chars)")
                
                # Parse closing date if available
                if job_data.get('application_deadline'):
                    job_details['closing_date'] = self.parse_closing_date(job_data['application_deadline'])
                
                # Get or create company
                company = self.get_or_create_company(job_details['company'])
                
                # Get or create location
                location = self.get_or_create_location(job_details['location'])
                
                # Categorize job
                category = self.categorize_job(job_data['title'], job_details['description'], job_details['company'])
                
                # Create job posting
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],
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
                    external_source='prosple.com.au',
                    external_url=job_data['url'][:500] if job_data['url'] else '',
                    posted_ago=job_data.get('posted_ago', '')[:50],
                    status='active',
                    additional_info={
                        'closing_date': job_details['closing_date'].isoformat() if job_details['closing_date'] else None,
                        'industry': job_details['industry'],
                        'job_level': job_details['job_level'],
                        'scrape_timestamp': datetime.now().isoformat(),
                        'source_type': 'nextjs_json'
                    }
                )
                
                logger.info(f"Saved job: {job_data['title']} at {company.name}")
                self.stats['new_jobs'] += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job {job_data.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

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
                    title=job_data['title'][:200],
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
                    external_source='prosple.com.au',
                    external_url=job_data['url'][:500],
                    posted_ago=job_data.get('posted_ago', '')[:50],
                    status='active',
                    additional_info={
                        'closing_date': job_details['closing_date'].isoformat() if job_details['closing_date'] else None,
                        'industry': job_details['industry'],
                        'job_level': job_details['job_level'],
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

    def scrape_jobs(self):
        """Main scraping method"""
        logger.info("Starting Prosple Australia job scraping...")
        
        with sync_playwright() as playwright:
            # Launch browser with additional stealth options
            browser = playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
            
            # Create context with realistic browser settings
            context = browser.new_context(
                user_agent=self.user_agent,
                viewport={'width': 1366, 'height': 768},
                screen={'width': 1366, 'height': 768},
                locale='en-AU',
                timezone_id='Australia/Sydney',
                geolocation={'longitude': 151.2093, 'latitude': -33.8688},  # Sydney coordinates
                permissions=['geolocation']
            )
            
            page = context.new_page()
            
            # Add stealth JavaScript to avoid detection
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-AU', 'en'],
                });
                
                window.chrome = {
                    runtime: {},
                };
            """)
            
            # Track network requests to find API calls
            api_requests = []
            graphql_requests = []
            pagination_responses = []  # Store GraphQL responses containing job data
            
            def handle_request(request):
                url = request.url.lower()
                if any(keyword in url for keyword in ['job', 'search', 'api', 'opportunity']):
                    api_requests.append(request.url)
                    logger.info(f"Captured relevant request: {request.url}")
                
                # Specifically track GraphQL requests
                if 'graphql' in url or 'internal' in url or request.method == 'POST':
                    graphql_requests.append({
                        'url': request.url,
                        'method': request.method,
                        'headers': dict(request.headers),
                        'post_data': request.post_data
                    })
                    logger.info(f"Captured GraphQL/API request: {request.url}")
            
            def handle_response(response):
                url = response.url.lower()
                if ('job' in url or 'search' in url or 'api' in url or 
                    'graphql' in url or 'internal' in url):
                    try:
                        # Try to capture response data for analysis
                        if response.status == 200:
                            logger.info(f"Successful response from: {response.url}")
                            
                            # For GraphQL responses with job data, capture the response
                            if ('internal' in url and response.request.method == 'POST'):
                                try:
                                    response_data = response.json()
                                    pagination_responses.append({
                                        'url': response.url,
                                        'data': response_data,
                                        'timestamp': time.time()
                                    })
                                    logger.info("Captured pagination API response with job data")
                                except Exception as json_error:
                                    logger.debug(f"Could not parse GraphQL response as JSON: {json_error}")
                                    logger.info("Found potential job data API response")
                    except Exception as e:
                        logger.debug(f"Error handling response: {e}")
            
            page.on("request", handle_request)
            page.on("response", handle_response)
            
            try:
                # Navigate to the jobs page with shorter timeout to get analytics data
                logger.info(f"Navigating to: {self.search_url}")
                try:
                    page.goto(self.search_url, timeout=30000, wait_until='domcontentloaded')
                    self.human_delay(5, 8)
                    
                    # Wait for basic page structure
                    page.wait_for_selector('body', timeout=10000)
                    logger.info("Page body loaded, extracting Next.js data immediately...")
                    
                    # Extract jobs from multiple pages using pagination
                    logger.info("Starting pagination-aware job extraction...")
                    all_jobs = []
                    current_page = 1
                    max_pages = 10  # Allow more pages to get sufficient jobs for larger requests
                    
                    while len(all_jobs) < (self.max_jobs or 1000) and current_page <= max_pages:
                        logger.info(f"Extracting jobs from page {current_page}...")
                        
                        # Extract Next.js data from current page (every page has same UI structure)
                        nextjs_jobs = self.extract_jobs_from_nextjs_data(page)
                        if nextjs_jobs:
                            logger.info(f"Found {len(nextjs_jobs)} jobs on page {current_page} (from Next.js data)")
                            all_jobs.extend(nextjs_jobs)
                        else:
                            logger.warning(f"No jobs found in Next.js data for page {current_page}")
                            # If no Next.js data found, this page might be empty or we reached the end
                            break
                                    
                        # Check if we extracted any jobs at all
                        if current_page == 1 and len(all_jobs) == 0:
                            logger.error("No jobs found on first page - stopping")
                            break
                        
                        # Check if we have enough jobs
                        if self.max_jobs and len(all_jobs) >= self.max_jobs:
                            logger.info(f"Reached job limit of {self.max_jobs}, stopping pagination")
                            break
                            
                        # Only proceed with pagination if we have jobs
                        if len(all_jobs) > 0:
                                
                            # Try to navigate to next page
                            try:
                                # Multiple strategies to find pagination elements
                                next_button = None
                                next_page_num = current_page + 1
                                
                                # Strategy 1: Use exact selectors from the actual Prosple HTML structure
                                pagination_selectors = [
                                    # Exact selectors from the provided HTML
                                    'button[aria-label="Goto next page"]',
                                    f'button[aria-label="Goto Page {next_page_num}"]',
                                    'nav[aria-label="Pagination Navigation"] button[aria-label*="next" i]',
                                    'nav[aria-label="Pagination Navigation"] button:has(svg)',
                                    '.sc-dff9ec26-2.eYRdpA',  # Next button class
                                    f'.sc-dff9ec26-2.jSaIns:has-text("{next_page_num}")',  # Page number button class
                                    
                                    # Fallback selectors
                                    f'button:has-text("{next_page_num}")',
                                    f'a:has-text("{next_page_num}")',
                                    'button[aria-label*="Goto next" i]',
                                    'button[aria-label*="Go to next" i]',
                                    'button[aria-label*="next page" i]',
                                    f'button[aria-label*="Goto Page {next_page_num}" i]',
                                    f'button[aria-label*="Go to Page {next_page_num}" i]',
                                    
                                    # Generic navigation selectors
                                    'nav[role="navigation"] button[aria-label*="next" i]',
                                    '[class*="pagination"] button[aria-label*="next" i]',
                                    'button:has(svg) >> css=nav[aria-label*="Pagination"]',
                                ]
                                
                                for selector in pagination_selectors:
                                    try:
                                        # Try to find a single button first
                                        single_button = page.query_selector(selector)
                                        if single_button and single_button.is_visible() and single_button.is_enabled():
                                            # For aria-label based selectors, we can trust them directly
                                            if 'aria-label' in selector:
                                                next_button = single_button
                                                logger.info(f"Found pagination button using aria-label selector: {selector}")
                                                break
                                            
                                            # For other selectors, check content
                                            text_content = single_button.text_content() or ""
                                            aria_label = single_button.get_attribute('aria-label') or ""
                                            
                                            if (str(next_page_num) in text_content or 
                                                "next" in text_content.lower() or
                                                "next" in aria_label.lower() or
                                                f"Page {next_page_num}" in aria_label):
                                                next_button = single_button
                                                logger.info(f"Found pagination button using selector: {selector}")
                                                break
                                        
                                        # Fallback to multiple button search
                                        potential_buttons = page.query_selector_all(selector)
                                        for btn in potential_buttons:
                                            if btn.is_visible() and btn.is_enabled():
                                                text_content = btn.text_content() or ""
                                                aria_label = btn.get_attribute('aria-label') or ""
                                                
                                                if (str(next_page_num) in text_content or 
                                                    "next" in text_content.lower() or
                                                    "next" in aria_label.lower() or
                                                    f"Page {next_page_num}" in aria_label):
                                                    next_button = btn
                                                    logger.info(f"Found pagination button from list using selector: {selector}")
                                                    break
                                        
                                        if next_button:
                                            break
                                    except Exception as e:
                                        logger.debug(f"Selector {selector} failed: {e}")
                                        continue
                                
                                if next_button:
                                    logger.info(f"Found pagination button for page {next_page_num}")
                                    
                                    # Scroll to button to ensure it's visible
                                    next_button.scroll_into_view_if_needed()
                                    time.sleep(1)
                                    
                                    # Try different click methods
                                    try:
                                        # First try: force click to bypass intercepting elements
                                        next_button.click(force=True)
                                    except:
                                        try:
                                            # Second try: JavaScript click
                                            page.evaluate('element => element.click()', next_button)
                                        except:
                                            # Third try: dispatch click event
                                            next_button.dispatch_event('click')
                                    
                                    logger.info(f"Successfully clicked to navigate to page {next_page_num}")
                                    
                                    # Wait for page to load and verify navigation
                                    try:
                                        # Wait for network to settle
                                        page.wait_for_load_state('networkidle', timeout=20000)
                                        time.sleep(2)
                                        
                                        # Verify we're on the new page by checking the current page indicator
                                        try:
                                            current_page_indicator = page.query_selector(f'button[aria-label="Current Page, Page {next_page_num}"]')
                                            if current_page_indicator:
                                                logger.info(f"Successfully navigated to page {next_page_num}")
                                                current_page = next_page_num
                                            else:
                                                # Alternative verification - check if page 1 is no longer current
                                                page_1_current = page.query_selector('button[aria-label="Current Page, Page 1"]')
                                                if not page_1_current:
                                                    logger.info(f"Successfully navigated away from page 1")
                                                    current_page = next_page_num
                                                else:
                                                    logger.warning("Page navigation may have failed - still on page 1")
                                                    break
                                        except Exception as e:
                                            logger.warning(f"Could not verify page navigation: {e}")
                                            # Assume success and continue
                                            current_page = next_page_num
                                            
                                        time.sleep(5)  # Additional wait for dynamic content to load
                                        
                                        # CRITICAL: Wait for Next.js data to refresh after pagination
                                        logger.info(f"Waiting for Next.js data to refresh on page {next_page_num}...")
                                        
                                        # Wait for page content to fully refresh - this is crucial
                                        try:
                                            # Wait for the page content to change by checking URL parameters
                                            page.wait_for_function(f"""
                                                () => {{
                                                    const urlParams = new URLSearchParams(window.location.search);
                                                    const start = urlParams.get('start');
                                                    return start === '{(next_page_num - 1) * 20}' || window.location.search.includes('start={(next_page_num - 1) * 20}');
                                                }}
                                            """, timeout=15000)
                                            logger.info(f"URL parameters updated for page {next_page_num}")
                                        except:
                                            logger.warning("URL parameter check timeout, but continuing...")
                                        
                                        # Additional wait for Next.js data to refresh
                                        time.sleep(3)
                                        
                                        # Now extract fresh Next.js data from the new page
                                        current_page = next_page_num
                                        continue  # Continue to next iteration to extract fresh data
                                            
                                    except Exception as e:
                                        logger.warning(f"Page load timeout: {e}")
                                        time.sleep(5)  # Fallback wait
                                        current_page = next_page_num  # Assume success
                                else:
                                    logger.info(f"No pagination button found for page {next_page_num} - reached end of pages")
                                    break
                                    
                            except Exception as e:
                                logger.warning(f"Error navigating to next page: {e}")
                                break
                        else:
                            logger.warning(f"No jobs found on page {current_page}, stopping pagination")
                            break
                    
                    if all_jobs:
                        # Limit to max_jobs if specified
                        if self.max_jobs:
                            all_jobs = all_jobs[:self.max_jobs]
                        
                        logger.info(f"SUCCESS! Extracted {len(all_jobs)} total jobs from {current_page} pages")
                        self.process_nextjs_jobs(all_jobs, page)
                        return  # Exit early with complete data
                    else:
                        logger.warning("Next.js extraction failed, continuing with analytics fallback")
                    
                    # Wait for analytics to load (shorter time)
                    time.sleep(10)  # Reduced wait time to get analytics faster
                    
                except Exception as e:
                    logger.warning(f"Page navigation issue, but continuing: {e}")
                    # Continue anyway - we might still have captured analytics data
                
                # Check if we got redirected or blocked
                current_url = page.url
                logger.info(f"Current URL after navigation: {current_url}")
                
                # Check page title to ensure we're on the right page
                title = page.title()
                logger.info(f"Page title: '{title}'")
                
                if not title or 'blocked' in title.lower() or 'captcha' in title.lower():
                    logger.warning("Possible blocking or CAPTCHA detected")
                    # Try to take a screenshot for debugging
                    try:
                        page.screenshot(path='prosple_page_screenshot.png')
                        logger.info("Screenshot saved as prosple_page_screenshot.png")
                    except Exception as e:
                        logger.warning(f"Could not take screenshot: {e}")
                
                # Save page content for debugging with better error handling
                try:
                    content = page.content()
                    logger.info(f"Page content length: {len(content)} characters")
                    with open('prosple_debug.html', 'w', encoding='utf-8') as f:
                        f.write(content)
                    logger.info("Saved page content to prosple_debug.html for analysis")
                    
                    # Log first 500 characters for immediate debugging
                    if content:
                        logger.info(f"Page content preview: {content[:500]}")
                    else:
                        logger.warning("Page content is empty!")
                        
                except Exception as e:
                    logger.error(f"Error saving page content: {e}")
                
                # Try to trigger search/filtering to load job data
                try:
                    # Look for search button or submit button
                    search_button = page.query_selector('button[type="submit"], .search-button, [class*="search"], button:has-text("Search")')
                    if search_button:
                        logger.info("Found search button, clicking to trigger job loading...")
                        search_button.click()
                        time.sleep(5)  # Wait for results to load
                        
                    # Also try pressing Enter in any search field
                    search_input = page.query_selector('input[type="search"], input[placeholder*="search"], input[name*="search"]')
                    if search_input:
                        logger.info("Found search input, pressing Enter to trigger search...")
                        search_input.press('Enter')
                        time.sleep(5)
                        
                except Exception as e:
                    logger.info(f"Could not interact with search elements: {e}")
                
                # Extract job data from Next.js JSON immediately  
                logger.info("Extracting job data from Next.js JSON...")
                # Fallback to analytics if Next.js fails  
                logger.warning("Next.js extraction failed, falling back to analytics...")
                analytics_job_ids = self.extract_job_ids_from_analytics(api_requests)
                if analytics_job_ids:
                    logger.info(f"Successfully extracted {len(analytics_job_ids)} job IDs from analytics")
                    jobs_to_process = analytics_job_ids[:self.max_jobs] if self.max_jobs else analytics_job_ids
                    
                    for i, job_id in enumerate(jobs_to_process, 1):
                        try:
                            logger.info(f"Processing job {i}/{len(jobs_to_process)}: ID {job_id}")
                            job_url = f"https://au.prosple.com/graduate-opportunities/{job_id}"
                            job_details = self.get_job_details_quick(job_url, job_id)
                            
                            if job_details:
                                if 'processed' not in self.stats:
                                    self.stats['processed'] = 0
                                self.stats['processed'] += 1
                                
                                if self.save_job_from_data(job_details, None):
                                    if 'saved' not in self.stats:
                                        self.stats['saved'] = 0
                                    self.stats['saved'] += 1
                                else:
                                    if 'duplicates' not in self.stats:
                                        self.stats['duplicates'] = 0
                                    self.stats['duplicates'] += 1
                                    
                            self.human_delay(1, 2)  # Short delay between jobs
                        except Exception as e:
                            logger.error(f"Error processing job ID {job_id}: {e}")
                            if 'errors' not in self.stats:
                                self.stats['errors'] = 0
                            self.stats['errors'] += 1
                    
                    logger.info(f"Analytics extraction completed - processed {len(jobs_to_process)} jobs")
                    return
                
                logger.info("Proceeding to traditional scraping as fallback...")
                
                # Log any API requests that were captured
                if api_requests:
                    logger.info(f"Captured {len(api_requests)} relevant API requests:")
                    for url in api_requests:
                        logger.info(f"  - {url}")
                else:
                    logger.info("No job-related API requests detected")
                
                # Log GraphQL requests separately
                if graphql_requests:
                    logger.info(f"Captured {len(graphql_requests)} GraphQL/API requests:")
                    for req in graphql_requests:
                        logger.info(f"  - {req['method']} {req['url']}")
                        if req['post_data']:
                            logger.info(f"    POST data: {req['post_data'][:200]}...")
                else:
                    logger.info("No GraphQL/API requests detected")
                
                # Don't wait for additional page loads - we already have the analytics data
                logger.info("Skipping additional page loads - analytics data should be available")
                
                # Debug: Log page content to understand structure
                logger.info(f"Page URL: {page.url}")
                logger.info(f"Page title: {page.title()}")
                
                # Save page content for debugging
                with open('prosple_debug.html', 'w', encoding='utf-8') as f:
                    f.write(page.content())
                logger.info("Saved page content to prosple_debug.html for analysis")
                
                # Find job listings using generic selectors (no site-specific URLs)
                # Updated selectors based on current website structure (December 2024)
                job_selectors = [
                    'article',                                # Primary: Article elements containing job cards
                    'div[class*="card"]',                     # Card divs containing job info
                    'a[href*="/graduate-opportunities/"]',   # Direct links to graduate opportunities
                    'a[href*="/opportunities/"]',            # Direct links to job opportunities 
                    'section[class*="job"]',                 # Section elements with job classes
                    'div[class*="opportunity"]',             # Div elements with opportunity classes
                    '[data-testid*="job"]',                  # Data attributes with job
                    '[data-testid*="opportunity"]',          # Data attributes with opportunity
                    'li:has(h2)',                            # Li containing h2 job titles
                    'li:has(h3)',                            # Li containing h3 job titles
                    '.job-listing',                          # Traditional job listing classes
                    '.job-card',
                    '.job-item',
                    '.opportunity-card',
                    '[role="listitem"]',                     # List items that might contain jobs
                    'div[class*="search-result"]'            # Search result containers
                ]
                
                job_elements = []
                for selector in job_selectors:
                    try:
                        elements = page.query_selector_all(selector)
                        if elements:
                            job_elements = elements
                            logger.info(f"Found {len(elements)} jobs using selector: {selector}")
                            break
                        else:
                            logger.debug(f"No elements found with selector: {selector}")
                    except Exception as e:
                        logger.debug(f"Error with selector {selector}: {e}")
                        continue
                
                # Try analytics-based approach first - this is most reliable
                logger.info("Trying analytics-based job extraction...")
                job_ids = self.extract_job_ids_from_analytics(api_requests)
                
                if job_ids:
                    logger.info(f"Found {len(job_ids)} job IDs from analytics, fetching job data...")
                    analytics_jobs = self.fetch_job_data_from_api(job_ids, page)
                    
                    if analytics_jobs:
                        logger.info(f"Successfully extracted {len(analytics_jobs)} jobs from analytics API")
                        # Process jobs from analytics data
                        jobs_to_process = analytics_jobs[:self.max_jobs] if self.max_jobs else analytics_jobs
                        
                        for i, job_data in enumerate(jobs_to_process, 1):
                            try:
                                logger.info(f"Processing job {i}/{len(jobs_to_process)}: {job_data['title']}")
                                self.stats['total_processed'] += 1
                                success = self.save_job_from_data(job_data, page)
                                
                                if success:
                                    logger.info(f"Successfully saved: {job_data['title']}")
                                
                                # Add delay between jobs
                                self.human_delay(1, 2)
                                
                            except Exception as e:
                                logger.error(f"Error processing job {i}: {e}")
                                self.stats['errors'] += 1
                                continue
                        
                        return  # Exit here since we processed jobs from analytics
                
                # Fallback 1: If no jobs found in analytics and no job elements, try Next.js JSON data
                if not job_elements:
                    logger.warning("No job elements found with standard selectors, trying Next.js data extraction...")
                    nextjs_jobs = self.extract_jobs_from_nextjs_data(page)
                    
                    if nextjs_jobs:
                        logger.info(f"Successfully extracted {len(nextjs_jobs)} jobs from Next.js data")
                        # Process jobs from JSON data
                        jobs_to_process = nextjs_jobs[:self.max_jobs] if self.max_jobs else nextjs_jobs
                        
                        for i, job_data in enumerate(jobs_to_process, 1):
                            try:
                                logger.info(f"Processing job {i}/{len(jobs_to_process)}: {job_data['title']}")
                                self.stats['total_processed'] += 1
                                success = self.save_job_from_data(job_data, page)
                                
                                if success:
                                    logger.info(f"Successfully saved: {job_data['title']}")
                                
                                # Add delay between jobs
                                self.human_delay(1, 2)
                                
                            except Exception as e:
                                logger.error(f"Error processing job {i}: {e}")
                                self.stats['errors'] += 1
                                continue
                        
                        return  # Exit here since we processed jobs from JSON
                    
                    # Last resort: try to find any links that might be job listings
                    logger.warning("No Next.js data found, trying link analysis...")
                    all_links = page.query_selector_all('a[href]')
                    job_links = []
                    for link in all_links:
                        href = link.get_attribute('href')
                        if href and any(keyword in href.lower() for keyword in ['/job', '/position', '/opportunity', '/career']):
                            job_links.append(link)
                    
                    if job_links:
                        job_elements = job_links
                        logger.info(f"Found {len(job_links)} potential job links through link analysis")
                    else:
                        logger.error("No job elements found on the page")
                        logger.info("Content preview:")
                        logger.info(page.content()[:2000])
                        return
                
                # First, collect all job URLs to avoid DOM context issues
                logger.info("Step 1: Collecting all job URLs from the listing page...")
                job_urls = []
                jobs_to_process = job_elements[:self.max_jobs] if self.max_jobs else job_elements
                
                for i, job_element in enumerate(jobs_to_process, 1):
                    try:
                        logger.info(f"Extracting URL for job {i}/{len(jobs_to_process)}")
                        job_data = self.extract_job_data(job_element, page)
                        
                        if job_data and job_data.get('url'):
                            job_urls.append({
                                'title': job_data['title'], 
                                'url': job_data['url']
                            })
                            logger.info(f"Collected URL for: {job_data['title']}")
                        else:
                            logger.warning(f"Could not extract URL for job {i}")
                            
                    except Exception as e:
                        logger.error(f"Error extracting URL for job {i}: {e}")
                        continue
                
                logger.info(f"Step 2: Successfully collected {len(job_urls)} job URLs")
                
                # Now process each job URL individually to avoid DOM context issues
                successfully_extracted = 0
                for i, job_info in enumerate(job_urls, 1):
                    try:
                        logger.info(f"Processing job {i}/{len(job_urls)}: {job_info['title']}")
                        
                        # Create job_data structure for saving
                        job_data = {
                            'title': job_info['title'],
                            'url': job_info['url']
                        }
                        
                        self.stats['total_processed'] += 1
                        success = self.save_job(job_data, page)
                        
                        if success:
                            logger.info(f"Successfully saved: {job_data['title']}")
                            successfully_extracted += 1
                        
                        # Add delay between jobs
                        self.human_delay(1, 2)
                            
                    except Exception as e:
                        logger.error(f"Error processing job {i}: {e}")
                        self.stats['errors'] += 1
                        continue
                
                # If we found job elements but couldn't extract data from any of them,
                # try the Next.js JSON extraction as a fallback
                if job_elements and successfully_extracted == 0:
                    logger.warning("Found job elements but couldn't extract data from any. Trying Next.js data extraction as fallback...")
                    nextjs_jobs = self.extract_jobs_from_nextjs_data(page)
                    
                    if nextjs_jobs:
                        logger.info(f"Successfully extracted {len(nextjs_jobs)} jobs from Next.js data")
                        # Process jobs from JSON data
                        jobs_to_process = nextjs_jobs[:self.max_jobs] if self.max_jobs else nextjs_jobs
                        
                        for i, job_data in enumerate(jobs_to_process, 1):
                            try:
                                logger.info(f"Processing job {i}/{len(jobs_to_process)}: {job_data['title']}")
                                self.stats['total_processed'] += 1
                                success = self.save_job_from_data(job_data, page)
                                
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
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
            logger.info(f"Job limit set to: {max_jobs}")
        except ValueError:
            logger.error("Invalid job limit. Please provide a number.")
            sys.exit(1)
    
    # Create and run scraper (headless=False to see the browser)
    scraper = ProspleAustraliaScraper(max_jobs=max_jobs, headless=False)
    scraper.scrape_jobs()


if __name__ == "__main__":
    main()
