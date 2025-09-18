#!/usr/bin/env python
"""
The Creative Store Australia Job Scraper using Playwright

This script scrapes job listings from thecreativestore.com.au/jobs/ using a robust approach
that handles job details and comprehensive data extraction.

Features:
- Professional database structure integration
- Human-like behavior to avoid detection
- Advanced salary and location extraction
- Robust error handling and logging
- Integration with existing Django models

Usage:
    python thecreativestore_australia_scraper.py [max_jobs]

Example:
    python thecreativestore_australia_scraper.py 50
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
import requests
from bs4 import BeautifulSoup

# Set up Django environment BEFORE any Django imports
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
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

User = get_user_model()

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('thecreativestore_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TheCreativeStoreScraper:
    """Professional scraper for The Creative Store job listings."""
    
    def __init__(self, max_jobs=None, headless=False):
        """Initialize the scraper with configuration."""
        self.base_url = "https://thecreativestore.com.au"
        self.jobs_url = "https://thecreativestore.com.au/jobs/"
        self.max_jobs = max_jobs
        self.headless = headless
        self.scraped_count = 0
        self.skipped_count = 0
        self.error_count = 0
        self.start_time = datetime.now()
        
        # Initialize database user
        self.user, created = User.objects.get_or_create(
            username='scraper_user',
            defaults={'email': 'scraper@example.com'}
        )
        
        # Initialize company
        self.company, created = Company.objects.get_or_create(
            name='The Creative Store',
            defaults={
                'website': 'https://thecreativestore.com.au',
                'description': 'Creative talent agency and job board',
                'company_size': 'medium'
            }
        )
        
        logger.info(f"Initialized TheCreativeStoreScraper - Max jobs: {max_jobs}")
    
    def extract_salary_info(self, salary_text):
        """Extract salary information from text."""
        if not salary_text:
            return None, None, 'yearly', None
        
        # Clean the text
        salary_text = re.sub(r'\s+', ' ', salary_text.strip())
        
        # Common patterns for Australian salaries
        patterns = [
            # $50,000 - $60,000
            r'\$?([\d,]+)\s*-\s*\$?([\d,]+)',
            # $50,000
            r'\$?([\d,]+)',
            # 50k - 60k
            r'([\d,]+)k\s*-\s*([\d,]+)k',
            # 50k
            r'([\d,]+)k',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, salary_text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 2:
                    # Range
                    min_sal = re.sub(r'[,$]', '', groups[0])
                    max_sal = re.sub(r'[,$]', '', groups[1])
                    
                    # Handle 'k' suffix
                    if 'k' in salary_text.lower():
                        min_sal = str(int(min_sal) * 1000)
                        max_sal = str(int(max_sal) * 1000)
                    
                    return Decimal(min_sal), Decimal(max_sal), 'yearly', salary_text
                else:
                    # Single value
                    sal = re.sub(r'[,$]', '', groups[0])
                    if 'k' in salary_text.lower():
                        sal = str(int(sal) * 1000)
                    return Decimal(sal), Decimal(sal), 'yearly', salary_text
        
        return None, None, 'yearly', salary_text
    
    def extract_skills_from_description(self, description):
        """Extract skills and preferred skills from job description."""
        if not description:
            return [], []
        
        # Common technical skills keywords
        technical_skills = [
            # Programming Languages
            'python', 'javascript', 'java', 'c#', 'php', 'ruby', 'swift', 'kotlin', 'go', 'rust',
            'html', 'css', 'sass', 'less', 'typescript', 'sql', 'nosql', 'mongodb', 'postgresql',
            
            # Frameworks & Libraries
            'react', 'angular', 'vue', 'node.js', 'express', 'django', 'flask', 'laravel',
            'spring', 'bootstrap', 'jquery', 'redux', 'graphql', 'rest api', 'api',
            
            # Creative/Design Skills
            'photoshop', 'illustrator', 'indesign', 'sketch', 'figma', 'adobe creative suite',
            'after effects', 'premiere pro', 'cinema 4d', '3ds max', 'maya', 'blender',
            'ui/ux', 'user experience', 'user interface', 'graphic design', 'web design',
            'brand design', 'print design', 'digital design', 'motion graphics', 'animation',
            'typography', 'layout design', 'concept development', 'creative direction',
            
            # Marketing & Content
            'content marketing', 'social media', 'seo', 'sem', 'google ads', 'facebook ads',
            'email marketing', 'copywriting', 'content creation', 'brand strategy', 'campaign management',
            'analytics', 'google analytics', 'digital marketing', 'marketing automation',
            
            # Tools & Platforms
            'git', 'github', 'docker', 'kubernetes', 'aws', 'azure', 'google cloud',
            'jenkins', 'jira', 'confluence', 'slack', 'trello', 'asana', 'monday.com',
            'salesforce', 'hubspot', 'mailchimp', 'wordpress', 'shopify', 'woocommerce',
            
            # General Skills
            'project management', 'agile', 'scrum', 'kanban', 'team leadership', 'communication',
            'problem solving', 'analytical thinking', 'time management', 'collaboration',
            'client management', 'presentation skills', 'research', 'data analysis'
        ]
        
        # Convert description to lowercase for searching
        desc_lower = description.lower()
        
        # Find skills mentioned in description
        found_skills = []
        for skill in technical_skills:
            # Look for whole word matches
            if re.search(r'\b' + re.escape(skill.lower()) + r'\b', desc_lower):
                found_skills.append(skill.title())
        
        # Remove duplicates while preserving order
        found_skills = list(dict.fromkeys(found_skills))
        
        # Split into required and preferred based on context
        required_skills = []
        preferred_skills = []
        
        # Look for sections that indicate requirements vs preferences
        required_indicators = ['required', 'must have', 'essential', 'mandatory', 'need to have']
        preferred_indicators = ['preferred', 'desirable', 'nice to have', 'bonus', 'advantage', 'plus']
        
        # Split description into sentences/sections
        sentences = re.split(r'[.\n]', description)
        
        for skill in found_skills:
            skill_lower = skill.lower()
            is_required = False
            is_preferred = False
            
            # Check context around the skill
            for sentence in sentences:
                if skill_lower in sentence.lower():
                    sentence_lower = sentence.lower()
                    
                    # Check if it's in a required context
                    if any(indicator in sentence_lower for indicator in required_indicators):
                        is_required = True
                        break
                    # Check if it's in a preferred context
                    elif any(indicator in sentence_lower for indicator in preferred_indicators):
                        is_preferred = True
            
            # Default to required if not specifically marked as preferred
            if is_required or (not is_preferred):
                required_skills.append(skill)
            else:
                preferred_skills.append(skill)
        
        # Limit to reasonable number of skills
        required_skills = required_skills[:10]
        preferred_skills = preferred_skills[:8]
        
        return required_skills, preferred_skills
    
    def convert_text_to_html(self, text):
        """Convert plain text description to proper HTML format."""
        if not text:
            return text
        
        # Clean up the text first
        text = text.strip()
        
        # Split into paragraphs
        paragraphs = re.split(r'\n\s*\n', text)
        
        html_content = []
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # Check if it's a list item
            if re.match(r'^[•·*-]\s', para) or re.match(r'^\d+\.\s', para):
                # Handle bullet points and numbered lists
                lines = para.split('\n')
                if len(lines) > 1:
                    html_content.append('<ul>')
                    for line in lines:
                        line = line.strip()
                        if line:
                            # Remove bullet points
                            line = re.sub(r'^[•·*-]\s*', '', line)
                            line = re.sub(r'^\d+\.\s*', '', line)
                            html_content.append(f'<li>{line}</li>')
                    html_content.append('</ul>')
                else:
                    # Single list item
                    para = re.sub(r'^[•·*-]\s*', '', para)
                    para = re.sub(r'^\d+\.\s*', '', para)
                    html_content.append(f'<p>• {para}</p>')
            
            # Check if it's a heading (all caps or starts with specific words)
            elif (para.isupper() and len(para) < 100) or \
                 any(para.lower().startswith(heading) for heading in 
                     ['about', 'responsibilities', 'requirements', 'qualifications', 
                      'skills', 'experience', 'what we offer', 'benefits', 'key responsibilities']):
                html_content.append(f'<h3>{para}</h3>')
            
            # Regular paragraph
            else:
                # Handle line breaks within paragraphs
                para = para.replace('\n', '<br>\n')
                html_content.append(f'<p>{para}</p>')
        
        return '\n'.join(html_content)
    
    def scrape_company_logo(self, page):
        """Scrape company logo from the current page."""
        try:
            logo_selectors = [
                'img[alt*="logo"]',
                'img[class*="logo"]',
                'img[id*="logo"]',
                '.company-logo img',
                '.logo img',
                '#logo img',
                '.header img',
                '.brand img',
                'img[src*="logo"]'
            ]
            
            for selector in logo_selectors:
                try:
                    logo_element = page.locator(selector).first
                    if logo_element.is_visible():
                        src = logo_element.get_attribute('src')
                        if src:
                            # Convert relative URL to absolute
                            if src.startswith('//'):
                                logo_url = 'https:' + src
                            elif src.startswith('/'):
                                logo_url = urljoin(self.base_url, src)
                            elif src.startswith('http'):
                                logo_url = src
                            else:
                                logo_url = urljoin(self.base_url, src)
                            
                            # Validate that it's actually an image
                            if any(ext in logo_url.lower() for ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp']):
                                logger.info(f"Found company logo: {logo_url}")
                                return logo_url
                except Exception as e:
                    continue
            
            logger.warning("No company logo found")
            return None
            
        except Exception as e:
            logger.error(f"Error scraping company logo: {str(e)}")
            return None
    
    def extract_location_info(self, location_text):
        """Extract and get/create location from text."""
        if not location_text:
            return None
        
        # Clean location text
        location_text = location_text.strip()
        
        # Try to get existing location - use filter().first() to handle duplicates
        try:
            location = Location.objects.filter(name__iexact=location_text).first()
            if location:
                return location
        except Exception as e:
            logger.warning(f"Error querying location '{location_text}': {str(e)}")
        
        # Parse Australian location format
        parts = [part.strip() for part in location_text.split(',')]
        
        if len(parts) >= 2:
            city = parts[0]
            state = parts[1]
            
            # Try to get or create new location
            location, created = Location.objects.get_or_create(
                name=location_text,
                defaults={
                    'city': city,
                    'state': state,
                    'country': 'Australia'
                }
            )
            return location
        else:
            # Single location name
            location, created = Location.objects.get_or_create(
                name=location_text,
                defaults={
                    'country': 'Australia'
                }
            )
            return location
    
    def extract_job_data_from_card(self, element, index):
        """Extract job data directly from job card element."""
        try:
            job_data = {}
            
            # Extract title
            try:
                title_element = element.locator('.title').first
                job_data['title'] = title_element.inner_text().strip()
            except:
                job_data['title'] = f'Job Position {index + 1}'
            
            # Extract description (get HTML content)
            try:
                description_element = element.locator('.description').first
                # Try to get HTML content first, then fall back to text
                try:
                    description_html = description_element.inner_html().strip()
                    if description_html:
                        job_data['description'] = description_html
                    else:
                        description_text = description_element.inner_text().strip()
                        job_data['description'] = self.convert_text_to_html(description_text)
                except:
                    description_text = description_element.inner_text().strip()
                    job_data['description'] = self.convert_text_to_html(description_text)
            except:
                job_data['description'] = '<p>No description available</p>'
            
            # Extract date and job ID
            try:
                date_element = element.locator('.date').first
                date_text = date_element.inner_text().strip()
                job_data['posted_ago'] = date_text
                
                # Extract job ID if present
                if '#' in date_text:
                    parts = date_text.split('#')
                    if len(parts) > 1:
                        job_data['external_id'] = parts[1].strip()
            except:
                job_data['posted_ago'] = None
                job_data['external_id'] = None
            
            # Extract tags (location, job type, salary)
            try:
                tag_elements = element.locator('.tags li').all()
                tags = []
                location = None
                job_type = None
                salary = None
                
                for tag in tag_elements:
                    tag_text = tag.inner_text().strip()
                    tags.append(tag_text)
                    
                    # Determine what type of tag this is
                    if any(city in tag_text for city in ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Remote', 'Marrickville', 'Surry Hills', 'Cheltenham','Townsville']):
                        location = tag_text
                    elif any(job_type_word in tag_text.lower() for job_type_word in ['permanent', 'contract', 'part time', 'full time', 'casual', 'freelance']):
                        job_type = tag_text
                    elif '$' in tag_text or 'k' in tag_text.lower():
                        salary = tag_text
                
                job_data['location'] = location
                job_data['job_type'] = job_type  
                job_data['salary'] = salary
                job_data['tags'] = ', '.join(tags)
                
            except Exception as e:
                logger.warning(f"Error extracting tags: {str(e)}")
                job_data['location'] = None
                job_data['job_type'] = None
                job_data['salary'] = None
                job_data['tags'] = ''
            
            # Try to extract company logo from job card
            try:
                logo_element = element.locator('img').first
                if logo_element:
                    logo_src = logo_element.get_attribute('src')
                    if logo_src:
                        if logo_src.startswith('//'):
                            job_data['company_logo'] = 'https:' + logo_src
                        elif logo_src.startswith('/'):
                            job_data['company_logo'] = urljoin(self.base_url, logo_src)
                        elif logo_src.startswith('http'):
                            job_data['company_logo'] = logo_src
                        else:
                            job_data['company_logo'] = urljoin(self.base_url, logo_src)
                    else:
                        job_data['company_logo'] = None
                else:
                    job_data['company_logo'] = None
            except:
                job_data['company_logo'] = None

            # Additional info
            job_data['additional_info'] = {
                'card_index': index,
                'source': 'thecreativestore.com.au',
                'extraction_method': 'job_card'
            }
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job data from card: {str(e)}")
            return None
    
    def parse_job_details(self, page, job_url):
        """Parse detailed job information from job detail page."""
        try:
            logger.info(f"Parsing job details from: {job_url}")
            
            # Navigate to job detail page
            page.goto(job_url, wait_until='networkidle')
            time.sleep(random.uniform(1, 3))
            
            # Extract job details - these selectors will need to be updated
            # based on the actual HTML structure you provide
            job_data = {}
            
            # Basic job information
            try:
                job_data['title'] = page.locator('h1').first.inner_text().strip()
            except:
                job_data['title'] = 'Unknown Position'
            
            try:
                # Try to get HTML content first, then fall back to text
                description_element = page.locator('.job-description, .description, .content').first
                try:
                    description_html = description_element.inner_html().strip()
                    if description_html:
                        job_data['description'] = description_html
                    else:
                        description_text = description_element.inner_text().strip()
                        job_data['description'] = self.convert_text_to_html(description_text)
                except:
                    description_text = description_element.inner_text().strip()
                    job_data['description'] = self.convert_text_to_html(description_text)
            except:
                job_data['description'] = '<p>No description available</p>'
            
            # Location
            try:
                location_text = page.locator('.location, .job-location').first.inner_text().strip()
                job_data['location'] = location_text
            except:
                job_data['location'] = None
            
            # Salary
            try:
                salary_text = page.locator('.salary, .job-salary, .pay').first.inner_text().strip()
                job_data['salary'] = salary_text
            except:
                job_data['salary'] = None
            
            # Job type
            try:
                job_type_text = page.locator('.job-type, .employment-type').first.inner_text().strip()
                job_data['job_type'] = job_type_text
            except:
                job_data['job_type'] = None
            
            # Posted date
            try:
                posted_text = page.locator('.posted, .date-posted, .job-date').first.inner_text().strip()
                job_data['posted_ago'] = posted_text
            except:
                job_data['posted_ago'] = None
            
            # Scrape company logo
            try:
                job_data['company_logo'] = self.scrape_company_logo(page)
            except Exception as e:
                logger.warning(f"Error scraping company logo: {str(e)}")
                job_data['company_logo'] = None

            # Additional information
            try:
                # Extract any additional job details
                job_data['additional_info'] = {
                    'scraped_from': job_url,
                    'source': 'thecreativestore.com.au'
                }
            except:
                job_data['additional_info'] = {}
            
            return job_data
            
        except Exception as e:
            logger.error(f"Error parsing job details from {job_url}: {str(e)}")
            return None
    
    def save_job_to_database(self, job_data, job_url):
        """Save job data to database using Django models."""
        from concurrent.futures import ThreadPoolExecutor
        import concurrent.futures
        
        def save_in_thread():
            try:
                # Close any existing database connections to avoid async issues
                from django.db import connection
                connection.close()
                
                with transaction.atomic():
                    # Check if job already exists
                    if JobPosting.objects.filter(external_url=job_url).exists():
                        logger.info(f"Job already exists: {job_url}")
                        return 'skipped'
                    
                    # Process location
                    location = None
                    if job_data.get('location'):
                        location = self.extract_location_info(job_data['location'])
                    
                    # Process salary
                    salary_min, salary_max, salary_type, salary_raw = self.extract_salary_info(job_data.get('salary'))
                    
                    # Extract skills from description
                    description_text = job_data.get('description', '')
                    # Convert HTML to text for skill extraction
                    if description_text:
                        # Simple HTML to text conversion for skill extraction
                        import html
                        text_for_skills = re.sub(r'<[^>]+>', ' ', description_text)
                        text_for_skills = html.unescape(text_for_skills)
                        required_skills, preferred_skills = self.extract_skills_from_description(text_for_skills)
                    else:
                        required_skills, preferred_skills = [], []
                    
                    # Convert skills lists to comma-separated strings
                    skills_str = ', '.join(required_skills) if required_skills else ''
                    preferred_skills_str = ', '.join(preferred_skills) if preferred_skills else ''
                    
                    # Update company logo if available
                    company_logo_url = job_data.get('company_logo')
                    if company_logo_url and self.company:
                        try:
                            # Only update if company doesn't have a logo or if we have a better one
                            if not self.company.logo:
                                self.company.logo = company_logo_url
                                self.company.save()
                                logger.info(f"Updated company logo: {company_logo_url}")
                        except Exception as e:
                            logger.warning(f"Error updating company logo: {str(e)}")
                    
                    # Determine job category (default to 'other' for now)
                    job_category = 'other'  # Could be enhanced with keyword matching
                    
                    # Determine job type
                    job_type_mapping = {
                        'full time': 'full_time',
                        'full-time': 'full_time',
                        'part time': 'part_time',
                        'part-time': 'part_time',
                        'contract': 'contract',
                        'temporary': 'temporary',
                        'permanent': 'permanent',
                        'casual': 'casual',
                        'freelance': 'freelance',
                        'internship': 'internship',
                    }
                    
                    job_type = 'full_time'  # default
                    if job_data.get('job_type'):
                        job_type_text = job_data['job_type'].lower()
                        for key, value in job_type_mapping.items():
                            if key in job_type_text:
                                job_type = value
                                break
                    
                    # Create job posting
                    job_posting = JobPosting.objects.create(
                        title=job_data.get('title', 'Unknown Position'),
                        description=job_data.get('description', '<p>No description available</p>'),
                        company=self.company,
                        posted_by=self.user,
                        location=location,
                        job_category=job_category,
                        job_type=job_type,
                        salary_min=salary_min,
                        salary_max=salary_max,
                        salary_type=salary_type,
                        salary_raw_text=salary_raw,
                        salary_currency='AUD',
                        external_source='thecreativestore.com.au',
                        external_url=job_url,
                        external_id=job_data.get('external_id', ''),
                        posted_ago=job_data.get('posted_ago', ''),
                        tags=job_data.get('tags', ''),
                        skills=skills_str,
                        preferred_skills=preferred_skills_str,
                        additional_info=job_data.get('additional_info', {}),
                        status='active'
                    )
                    
                    logger.info(f"Saved job: {job_posting.title} ({job_posting.id})")
                    return 'saved'
                    
            except Exception as e:
                logger.error(f"Error saving job to database: {str(e)}")
                return 'error'
        
        # Execute the database operation in a thread
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(save_in_thread)
                result = future.result(timeout=30)  # 30 second timeout
                
                if result == 'saved':
                    self.scraped_count += 1
                    return True
                elif result == 'skipped':
                    self.skipped_count += 1
                    return False
                else:
                    self.error_count += 1
                    return False
                    
        except Exception as e:
            logger.error(f"Error in threaded database operation: {str(e)}")
            self.error_count += 1
            return False
    
    def scrape_jobs_listing(self, page):
        """Scrape job listings from the main jobs page."""
        try:
            logger.info("Scraping jobs from main listing page")
            
            # Navigate to jobs page
            page.goto(self.jobs_url, wait_until='networkidle')
            time.sleep(random.uniform(3, 5))
            
            # Wait for any dynamic content to load
            try:
                page.wait_for_load_state('domcontentloaded')
                time.sleep(2)
            except:
                pass
            
            # First, let's inspect the page structure
            logger.info("Inspecting page structure...")
            
            # Save page content for debugging
            page_content = page.content()
            with open('debug_page_content.html', 'w', encoding='utf-8') as f:
                f.write(page_content)
            logger.info("Saved page content to debug_page_content.html")
            
            # Look for various job-related elements
            job_links = []
            
            # Try different common job listing patterns
            job_selectors = [
                # Common job card selectors
                '.job-listing',
                '.job-item', 
                '.job-card',
                '.job',
                '.position',
                '.listing',
                '.opportunity',
                # Div-based job containers
                'div[class*="job"]',
                'div[class*="position"]',
                'div[class*="listing"]',
                # List items
                'li[class*="job"]',
                'li[class*="position"]',
                # Article elements
                'article',
                # Generic containers that might contain jobs
                '.content .item',
                '.main .item',
                '#jobs .item',
                '#content .item'
            ]
            
            logger.info("Searching for job elements...")
            found_elements = False
            
            for selector in job_selectors:
                try:
                    elements = page.locator(selector).all()
                    if elements:
                        logger.info(f"Found {len(elements)} elements with selector: {selector}")
                        found_elements = True
                        
                        # For The Creative Store, job data is embedded in job cards, not separate pages
                        # Extract job data directly from job cards
                        if selector == '.job-card':
                            logger.info("Extracting job data from job cards...")
                            for i, element in enumerate(elements):
                                try:
                                    job_data = self.extract_job_data_from_card(element, i)
                                    if job_data:
                                        # Create a unique URL for this job
                                        job_url = f"{self.jobs_url}#job-{i+1}"
                                        job_links.append((job_url, job_data))
                                        logger.info(f"Extracted job {i+1}: {job_data.get('title', 'Unknown')}")
                                except Exception as e:
                                    logger.error(f"Error extracting job data from card {i+1}: {str(e)}")
                                    continue
                            break  # We found job cards, no need to check other selectors
                        else:
                            # For other selectors, look for links
                            for element in elements:
                                try:
                                    # Look for links within each element
                                    links = element.locator('a').all()
                                    for link in links:
                                        href = link.get_attribute('href')
                                        if href:
                                            full_url = urljoin(self.base_url, href)
                                            
                                            # Filter for Australian job links
                                            if (
                                                'thecreativestore.com.au' in full_url and
                                                full_url != self.jobs_url and
                                                not any(domain in full_url for domain in ['.co.nz', '.uk', '.com.sg'])
                                            ):
                                                if full_url not in job_links:
                                                    job_links.append(full_url)
                                                    logger.info(f"Found job link: {full_url}")
                                        
                                except Exception as e:
                                    continue
                        
                        if job_links:
                            break  # Found some links, use these
                            
                except Exception as e:
                    continue
            
            # If still no job links, let's check all links on the page more broadly
            if not job_links:
                logger.warning("No job elements found. Checking all page links...")
                all_links = page.locator('a').all()
                
                for link in all_links:
                    try:
                        href = link.get_attribute('href')
                        text = link.inner_text().strip()
                        
                        if href and text:
                            full_url = urljoin(self.base_url, href)
                            
                            # Look for any link that might be a job
                            if (
                                'thecreativestore.com.au' in full_url and
                                (
                                    '/job' in href.lower() or
                                    '/position' in href.lower() or
                                    '/role' in href.lower() or
                                    any(keyword in text.lower() for keyword in ['designer', 'creative', 'marketing', 'art', 'digital', 'brand', 'copywriter', 'producer'])
                                ) and
                                full_url != self.jobs_url
                            ):
                                if full_url not in job_links:
                                    job_links.append(full_url)
                                    logger.info(f"Found potential job link: {full_url} - Text: {text[:50]}")
                    except Exception as e:
                        continue
            
            # If STILL no links, let's see what's actually on the page
            if not job_links:
                logger.warning("Still no job links found. Analyzing page content...")
                
                # Check page title
                title = page.title()
                logger.info(f"Page title: {title}")
                
                # Check for any text that mentions jobs
                page_text = page.inner_text()
                if 'job' in page_text.lower() or 'position' in page_text.lower():
                    logger.info("Page contains job-related text")
                    
                    # Look for any clickable elements with job-related text
                    clickable_elements = page.locator('a, button, div[onclick], [data-href]').all()
                    for element in clickable_elements[:20]:  # Check first 20
                        try:
                            text = element.inner_text().strip()
                            if text and any(keyword in text.lower() for keyword in ['view', 'apply', 'more', 'details', 'read']):
                                href = element.get_attribute('href') or element.get_attribute('data-href')
                                onclick = element.get_attribute('onclick')
                                logger.info(f"Clickable element: Text='{text[:30]}', href='{href}', onclick='{onclick}'")
                        except:
                            continue
                else:
                    logger.warning("No job-related text found on page")
                
                # Show first few links found
                all_links = page.locator('a').all()
                logger.info("First 15 links on page:")
                for i, link in enumerate(all_links[:15]):
                    try:
                        href = link.get_attribute('href')
                        text = link.inner_text().strip()
                        logger.info(f"{i+1}. {href} - '{text[:40]}'")
                    except:
                        continue
            
            logger.info(f"Total job links found: {len(job_links)}")
            return job_links
            
        except Exception as e:
            logger.error(f"Error scraping jobs listing: {str(e)}")
            return []
    
    def run_scraper(self):
        """Main scraper execution method."""
        logger.info("Starting The Creative Store job scraper")
        
        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
            )
            
            # Create context with realistic settings
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = context.new_page()
            
            try:
                # Get job links from main page
                job_links = self.scrape_jobs_listing(page)
                
                if not job_links:
                    logger.warning("No job links found")
                    return
                
                # Limit jobs if specified
                if self.max_jobs:
                    job_links = job_links[:self.max_jobs]
                
                logger.info(f"Processing {len(job_links)} job links")
                
                # Process each job
                for i, job_item in enumerate(job_links, 1):
                    if self.max_jobs and i > self.max_jobs:
                        break
                    
                    # Handle both tuple (job_url, job_data) and string (job_url) formats
                    if isinstance(job_item, tuple):
                        job_url, job_data = job_item
                        logger.info(f"Processing embedded job {i}/{len(job_links)}: {job_data.get('title', 'Unknown')}")
                        
                        # Save job data directly (already extracted from job card)
                        if job_data:
                            self.save_job_to_database(job_data, job_url)
                        else:
                            logger.warning(f"No job data for: {job_url}")
                            self.error_count += 1
                    else:
                        job_url = job_item
                        logger.info(f"Processing job {i}/{len(job_links)}: {job_url}")
                        
                        # Parse job details from separate page
                        job_data = self.parse_job_details(page, job_url)
                        
                        if job_data:
                            # Save to database
                            self.save_job_to_database(job_data, job_url)
                        else:
                            logger.warning(f"Failed to parse job data for: {job_url}")
                            self.error_count += 1
                        
                        # Random delay between requests
                        time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                logger.error(f"Error in main scraper loop: {str(e)}")
            
            finally:
                browser.close()
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print scraping summary."""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Source: The Creative Store (thecreativestore.com.au)")
        logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Jobs scraped: {self.scraped_count}")
        logger.info(f"Jobs skipped (duplicates): {self.skipped_count}")
        logger.info(f"Errors: {self.error_count}")
        logger.info(f"Total processed: {self.scraped_count + self.skipped_count + self.error_count}")
        logger.info("=" * 60)


def main():
    """Main entry point."""
    max_jobs = None
    headless = True
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid max_jobs argument. Please provide a number.")
            sys.exit(1)
    
    # For development, run with visible browser
    if len(sys.argv) > 2 and sys.argv[2] == '--visible':
        headless = False
    
    # Create and run scraper
    scraper = TheCreativeStoreScraper(max_jobs=max_jobs, headless=headless)
    scraper.run_scraper()


if __name__ == "__main__":
    main()


def run(max_jobs=None, headless=True):
    """Automation entrypoint for The Creative Store scraper."""
    try:
        scraper = TheCreativeStoreScraper(max_jobs=max_jobs, headless=headless)
        scraper.run_scraper()
        return {
            'success': True,
            'jobs_scraped': scraper.scraped_count,
            'skipped_count': scraper.skipped_count,
            'error_count': scraper.error_count,
            'message': 'The Creative Store scraping completed'
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
