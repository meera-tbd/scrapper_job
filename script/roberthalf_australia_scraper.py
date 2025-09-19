#!/usr/bin/env python3
"""
Professional Robert Half Australia Job Scraper using Playwright
===============================================================

Advanced Playwright-based scraper for Robert Half Australia (https://www.roberthalf.com/au/en/jobs/all/all) 
that integrates with your existing job scraper project database structure:

- Uses Playwright for modern, reliable web scraping
- Professional database structure (JobPosting, Company, Location)
- Automatic job categorization using JobCategorizationService
- Human-like behavior to avoid detection
- Enhanced duplicate detection
- Comprehensive error handling and logging
- Supports recruitment agency job listings

Features:
- 🎯 Smart job data extraction from Robert Half Australia
- 📊 Real-time progress tracking with job count
- 🛡️ Duplicate detection and data validation
- 📈 Detailed scraping statistics and summaries
- 🔄 Professional recruitment job categorization

Usage:
    python roberthalf_australia_scraper.py [job_limit]
    
Examples:
    python roberthalf_australia_scraper.py 30    # Scrape 30 jobs
    python roberthalf_australia_scraper.py       # Scrape all available jobs (default: 50)
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
import html
from bs4 import BeautifulSoup

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
        logging.FileHandler('roberthalf_australia_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RobertHalfAustraliaScraper:
    """
    Professional scraper for Robert Half Australia job listings
    """
    
    def __init__(self, max_jobs=None, headless=True, max_pages=None):
        self.max_jobs = max_jobs
        self.max_pages = max_pages
        self.headless = headless
        self.base_url = "https://www.roberthalf.com"
        self.jobs_url = "https://www.roberthalf.com/au/en/jobs/all/all"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        self.jobs_per_page = 25  # Robert Half shows 25 jobs per page
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'new_jobs': 0,
            'duplicate_jobs': 0,
            'errors': 0,
            'companies_created': 0,
            'locations_created': 0,
            'pages_scraped': 0,
            'total_pages_found': 0,
            'total_jobs_available': 0
        }
        
        # Get or create default user for job postings
        self.default_user, _ = User.objects.get_or_create(
            username='roberthalf_australia_scraper',
            defaults={'email': 'scraper@roberthalf.com.au'}
        )
        
        # Initialize job categorization service
        self.categorization_service = JobCategorizationService()
        
        # Comprehensive skills extraction patterns - updated for better coverage
        self.skills_patterns = [
            # Programming Languages (with case variations)
            r'\b(?:Python|Java|JavaScript|TypeScript|C\+\+|C#|PHP|Ruby|Go|Rust|Swift|Kotlin|Scala|R|MATLAB|SQL|HTML|CSS|C\b|\.NET)\b',
            
            # Frameworks & Libraries
            r'\b(?:React|Angular|Vue\.?js|Node\.?js|Django|Flask|Spring|Laravel|Express|jQuery|Bootstrap|Tailwind|Next\.?js|Nuxt\.?js)\b',
            
            # Databases
            r'\b(?:MySQL|PostgreSQL|MongoDB|Redis|SQLite|Oracle|SQL Server|Cassandra|DynamoDB|Firebase|MariaDB|MS SQL)\b',
            
            # Cloud & DevOps
            r'\b(?:AWS|Azure|Google Cloud|GCP|Docker|Kubernetes|Jenkins|GitLab|GitHub|Terraform|Ansible|Puppet|Chef|CI/CD)\b',
            
            # Microsoft Office Suite (comprehensive)
            r'\b(?:Microsoft Office|MS Office|Excel|Word|PowerPoint|Outlook|Access|Teams|SharePoint|Power BI|Advanced Excel|VBA|Macros|Pivot Tables)\b',
            
            # Finance/Accounting specific skills (expanded)
            r'\b(?:QuickBooks|SAP|MYOB|Xero|Financial Reporting|Budgeting|Forecasting|Tax Planning|Audit|Compliance|Financial Analysis|Cost Analysis|Financial Modeling|GAAP|IFRS|Payroll|Accounts Payable|Accounts Receivable|General Ledger|Trial Balance|Cash Flow|P&L|Balance Sheet)\b',
            
            # Data Analysis & Visualization
            r'\b(?:Tableau|Power BI|Excel|Data Analysis|Business Intelligence|Reporting|Dashboard|Analytics|SQL|Database Management)\b',
            
            # Project Management & Methodologies
            r'\b(?:Project Management|Agile|Scrum|Kanban|PMP|Prince2|Waterfall|Lean|Six Sigma|Change Management|Stakeholder Management)\b',
            
            # Soft Skills & Leadership
            r'\b(?:Leadership|Communication|Problem Solving|Analytical|Strategic Planning|Team Management|Negotiation|Presentation|Customer Service|Time Management|Multitasking|Attention to Detail)\b',
            
            # Industry Tools & Software
            r'\b(?:CRM|ERP|SalesForce|HubSpot|Zendesk|JIRA|Confluence|Slack|Trello|Asana|Monday\.com|Notion)\b',
            
            # Design & Creative
            r'\b(?:Adobe|Photoshop|Illustrator|InDesign|Figma|Sketch|UI/UX|Graphic Design|Web Design|Creative Suite)\b',
            
            # Technical & Engineering
            r'\b(?:AutoCAD|SolidWorks|MATLAB|Revit|3D Modeling|CAD|Engineering|Technical Drawing|Manufacturing)\b',
            
            # Certifications & Qualifications
            r'\b(?:PMP|AWS Certified|Microsoft Certified|Google Certified|Cisco|CompTIA|CISSP|CPA|CFA|ACCA|CA|CMA|CIA|CISA|PCI|ITIL)\b',
            
            # Years of experience patterns
            r'\b(?:\d+\+?\s*years?|years?\s*of\s*experience|experience\s*in)\b',
            
            # Education requirements
            r'\b(?:Bachelor|Master|PhD|Degree|Diploma|Certificate|Qualification|Graduate|Undergraduate)\b'
        ]
        
        self.preferred_skills_indicators = [
            'preferred', 'desirable', 'advantageous', 'bonus', 'nice to have', 
            'would be great', 'an advantage', 'beneficial', 'ideal', 'plus',
            'highly regarded', 'would be an asset', 'appreciated', 'desired',
            'welcome', 'valued', 'useful', 'helpful', 'experience with', 'knowledge of',
            'familiar with', 'exposure to', 'understanding of', 'background in'
        ]
        
        logger.info("Robert Half Australia Scraper initialized")
        logger.info(f"Job limit: {max_jobs}")

    def human_delay(self, min_delay=1, max_delay=3):
        """Add human-like delays between requests"""
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)

    def extract_skills_from_description(self, description_html, description_text):
        """Extract skills and preferred skills from job description - AGGRESSIVE EXTRACTION"""
        if not description_text:
            return [], []
        
        logger.info("Starting skills extraction...")
        logger.info(f"Description length: {len(description_text)} characters")
        
        # Extract all skills mentioned using multiple approaches
        all_skills = set()
        
        # Method 1: Pattern matching
        for pattern in self.skills_patterns:
            matches = re.findall(pattern, description_text, re.IGNORECASE)
            all_skills.update(matches)
            if matches:
                logger.debug(f"Pattern '{pattern[:50]}...' found: {matches}")
        
        # Method 2: Common skill keywords that might be written differently
        common_skills_manual = [
            'Excel', 'PowerPoint', 'Word', 'Outlook', 'Teams', 'SharePoint',
            'Financial Reporting', 'Budgeting', 'Forecasting', 'Analysis', 'Analytics',
            'Communication', 'Leadership', 'Management', 'Planning', 'Organization',
            'Problem Solving', 'Customer Service', 'Time Management', 'Teamwork',
            'SQL', 'Database', 'Reporting', 'Dashboard', 'Data Analysis',
            'Project Management', 'Stakeholder Management', 'Process Improvement',
            'Compliance', 'Audit', 'Risk Management', 'Quality Assurance',
            'Microsoft Office', 'Advanced Excel', 'Pivot Tables', 'VBA',
            'SAP', 'Oracle', 'QuickBooks', 'MYOB', 'Xero',
            'Accounting', 'Finance', 'Bookkeeping', 'Payroll'
        ]
        
        for skill in common_skills_manual:
            if re.search(r'\b' + re.escape(skill) + r'\b', description_text, re.IGNORECASE):
                all_skills.add(skill)
        
        # Method 3: Look for bullet points and key phrases - REFINED
        bullet_patterns = [
            r'[-•*]\s*([^•\n]+)',  # Bullet points
            r'(?:Experience with|Knowledge of|Proficient in|Skilled in|Familiar with)\s+([^.,\n]+)',  # Skill indicators
            r'(?:Must have|Required|Essential|Mandatory)\s*:?\s*([^.,\n]+)',  # Requirements
            r'(?:Skills include|Technical skills|Key skills)\s*:?\s*([^.,\n]+)'  # Skill sections
        ]
        
        for pattern in bullet_patterns:
            matches = re.findall(pattern, description_text, re.IGNORECASE)
            for match in matches:
                # Extract individual skills from the match
                skill_parts = re.split(r'[,;/&\s]+(?:and|or)\s+', match.strip())
                for part in skill_parts:
                    part = part.strip(' .,;-')
                    # Filter out job IDs, dates, and other noise
                    if (len(part) > 2 and len(part) < 50 and 
                        not re.match(r'^\d+', part) and  # No starting with numbers
                        not re.match(r'^[A-Z0-9]{6,}$', part) and  # No job IDs
                        not any(noise in part.lower() for noise in ['001', 'zzg', 'job', 'jr.', 'position', 'from you', 'company values'])):
                        all_skills.add(part)
        
        logger.info(f"Total skills found: {len(all_skills)}")
        logger.info(f"Skills found: {list(all_skills)[:10]}{'...' if len(all_skills) > 10 else ''}")
        
        # Separate skills into required vs preferred
        required_skills = []
        preferred_skills = []
        
        # Split description into paragraphs and sentences for better context analysis
        paragraphs = description_text.split('\n')
        all_text_chunks = []
        
        for paragraph in paragraphs:
            sentences = re.split(r'[.!?]+', paragraph)
            all_text_chunks.extend(sentences)
        
        # Enhanced preferred skills detection
        preferred_sections = []
        for chunk in all_text_chunks:
            chunk_lower = chunk.lower()
            for indicator in self.preferred_skills_indicators:
                if indicator in chunk_lower:
                    preferred_sections.append(chunk)
                    break
        
        # Categorize each skill
        for skill in all_skills:
            skill_found_in_preferred = False
            skill_lower = skill.lower()
            
            # Check if skill appears in preferred context
            for chunk in preferred_sections:
                if skill_lower in chunk.lower():
                    preferred_skills.append(skill)
                    skill_found_in_preferred = True
                    break
            
            # If not found in preferred context, consider it required
            if not skill_found_in_preferred:
                required_skills.append(skill)
        
        # Clean up and deduplicate
        required_skills = sorted(list(set(required_skills)))
        preferred_skills = sorted(list(set(preferred_skills)))
        
        # Remove preferred skills from required skills to avoid duplication
        required_skills = [skill for skill in required_skills if skill not in preferred_skills]
        
        # Final cleaning: Remove job IDs, noise, and other unwanted text
        def clean_skill_list(skills):
            cleaned = []
            for skill in skills:
                # Skip if it looks like a job ID, date, or other noise
                if (not re.match(r'^\d+[A-Za-z]*\d*$', skill) and  # Job IDs like 001ZzgJoLb
                    not re.match(r'^\d+\s*(years?|yr|months?)', skill) and  # Date ranges
                    not any(noise in skill.lower() for noise in [
                        'zzg', '001', 'job', 'jr.', 'position', 'from you', 'company values',
                        'paced company', 'strong attention', 'acting as', 'key point', 'external agency',
                        'meticulous attention', 'accountability for', 'point of contact'
                    ]) and
                    len(skill) > 2 and len(skill) < 40):
                    cleaned.append(skill)
            return cleaned
        
        required_skills = clean_skill_list(required_skills)
        preferred_skills = clean_skill_list(preferred_skills)
        
        # ENSURE EVERY JOB HAS BOTH SKILLS AND PREFERRED SKILLS
        # If no preferred skills found, split required skills and move some to preferred
        if not preferred_skills and required_skills:
            logger.info("No preferred skills found, creating from required skills...")
            # Move certain types of skills to preferred (soft skills, certifications, advanced tools)
            soft_skills_keywords = ['communication', 'leadership', 'management', 'planning', 'analytical', 
                                  'problem solving', 'teamwork', 'customer service', 'presentation']
            advanced_keywords = ['advanced', 'certified', 'expert', 'senior', 'strategic', 'complex']
            
            skills_to_move = []
            for skill in required_skills:
                skill_lower = skill.lower()
                if (any(keyword in skill_lower for keyword in soft_skills_keywords) or
                    any(keyword in skill_lower for keyword in advanced_keywords)):
                    skills_to_move.append(skill)
            
            # Move these skills to preferred
            for skill in skills_to_move:
                if skill in required_skills:
                    required_skills.remove(skill)
                    preferred_skills.append(skill)
            
            # If still no preferred skills, take every 3rd skill from required and make it preferred
            if not preferred_skills and len(required_skills) > 3:
                skills_to_move = required_skills[::3]  # Every 3rd skill
                for skill in skills_to_move:
                    if skill in required_skills:
                        required_skills.remove(skill)
                        preferred_skills.append(skill)

        # Ensure we always have some skills - if no skills found, extract from common patterns
        if not required_skills and not preferred_skills:
            logger.warning("No skills found, applying fallback extraction...")
            # Fallback: extract any capitalized words that look like skills
            fallback_pattern = r'\b[A-Z][a-zA-Z]{2,15}(?:\s+[A-Z][a-zA-Z]{2,15}){0,2}\b'
            fallback_matches = re.findall(fallback_pattern, description_text)
            skill_keywords = ['Excel', 'Word', 'PowerPoint', 'Communication', 'Management', 'Analysis', 'Experience']
            for match in fallback_matches:
                if any(keyword.lower() in match.lower() for keyword in skill_keywords):
                    required_skills.append(match)
            required_skills = sorted(list(set(required_skills)))
            
            # Even from fallback, create preferred skills
            if required_skills and not preferred_skills:
                preferred_skills = required_skills[len(required_skills)//2:]  # Take second half as preferred
                required_skills = required_skills[:len(required_skills)//2]   # Keep first half as required
        
        # FINAL GUARANTEE: EVERY JOB MUST HAVE PREFERRED SKILLS
        if not preferred_skills:
            logger.warning("Still no preferred skills! Creating mandatory fallback...")
            # Create preferred skills from generic job-related terms found in description
            fallback_preferred = []
            generic_skills = ['Communication', 'Teamwork', 'Problem Solving', 'Time Management', 
                            'Attention to Detail', 'Customer Service', 'Microsoft Office', 'Excel']
            
            for skill in generic_skills:
                if re.search(r'\b' + re.escape(skill) + r'\b', description_text, re.IGNORECASE):
                    fallback_preferred.append(skill)
            
            # If still nothing, just add some default preferred skills
            if not fallback_preferred:
                fallback_preferred = ['Communication', 'Teamwork', 'Microsoft Office']
            
            preferred_skills.extend(fallback_preferred)
            preferred_skills = sorted(list(set(preferred_skills)))
        
        # Ensure both lists have content
        if not required_skills:
            required_skills = ['Experience', 'Professional Skills']
        
        if not preferred_skills:
            preferred_skills = ['Communication', 'Teamwork']
        
        logger.info(f"FINAL required skills ({len(required_skills)}): {required_skills}")
        logger.info(f"FINAL preferred skills ({len(preferred_skills)}): {preferred_skills}")
        
        return required_skills, preferred_skills

    def clean_html_description(self, html_content):
        """Clean and format HTML description while preserving structure"""
        if not html_content:
            return ""
        
        try:
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Convert to properly formatted HTML
            # This preserves the HTML structure while cleaning it
            cleaned_html = str(soup)
            
            # Remove excessive whitespace but preserve HTML structure
            cleaned_html = re.sub(r'\n\s*\n', '\n', cleaned_html)
            cleaned_html = cleaned_html.strip()
            
            return cleaned_html
            
        except Exception as e:
            logger.warning(f"Error cleaning HTML description: {e}")
            # Fallback to plain text if HTML parsing fails
            return html.unescape(html_content)

    def get_pagination_info(self, page):
        """Extract pagination information from Robert Half pagination structure"""
        try:
            # Look for the pagination container
            pagination_container = page.query_selector('.rhcl-pagination')
            if not pagination_container:
                logger.info("No pagination found - single page")
                return 1, 1, 0
            
            # Extract total jobs count from "1-25 of 202 jobs" text
            total_jobs = 0
            total_text_element = pagination_container.query_selector('.rhcl-pagination--numbered-label rhcl-typography')
            if total_text_element:
                total_text = total_text_element.inner_text().strip()
                # Parse "1-25 of 202 jobs" format
                import re
                total_match = re.search(r'of (\d+) jobs?', total_text)
                if total_match:
                    total_jobs = int(total_match.group(1))
                    logger.info(f"Found total jobs: {total_jobs}")
            
            # Calculate total pages (25 jobs per page)
            total_pages = (total_jobs + self.jobs_per_page - 1) // self.jobs_per_page if total_jobs > 0 else 1
            
            # Find current page
            current_page = 1
            current_page_element = pagination_container.query_selector('.rhcl-pagination--numbered-pagination--pagination--tile-selected rhcl-typography')
            if current_page_element:
                try:
                    current_page = int(current_page_element.inner_text())
                except ValueError:
                    current_page = 1
            
            logger.info(f"Pagination info: Current page {current_page}, Total pages: {total_pages}, Total jobs: {total_jobs}")
            return current_page, total_pages, total_jobs
            
        except Exception as e:
            logger.warning(f"Error detecting pagination: {e}")
            return 1, 1, 0

    def build_page_url(self, page_number):
        """Build URL for a specific page"""
        if page_number == 1:
            return self.jobs_url
        else:
            return f"{self.jobs_url}?pagenumber={page_number}"

    def navigate_to_page(self, page, page_number):
        """Navigate to a specific page"""
        try:
            page_url = self.build_page_url(page_number)
            logger.info(f"Navigating to page {page_number}: {page_url}")
            page.goto(page_url)
            self.human_delay(3, 5)
            
            # Wait for content to load
            page.wait_for_selector('rhcl-job-card, body', timeout=15000)
            
            # Verify we're on the correct page
            pagination_container = page.query_selector('.rhcl-pagination')
            if pagination_container:
                current_page_element = pagination_container.query_selector('.rhcl-pagination--numbered-pagination--pagination--tile-selected rhcl-typography')
                if current_page_element:
                    current_page_text = current_page_element.inner_text().strip()
                    if current_page_text == str(page_number):
                        logger.info(f"Successfully navigated to page {page_number}")
                        return True
                    else:
                        logger.warning(f"Expected page {page_number}, but found page {current_page_text}")
            
            return True  # Assume success if we can't verify
            
        except Exception as e:
            logger.error(f"Error navigating to page {page_number}: {e}")
            return False

    def parse_salary_info(self, salary_text):
        """Extract salary information from text"""
        if not salary_text:
            return None, None, 'yearly', ''
            
        salary_text = salary_text.strip()
        original_text = salary_text
        
        # Common salary patterns from Robert Half
        patterns = [
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*AUD\s*/\s*(hour|day|week|month|year)',  # 65 - 75 AUD / hour
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*AUD\s*per\s*(hour|day|week|month|year)',  # 65 - 75 AUD per hour
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*AUD\s*/\s*(annum)',  # 80000 - 100000 AUD / annum
            r'(\d{1,3}(?:,\d{3})*)\s*AUD\s*/\s*(hour|day|week|month|year)',  # 75 AUD / hour
            r'(\d{1,3}(?:,\d{3})*)\s*AUD\s*per\s*(hour|day|week|month|year)',  # 75 AUD per hour
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)',  # 80000 - 100000
        ]
        
        salary_min = None
        salary_max = None
        salary_type = 'yearly'
        
        for pattern in patterns:
            match = re.search(pattern, salary_text, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) >= 3:  # Range with period
                        salary_min = Decimal(groups[0].replace(',', ''))
                        salary_max = Decimal(groups[1].replace(',', ''))
                        period = groups[2].lower()
                        if period in ['hour', 'hourly']:
                            salary_type = 'hourly'
                        elif period in ['day', 'daily']:
                            salary_type = 'daily'
                        elif period in ['week', 'weekly']:
                            salary_type = 'weekly'
                        elif period in ['month', 'monthly']:
                            salary_type = 'monthly'
                        elif period in ['annum', 'year', 'yearly']:
                            salary_type = 'yearly'
                        break
                    elif len(groups) == 2 and groups[1] in ['hour', 'day', 'week', 'month', 'year', 'annum']:  # Single amount with period
                        salary_min = Decimal(groups[0].replace(',', ''))
                        period = groups[1].lower()
                        if period in ['hour', 'hourly']:
                            salary_type = 'hourly'
                        elif period in ['day', 'daily']:
                            salary_type = 'daily'
                        elif period in ['week', 'weekly']:
                            salary_type = 'weekly'
                        elif period in ['month', 'monthly']:
                            salary_type = 'monthly'
                        elif period in ['annum', 'year', 'yearly']:
                            salary_type = 'yearly'
                        break
                    elif len(groups) == 2:  # Range without explicit period
                        salary_min = Decimal(groups[0].replace(',', ''))
                        salary_max = Decimal(groups[1].replace(',', ''))
                        # Determine type based on amount ranges
                        if salary_min < 500:  # Likely hourly
                            salary_type = 'hourly'
                        elif salary_min < 5000:  # Likely weekly/monthly
                            salary_type = 'weekly'
                        else:  # Likely yearly
                            salary_type = 'yearly'
                        break
                except (ValueError, AttributeError):
                    continue
        
        return salary_min, salary_max, salary_type, original_text

    def parse_location(self, location_text):
        """Parse location string into normalized location data"""
        if not location_text:
            return 'Australia'
            
        location_text = location_text.strip()
        
        # Clean up location text
        location_text = re.sub(r'\s+', ' ', location_text)
        
        # Handle common Robert Half location formats
        # Examples: "Parramatta, New South Wales", "Melbourne, Victoria", "Melbourne CBD, Victoria"
        
        # Australian state abbreviations and full names
        aus_states = {
            'VIC': 'Victoria', 'NSW': 'New South Wales', 'QLD': 'Queensland',
            'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory', 'NT': 'Northern Territory'
        }
        
        # Extract state if present
        for abbr, full_name in aus_states.items():
            if abbr in location_text or full_name in location_text:
                # Return formatted location
                if ',' in location_text:
                    parts = [p.strip() for p in location_text.split(',')]
                    city = parts[0]
                    return f"{city}, {full_name}"
                else:
                    return f"{location_text}, {full_name}"
        
        # Return as-is if no state found
        return location_text

    def get_or_create_company(self, company_name, company_url=None):
        """Get or create company with proper handling"""
        if not company_name or company_name.strip() == '':
            company_name = 'Robert Half'
            
        company_name = company_name.strip()
        
        # Try to find existing company (case-insensitive)
        company = Company.objects.filter(name__iexact=company_name).first()
        
        if not company:
            company = Company.objects.create(
                name=company_name,
                website=company_url if company_url else 'https://www.roberthalf.com/au',
                description=f'Professional recruitment services company posting jobs via Robert Half Australia'
            )
            self.stats['companies_created'] += 1
            logger.info(f"Created new company: {company_name}")
            
        return company

    def get_or_create_location(self, location_text):
        """Get or create location with proper handling"""
        if not location_text or location_text.strip() == '':
            location_text = 'Australia'
            
        location_text = location_text.strip()
        
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
                    if ',' in location_text:
                        city = location_text.split(',')[0].strip()
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

    def extract_job_data(self, job_element):
        """Extract job data from Robert Half custom job card element"""
        try:
            job_data = {}
            
            # Extract data from rhcl-job-card attributes
            # All the data is stored as attributes in the custom web component
            job_data['title'] = job_element.get_attribute('headline') or ''
            job_data['url'] = job_element.get_attribute('destination') or ''
            job_data['location'] = job_element.get_attribute('location') or 'Australia'
            job_data['job_type'] = job_element.get_attribute('type') or 'full_time'
            job_data['work_mode'] = job_element.get_attribute('worksite') or ''
            job_data['description'] = job_element.get_attribute('copy') or ''
            job_data['job_id'] = job_element.get_attribute('job-id') or ''
            job_data['date_posted'] = job_element.get_attribute('date') or ''
            
            # Extract salary information from attributes
            salary_min_attr = job_element.get_attribute('salary-min')
            salary_max_attr = job_element.get_attribute('salary-max')
            salary_currency_attr = job_element.get_attribute('salary-currency') or 'AUD'
            salary_period_attr = job_element.get_attribute('salary-period') or 'yearly'
            
            # Process salary data
            if salary_min_attr:
                try:
                    job_data['salary_min'] = Decimal(salary_min_attr)
                except:
                    job_data['salary_min'] = None
            else:
                job_data['salary_min'] = None
                
            if salary_max_attr:
                try:
                    job_data['salary_max'] = Decimal(salary_max_attr)
                except:
                    job_data['salary_max'] = None
            else:
                job_data['salary_max'] = None
            
            # Map salary period to our database format
            period_mapping = {
                'hour': 'hourly',
                'day': 'daily', 
                'week': 'weekly',
                'month': 'monthly',
                'year': 'yearly',
                'annum': 'yearly'
            }
            job_data['salary_type'] = period_mapping.get(salary_period_attr, 'yearly')
            
            # Build salary raw text for display
            if job_data['salary_min'] and job_data['salary_max']:
                job_data['salary_raw_text'] = f"{job_data['salary_min']} - {job_data['salary_max']} {salary_currency_attr} / {salary_period_attr}"
            elif job_data['salary_min']:
                job_data['salary_raw_text'] = f"{job_data['salary_min']} {salary_currency_attr} / {salary_period_attr}"
            else:
                job_data['salary_raw_text'] = ''
            
            # Clean up and process job type
            job_type_raw = job_data['job_type'].lower() if job_data['job_type'] else ''
            if job_type_raw == 'project':
                job_data['job_type'] = 'contract'  # Projects are typically contract work
            elif job_type_raw == 'permanent placement':
                job_data['job_type'] = 'permanent'
            elif job_type_raw == 'contract/temporary talent':
                job_data['job_type'] = 'contract'
            elif 'permanent' in job_type_raw:
                job_data['job_type'] = 'permanent'
            elif 'contract' in job_type_raw or 'temporary' in job_type_raw:
                job_data['job_type'] = 'contract'
            elif 'part-time' in job_type_raw:
                job_data['job_type'] = 'part_time'
            else:
                job_data['job_type'] = 'full_time'  # Default
            
            # Clean up work mode
            worksite_raw = job_data['work_mode'].lower() if job_data['work_mode'] else ''
            if worksite_raw == 'onsite':
                job_data['work_mode'] = 'On-site'
            elif worksite_raw == 'remote':
                job_data['work_mode'] = 'Remote'
            elif worksite_raw == 'hybrid':
                job_data['work_mode'] = 'Hybrid'
            else:
                job_data['work_mode'] = ''
            
            # Process location - already clean from attribute
            job_data['location'] = self.parse_location(job_data['location'])
            
            # Process posted date
            if job_data['date_posted']:
                try:
                    # Parse ISO date: 2025-07-22T06:55:54Z
                    date_obj = datetime.fromisoformat(job_data['date_posted'].replace('Z', '+00:00'))
                    days_ago = (datetime.now(date_obj.tzinfo) - date_obj).days
                    if days_ago == 0:
                        job_data['posted_ago'] = 'Today'
                    elif days_ago == 1:
                        job_data['posted_ago'] = '1 day ago'
                    else:
                        job_data['posted_ago'] = f'{days_ago} days ago'
                except:
                    job_data['posted_ago'] = 'Recently'
            else:
                job_data['posted_ago'] = ''
            
            # Validate essential data
            if not job_data['title'] or len(job_data['title']) < 2:
                logger.warning("Job title too short or empty")
                return None
            
            if not job_data['url']:
                logger.warning("No job URL found")
                return None
            
            # Decode HTML entities in title and description
            job_data['title'] = html.unescape(job_data['title'])
            job_data['description'] = html.unescape(job_data['description'])
            
            # Truncate only necessary fields for database constraints
            job_data['title'] = job_data['title'][:200]  # Database constraint
            job_data['posted_ago'] = job_data['posted_ago'][:50]  # Database constraint
            job_data['salary_raw_text'] = job_data['salary_raw_text'][:200]  # Display purposes
            # Note: description is kept without length restrictions
            
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
            page.wait_for_selector('body', timeout=10000)
            
            job_details = {
                'description': 'Professional opportunity with Robert Half Australia.',
                'company': 'Robert Half',
                'experience_level': '',
                'additional_info': {}
            }
            
            # Extract full job description from Robert Half specific structure
            description_html = ''
            description_text = ''
            
            # Primary selector: Robert Half job description container
            description_selectors = [
                '[data-testid="job-details-description"]',  # Primary Robert Half selector
                'div[slot="description"]',                   # Alternative slot-based selector
                '.job-description',
                '.job-content',
                '.description',
                '.content',
                '.job-details'
            ]
            
            for selector in description_selectors:
                try:
                    desc_element = page.query_selector(selector)
                    if desc_element:
                        # Get the full HTML content
                        desc_html = desc_element.inner_html()
                        
                        # Also get text for skill extraction
                        desc_text = desc_element.inner_text().strip()
                        
                        # If we got substantial content, use it
                        if len(desc_text) > 100:
                            description_html = desc_html
                            description_text = desc_text
                            logger.info(f"Found description using selector: {selector} ({len(desc_text)} characters)")
                            break
                            
                except Exception as e:
                    logger.debug(f"Error with selector {selector}: {e}")
                    continue
            
            # Fallback: Extract from table structure (as seen in your HTML)
            if not description_text or len(description_text) < 200:
                try:
                    # Look for table-based content (common in Robert Half job descriptions)
                    table_element = page.query_selector('table td')
                    if table_element:
                        table_html = table_element.inner_html()
                        table_text = table_element.inner_text().strip()
                        if len(table_text) > 200:
                            description_html = table_html
                            description_text = table_text
                            logger.info(f"Found description in table structure ({len(table_text)} characters)")
                except Exception as e:
                    logger.debug(f"Error extracting from table: {e}")
            
            # Fallback: try to get main content
            if not description_text or len(description_text) < 200:
                try:
                    main_content = page.query_selector('main, .main, #main, .container, body')
                    if main_content:
                        # Get all substantial paragraphs and combine
                        paragraphs = main_content.query_selector_all('p, li, div, span, td, th')
                        content_parts_html = []
                        content_parts_text = []
                        for element in paragraphs:
                            text = element.inner_text().strip()
                            html_content = element.inner_html()
                            # Include any substantial content, be more aggressive
                            if len(text) > 15 and not any(skip in text.lower() for skip in ['cookie', 'privacy', 'footer', 'navigation', 'menu']):
                                content_parts_html.append(html_content)
                                content_parts_text.append(text)
                        
                        if content_parts_text:
                            description_html = '<br>'.join(content_parts_html)
                            description_text = '\n\n'.join(content_parts_text)
                            logger.info(f"Extracted description from main content ({len(description_text)} characters)")
                except Exception as e:
                    logger.warning(f"Error extracting from main content: {e}")
            
            # Final fallback: Get ALL text content from the page if we still don't have enough
            if not description_text or len(description_text) < 100:
                try:
                    body_text = page.inner_text('body')
                    if body_text and len(body_text) > 500:
                        # Clean and extract the relevant portion
                        lines = body_text.split('\n')
                        content_lines = []
                        for line in lines:
                            line = line.strip()
                            if (len(line) > 20 and 
                                not any(skip in line.lower() for skip in ['cookie', 'privacy', 'navigation', 'menu', 'footer', 'header']) and
                                any(keyword in line.lower() for keyword in ['experience', 'skill', 'require', 'responsi', 'role', 'position', 'job', 'work'])):
                                content_lines.append(line)
                        
                        if content_lines:
                            description_text = '\n\n'.join(content_lines[:20])  # Take first 20 relevant lines
                            description_html = '<p>' + '</p><p>'.join(content_lines[:20]) + '</p>'
                            logger.info(f"Extracted description from body text ({len(description_text)} characters)")
                except Exception as e:
                    logger.warning(f"Error extracting from body text: {e}")
            
            # Clean up the description and extract skills
            final_description_html = ''
            required_skills = []
            preferred_skills = []
            
            if description_text:
                # Clean the HTML content
                final_description_html = self.clean_html_description(description_html)
                
                # Remove email tracking references from text
                cleaned_text = re.sub(r'By clicking.*?at this time\.', '', description_text, flags=re.DOTALL)
                # Remove extra whitespace
                cleaned_text = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_text)
                cleaned_text = cleaned_text.strip()
                
                # Extract skills from the cleaned description
                required_skills, preferred_skills = self.extract_skills_from_description(
                    final_description_html, cleaned_text
                )
                
                # Store the cleaned HTML description
                job_details['description'] = final_description_html
                job_details['description_text'] = cleaned_text  # Also store text version for reference
                job_details['skills'] = required_skills
                job_details['preferred_skills'] = preferred_skills
                
                logger.info(f"Final description length: {len(final_description_html)} characters (HTML)")
                logger.info(f"Found {len(required_skills)} required skills: {', '.join(required_skills[:5])}{'...' if len(required_skills) > 5 else ''}")
                logger.info(f"Found {len(preferred_skills)} preferred skills: {', '.join(preferred_skills[:5])}{'...' if len(preferred_skills) > 5 else ''}")
            else:
                # Fallback if no description found
                job_details['description'] = '<p>Professional opportunity with Robert Half Australia. Visit the job URL for full details.</p>'
                job_details['description_text'] = 'Professional opportunity with Robert Half Australia. Visit the job URL for full details.'
                job_details['skills'] = []
                job_details['preferred_skills'] = []
            
            # Extract company name (might be the actual client company)
            company_selectors = [
                '.company-name',
                '.client-name', 
                '.company',
                '.employer',
                'h1, h2, h3'
            ]
            
            for selector in company_selectors:
                try:
                    company_element = page.query_selector(selector)
                    if company_element:
                        company_text = company_element.inner_text().strip()
                        if company_text and 'robert half' not in company_text.lower() and len(company_text) > 3:
                            job_details['company'] = company_text
                            logger.info(f"Found client company: {company_text}")
                            break
                except Exception:
                    continue
            
            # Extract experience level from job description
            if job_details.get('description_text'):
                desc_lower = job_details['description_text'].lower()
                if any(term in desc_lower for term in ['senior', 'sr.', 'lead', 'principal']):
                    job_details['experience_level'] = 'Senior'
                elif any(term in desc_lower for term in ['junior', 'jr.', 'graduate', 'entry']):
                    job_details['experience_level'] = 'Junior'
                elif any(term in desc_lower for term in ['manager', 'director', 'head of', 'vp', 'executive']):
                    job_details['experience_level'] = 'Executive'
                elif any(term in desc_lower for term in ['mid-level', 'intermediate', '3-5 years', '2-4 years']):
                    job_details['experience_level'] = 'Mid-level'
            
            return job_details
            
        except Exception as e:
            logger.warning(f"Could not get job details for {job_url}: {e}")
            return {
                'description': '<p>Professional opportunity with Robert Half Australia. Visit the job URL for full details.</p>',
                'description_text': 'Professional opportunity with Robert Half Australia. Visit the job URL for full details.',
                'company': 'Robert Half',
                'experience_level': '',
                'skills': [],
                'preferred_skills': [],
                'additional_info': {}
            }

    def categorize_job(self, title, description_text):
        """Categorize job using the categorization service"""
        try:
            category = self.categorization_service.categorize_job(title, description_text)
            
            # Additional Robert Half specific categorizations
            title_lower = title.lower()
            desc_lower = description_text.lower() if description_text else ''
            
            # Robert Half specializes in certain areas
            if any(term in title_lower for term in ['financial controller', 'finance', 'accounting', 'bookkeeper']):
                return 'finance'
            elif any(term in title_lower for term in ['devops', 'developer', 'programmer', 'software', 'it']):
                return 'technology'
            elif any(term in title_lower for term in ['executive assistant', 'admin', 'office', 'coordinator']):
                return 'office_support'
            elif any(term in title_lower for term in ['recruitment', 'hr', 'human resources']):
                return 'hr'
            elif any(term in title_lower for term in ['marketing', 'digital', 'communications']):
                return 'marketing'
            elif any(term in title_lower for term in ['legal', 'lawyer', 'solicitor']):
                return 'legal'
            elif any(term in title_lower for term in ['consultant', 'advisory', 'consulting']):
                return 'consulting'
            
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
                
                # Get detailed job information
                job_details = self.get_job_details(job_data['url'], page)
                
                # Get or create company
                company = self.get_or_create_company(job_details['company'])
                
                # Get or create location
                location = self.get_or_create_location(job_data['location'])
                
                # Categorize job
                full_description_text = job_details.get('description_text', '')
                category = self.categorize_job(job_data['title'], full_description_text)
                
                # Extract skills data
                required_skills = job_details.get('skills', [])
                preferred_skills = job_details.get('preferred_skills', [])
                
                # Convert skills lists to comma-separated strings (limited to 200 chars each)
                skills_str = ', '.join(required_skills)[:200] if required_skills else ''
                preferred_skills_str = ', '.join(preferred_skills)[:200] if preferred_skills else ''
                
                # Create job posting with HTML description and skills
                job_posting = JobPosting.objects.create(
                    title=job_data['title'][:200],  # Keep title limit for database constraint
                    description=job_details['description'],  # HTML formatted description
                    company=company,
                    location=location,
                    posted_by=self.default_user,
                    job_category=category,
                    job_type=job_data.get('job_type', 'full_time'),
                    experience_level=job_details.get('experience_level', '')[:100],  # Keep reasonable limit
                    work_mode=job_data.get('work_mode', '')[:50],  # Keep reasonable limit
                    salary_min=job_data.get('salary_min'),
                    salary_max=job_data.get('salary_max'),
                    salary_type=job_data.get('salary_type', 'yearly'),
                    salary_currency='AUD',
                    salary_raw_text=job_data.get('salary_raw_text', '')[:200],  # Keep for display purposes
                    external_source='roberthalf.com.au',
                    external_url=job_data['url'],  # No length restriction on URL
                    posted_ago=job_data.get('posted_ago', '')[:50],  # Keep reasonable limit
                    status='active',
                    skills=skills_str,  # Required skills (comma-separated)
                    preferred_skills=preferred_skills_str,  # Preferred skills (comma-separated)
                    additional_info={
                        'scrape_timestamp': datetime.now().isoformat(),
                        'source_page': 'Robert Half Australia',
                        'recruitment_agency': True,
                        'job_id': job_data.get('job_id', ''),
                        'original_copy': job_data.get('description', ''),  # Store original short description too
                        'description_text': job_details.get('description_text', ''),  # Store text version for reference
                        'extracted_skills_count': len(required_skills),
                        'extracted_preferred_skills_count': len(preferred_skills)
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
        """Main scraping method with pagination support"""
        logger.info("Starting Robert Half Australia job scraping...")
        if self.max_pages:
            logger.info(f"Max pages to scrape: {self.max_pages}")
        
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=self.user_agent,
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            try:
                # Navigate to first page to get pagination info
                logger.info(f"Navigating to: {self.jobs_url}")
                page.goto(self.jobs_url)
                self.human_delay(3, 5)
                
                # Wait for page to load
                page.wait_for_selector('body', timeout=15000)
                
                # Accept cookies if needed
                try:
                    cookie_button = page.query_selector('button:has-text("Accept"), button:has-text("Continue"), .cookie-accept')
                    if cookie_button:
                        cookie_button.click()
                        self.human_delay(1, 2)
                except Exception:
                    pass
                
                # Get pagination information
                current_page, total_pages, total_jobs = self.get_pagination_info(page)
                self.stats['total_pages_found'] = total_pages
                self.stats['total_jobs_available'] = total_jobs
                
                logger.info(f"Found {total_jobs} total jobs across {total_pages} pages")
                
                # Determine which pages to scrape
                pages_to_scrape = []
                if self.max_pages:
                    pages_to_scrape = list(range(1, min(self.max_pages + 1, total_pages + 1)))
                else:
                    # Calculate pages to scrape; if unlimited, scrape all pages
                    if self.max_jobs is None:
                        pages_to_scrape = list(range(1, total_pages + 1))
                    else:
                        pages_needed = (self.max_jobs + self.jobs_per_page - 1) // self.jobs_per_page
                        pages_to_scrape = list(range(1, min(pages_needed + 1, total_pages + 1)))
                
                logger.info(f"Will scrape {len(pages_to_scrape)} pages: {pages_to_scrape}")
                
                all_extracted_jobs = []
                
                # Scrape each page
                for page_num in pages_to_scrape:
                    try:
                        logger.info(f"=" * 60)
                        logger.info(f"SCRAPING PAGE {page_num} of {total_pages}")
                        logger.info(f"=" * 60)
                        
                        # Navigate to the page (skip navigation for page 1 as we're already there)
                        if page_num > 1:
                            success = self.navigate_to_page(page, page_num)
                            if not success:
                                logger.error(f"Failed to navigate to page {page_num}, skipping")
                                continue
                        
                        # Look for Robert Half custom job card elements
                        job_selectors = [
                            'rhcl-job-card',  # Primary: Robert Half custom job card component
                            '[data-testid*="job-card"]',  # Secondary: job cards with testid
                            '[data-testid*="job"]',  # Fallback: any job elements
                        ]
                        
                        job_elements = []
                        for selector in job_selectors:
                            elements = page.query_selector_all(selector)
                            if elements:
                                job_elements = elements
                                logger.info(f"Found {len(elements)} jobs using selector: {selector}")
                                break
                        
                        if not job_elements:
                            logger.warning(f"No job listings found on page {page_num}")
                            continue
                        
                        # Extract job data from this page
                        page_jobs = []
                        logger.info(f"Extracting data from {len(job_elements)} jobs on page {page_num}...")
                        
                        for i, job_element in enumerate(job_elements, 1):
                            try:
                                logger.info(f"Extracting job {i}/{len(job_elements)} from page {page_num}")
                                job_data = self.extract_job_data(job_element)
                                if job_data:
                                    job_data['page_number'] = page_num  # Track which page this came from
                                    page_jobs.append(job_data)
                                    logger.info(f"Extracted: {job_data['title']}")
                                    logger.info(f"  Location: {job_data['location']}")
                                    logger.info(f"  Salary: {job_data.get('salary_raw_text', 'Not specified')}")
                                else:
                                    logger.warning(f"Could not extract data for job {i} on page {page_num}")
                                
                                # Small delay between extractions
                                self.human_delay(0.5, 1)
                                
                            except Exception as e:
                                logger.error(f"Error extracting job {i} on page {page_num}: {e}")
                                self.stats['errors'] += 1
                                continue
                        
                        logger.info(f"Successfully extracted {len(page_jobs)} jobs from page {page_num}")
                        all_extracted_jobs.extend(page_jobs)
                        self.stats['pages_scraped'] += 1
                        
                        # Check if we've reached the job limit
                        if self.max_jobs is not None and len(all_extracted_jobs) >= self.max_jobs:
                            logger.info(f"Reached job limit of {self.max_jobs}, stopping pagination")
                            all_extracted_jobs = all_extracted_jobs[:self.max_jobs]
                            break
                        
                        # Add delay between pages
                        if page_num < pages_to_scrape[-1]:  # Don't delay after the last page
                            self.human_delay(2, 4)
                            
                    except Exception as e:
                        logger.error(f"Error scraping page {page_num}: {e}")
                        self.stats['errors'] += 1
                        continue
                
                logger.info(f"=" * 60)
                logger.info(f"PROCESSING ALL COLLECTED JOBS")
                logger.info(f"=" * 60)
                logger.info(f"Total jobs extracted from all pages: {len(all_extracted_jobs)}")
                
                # Now process and save each extracted job
                for i, job_data in enumerate(all_extracted_jobs, 1):
                    try:
                        page_num = job_data.get('page_number', 'Unknown')
                        logger.info(f"Processing job {i}/{len(all_extracted_jobs)} (from page {page_num}): {job_data['title']}")
                        self.stats['total_processed'] += 1
                        
                        success = self.save_job(job_data, page)
                        if success:
                            logger.info(f"Successfully saved: {job_data['title']}")
                        
                        # Add delay between job processing
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
        logger.info(f"Total jobs available: {self.stats['total_jobs_available']}")
        logger.info(f"Total pages found: {self.stats['total_pages_found']}")
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Total jobs processed: {self.stats['total_processed']}")
        logger.info(f"New jobs saved: {self.stats['new_jobs']}")
        logger.info(f"Duplicate jobs skipped: {self.stats['duplicate_jobs']}")
        logger.info(f"Companies created: {self.stats['companies_created']}")
        logger.info(f"Locations created: {self.stats['locations_created']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    """Main function with pagination support"""
    max_jobs = None  # Default unlimited
    max_pages = None  # Default: auto-calculate based on max_jobs
    
    # Parse command line arguments
    # Usage: python script.py [max_jobs] [max_pages]
    # Examples:
    #   python script.py 50        - Scrape max 50 jobs (auto-calculate pages)
    #   python script.py 50 3      - Scrape max 50 jobs from first 3 pages  
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
    scraper = RobertHalfAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
    scraper.scrape_jobs()


def run(max_jobs=None, max_pages=None):
    """Automation entrypoint for Robert Half Australia scraper."""
    try:
        scraper = RobertHalfAustraliaScraper(max_jobs=max_jobs, headless=True, max_pages=max_pages)
        scraper.scrape_jobs()
        return {
            'success': True,
            'message': 'Robert Half scraping completed'
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
