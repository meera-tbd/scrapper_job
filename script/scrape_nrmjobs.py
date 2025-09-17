#!/usr/bin/env python
"""
NRMjobs.com.au scraper rewritten to Playwright.

- Collects job links from the NRMjobs search page
  `https://nrmjobs.com.au/jobs/search-jobs` (paginates via » when available)
- Opens each detail page and extracts: title, advertiser (company), location,
  description, salary, job type, and category
- Saves to Django `JobPosting` using the existing schema and helpers similar to
  `script/scrape_hays.py`

Usage:
  python script/scrape_nrmjobs.py [max_jobs]
"""

import os
import re
import sys
import time
import random
from typing import Optional, Union
import logging
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# Django setup (same convention as other Playwright scrapers)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

try:
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    project_root = os.getcwd()

sys.path.append(project_root)

import django

django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_nrmjobs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class NRMJobsScraper:
    def __init__(self, max_jobs: Optional[int] = None, headless: bool = True):
        self.base_url = "https://nrmjobs.com.au"
        self.search_url = f"{self.base_url}/jobs/search-jobs"
        self.max_jobs = max_jobs
        self.headless = headless
        self.scraper_user: Optional[User] = None
        self.scraped_count = 0

    def human_like_delay(self, a: float = 0.7, b: float = 1.8) -> None:
        time.sleep(random.uniform(a, b))

    def setup_user(self) -> None:
        self.scraper_user, _ = User.objects.get_or_create(
            username='nrmjobs_scraper',
            defaults={
                'email': 'scraper@nrmjobs.local',
                'first_name': 'NRMjobs',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_company(self, name: Optional[str], logo_url: str = '') -> Company:
        cname = (name or "NRM Advertiser").strip() or "NRM Advertiser"
        
        defaults = {
            'description': 'Advertiser published via NRMjobs',
            'website': self.base_url,
            'company_size': 'medium',
            'logo': logo_url or ''
        }
        
        company, created = Company.objects.get_or_create(
            name=cname,
            defaults=defaults
        )
        
        # Update logo if we have a new one and the company doesn't have one
        if logo_url and not company.logo:
            company.logo = logo_url
            company.save()
            logger.info(f"✅ Updated company logo for {cname}: {logo_url}")
            
        return company

    def get_or_create_location(self, text: Optional[str]) -> Optional[Location]:
        if not text:
            return None
        raw = text.strip()
        if not raw:
            return None
        # Expand state abbreviations
        abbrev = {
            'NSW': 'New South Wales',
            'VIC': 'Victoria',
            'QLD': 'Queensland',
            'SA': 'South Australia',
            'WA': 'Western Australia',
            'TAS': 'Tasmania',
            'NT': 'Northern Territory',
            'ACT': 'Australian Capital Territory',
        }
        for k, v in abbrev.items():
            raw = re.sub(rf'\b{k}\b', v, raw, flags=re.IGNORECASE)
        raw = re.sub(r'\s*,?\s*Australia\s*$', '', raw, flags=re.IGNORECASE).strip()
        city = ''
        state = ''
        if ',' in raw:
            parts = [p.strip() for p in raw.split(',')]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        elif ' - ' in raw:
            parts = [p.strip() for p in raw.split(' - ', 1)]
            if len(parts) == 2:
                state, city = parts[0], parts[1]
        else:
            state = raw
        name = f"{city}, {state}" if city and state else (state or city)
        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={'city': city, 'state': state, 'country': 'Australia'}
        )
        return location

    def normalize_job_type(self, text: Optional[str]) -> str:
        if not text:
            return 'full_time'
        t = text.lower()
        if 'casual' in t:
            return 'casual'
        if 'part' in t:
            return 'part_time'
        if 'contract' in t or 'fixed term' in t:
            return 'contract'
        if 'temp' in t or 'temporary' in t:
            return 'temporary'
        if 'intern' in t:
            return 'internship'
        if 'free' in t:
            return 'freelance'
        return 'full_time'

    def parse_salary(self, raw: Optional[str]) -> dict:
        result = {
            'salary_min': None,
            'salary_max': None,
            'salary_type': 'yearly',
            'salary_currency': 'AUD',
            'salary_raw_text': raw or ''
        }
        if not raw:
            return result
        text = raw.strip()
        if re.search(r'hour', text, re.IGNORECASE):
            result['salary_type'] = 'hourly'
        elif re.search(r'week', text, re.IGNORECASE):
            result['salary_type'] = 'weekly'
        elif re.search(r'month', text, re.IGNORECASE):
            result['salary_type'] = 'monthly'
        elif re.search(r'year|annum|pa|p\.a\.', text, re.IGNORECASE):
            result['salary_type'] = 'yearly'
        nums = re.findall(r'\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)', text)
        values = []
        for n in nums:
            try:
                values.append(float(n.replace(',', '')))
            except Exception:
                continue
        if values:
            non_zero = [v for v in values if v > 0]
            if len(non_zero) >= 2:
                result['salary_min'] = min(non_zero)
                result['salary_max'] = max(non_zero)
            elif len(non_zero) == 1:
                result['salary_min'] = non_zero[0]
                result['salary_max'] = non_zero[0]
        return result

    def ensure_category_choice(self, display_text: str) -> str:
        if not display_text:
            return 'other'
        key = slugify(display_text).replace('-', '_')[:50] or 'other'
        if not any(choice[0] == key for choice in JobPosting.JOB_CATEGORY_CHOICES):
            JobPosting.JOB_CATEGORY_CHOICES.append((key, display_text.strip()))
        return key

    def sanitize_for_model(self, data: dict) -> dict:
        safe = dict(data)
        try:
            parsed = urlparse(safe.get('external_url') or '')
            path = parsed.path or ''
            canon = f"{parsed.scheme or 'https'}://{parsed.netloc}{path}"
            safe['external_url'] = canon[:200]
        except Exception:
            if safe.get('external_url'):
                safe['external_url'] = str(safe['external_url'])[:200]
        if safe.get('title'):
            safe['title'] = safe['title'][:200]
        if safe.get('salary_raw_text') is not None:
            safe['salary_raw_text'] = safe['salary_raw_text'][:200]
        if safe.get('external_id'):
            safe['external_id'] = safe['external_id'][:100]
        if safe.get('posted_ago'):
            safe['posted_ago'] = safe['posted_ago'][:50]
        if safe.get('work_mode'):
            safe['work_mode'] = safe['work_mode'][:50]
        if safe.get('job_category'):
            safe['job_category'] = safe['job_category'][:50]
        if safe.get('job_type'):
            safe['job_type'] = safe['job_type'][:20]
        if safe.get('salary_currency'):
            safe['salary_currency'] = safe['salary_currency'][:3]
        if safe.get('skills'):
            safe['skills'] = safe['skills'][:200]
        if safe.get('preferred_skills'):
            safe['preferred_skills'] = safe['preferred_skills'][:200]
        if safe.get('job_closing_date'):
            safe['job_closing_date'] = safe['job_closing_date'][:100]
        if safe.get('company_logo'):
            safe['company_logo'] = safe['company_logo'][:500]  # URL length limit
        return safe

    def extract_job_links(self, page) -> list[str]:
        links: set[str] = set()
        try:
            # Load the first page
            page.goto(self.search_url, wait_until="domcontentloaded", timeout=35000)
            self.human_like_delay(1.0, 1.8)

            # Discover max page number from pagination controls
            max_pages = 1
            try:
                hrefs = page.eval_on_selector_all(
                    'a[href*="search-jobs?page="]',
                    'els => els.map(a => a.getAttribute("href"))'
                )
            except Exception:
                hrefs = []
            for href in hrefs or []:
                m = re.search(r'[?&]page=(\d+)', href)
                if m:
                    max_pages = max(max_pages, int(m.group(1)))
            # Also inspect numeric labels in the pager
            try:
                labels = page.eval_on_selector_all(
                    'a, span', 'els => els.map(e => (e.innerText||"").trim())'
                )
                for lbl in labels or []:
                    if lbl.isdigit():
                        max_pages = max(max_pages, int(lbl))
            except Exception:
                pass

            # Visit each page and collect job detail links
            for page_num in range(1, max_pages + 1):
                url = self.search_url if page_num == 1 else f"{self.search_url}?page={page_num}"
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=35000)
                except Exception:
                    page.goto(url, wait_until="load", timeout=60000)
                self.human_like_delay(0.9, 1.6)

                anchors = page.query_selector_all('a[href]')
                for a in anchors:
                    href = a.get_attribute('href') or ''
                    low = href.lower()
                    if not href or low.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    # Typical detail links like /jobs/2025/20026672/slug
                    if re.search(r"/jobs/20[0-9]{2}/[0-9]{5,}/", low):
                        full = href if low.startswith('http') else urljoin(self.base_url, href)
                        links.add(full)
        except Exception as e:
            logger.warning(f"Link extraction warning: {e}")
        return list(sorted(links))

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
        lines = [ln.strip() for ln in text.split('\n')]
        cleaned = []
        drop_prefixes = ['Return to your search results', 'Share', 'Apply', 'Login']
        for ln in lines:
            if not ln:
                continue
            if any(ln.startswith(p) for p in drop_prefixes):
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def extract_description_from_nrm_body(self, body_text: str) -> str:
        """Extract only the job description section from an NRMjobs advert page body.

        Strategy:
        - Start right after the last header label (preferably the line starting with 'Ref:').
        - End before any of the known footer/application sections (How to apply, Closing date, Enquiries, Date published, Noticeboard, Copyright, etc.).
        - Clean out navigation crumbs and short UI-only lines.
        """
        if not body_text:
            return ''
        text = body_text.replace('\r', '\n')
        text = re.sub(r'\n{2,}', '\n\n', text)

        # Determine start
        start_idx = -1
        start_patterns = [
            r"Ref:\s*.+?$",  # prefer to start after Ref line
            r"Salary\s*etc:\s*.+?$",
            r"Location:\s*.+?$",
            r"In this role[,:]",
            r"This role[’'`]?s focus",
            r"About the role",
            r"Role overview",
        ]
        for pat in start_patterns:
            m = re.search(pat, text, flags=re.IGNORECASE | re.MULTILINE)
            if m:
                start_idx = m.end()
                break
        if start_idx == -1:
            # Fallback: first occurrence of a substantial paragraph
            m = re.search(r"\n\n(.{120,}?)\n", text, flags=re.DOTALL)
            if m:
                start_idx = m.start(1)
            else:
                start_idx = 0

        # Determine end
        end_idx = len(text)
        end_patterns = [
            r"\nHow to apply[:]?",
            r"\nClosing date[:]?",
            r"\nEnquiries[:]?",
            r"\nDate published[:]?",
            r"\nNoticeboard",
            r"\nJob Categories",
            r"\nNRMjobs[:]?",
            r"\nAdvertisers[:]?",
            r"\nPortal[:]?",
            r"\nCopyright",
            r"\nAPPLY NOW",
        ]
        for pat in end_patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m and m.start() > start_idx:
                end_idx = min(end_idx, m.start())
        chunk = text[start_idx:end_idx].strip()

        # Remove repeated nav crumbs and very short UI lines
        lines = [ln.strip() for ln in chunk.split('\n')]
        drop_exact = {
            'Home', 'Bulletin', 'Search jobs', 'Pay an invoice', 'Advertisers', 'About NRMjobs', 'Register',
            'Tweet', 'APPLY NOW', 'Noticeboard', 'Job Categories', 'Jobs:', 'Notices:', 'NRMjobs:', 'Advertisers:',
            'Portal:', 'Pricing', 'Payments', 'Place advert', 'Contact us', 'About us', 'Why choose us',
            'General', 'Events', 'Courses', 'Services', 'Quiz answers', 'Slavery', 'Privacy'
        }
        cleaned = []
        for ln in lines:
            if not ln:
                continue
            if ln in drop_exact:
                continue
            if len(ln) <= 2 and ln not in {'-','•','–'}:
                continue
            cleaned.append(ln)
        result = '\n'.join(cleaned)
        # Final tidy
        result = re.sub(r'\n{3,}', '\n\n', result).strip()
        return result
    
    def clean_description_html(self, html_content: str) -> str:
        """
        Clean HTML content while preserving HTML structure for database storage.
        This maintains HTML formatting for proper display in the admin interface.
        """
        if not html_content:
            return html_content
            
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove unwanted tags but keep content
            for tag in soup.find_all(['script', 'style', 'meta', 'link', 'noscript']):
                tag.decompose()
            
            # Handle images - keep them as HTML but clean attributes
            for img in soup.find_all('img'):
                # Keep only src and alt attributes
                new_attrs = {}
                if img.get('src'):
                    new_attrs['src'] = img.get('src')
                if img.get('alt'):
                    new_attrs['alt'] = img.get('alt')
                img.attrs = new_attrs
            
            # Clean up attributes but preserve essential HTML structure
            for tag in soup.find_all():
                if tag.name == 'a':
                    # Keep href for links
                    new_attrs = {}
                    if tag.get('href'):
                        new_attrs['href'] = tag.get('href')
                    tag.attrs = new_attrs
                elif tag.name in ['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'strong', 'em', 'b', 'i']:
                    # Remove all attributes from structural tags but keep the tags
                    tag.attrs = {}
            
            # Normalize headings to h3 for consistency
            for h in soup.find_all(['h1', 'h2', 'h4', 'h5', 'h6']):
                h.name = 'h3'
            
            # Convert some divs and spans to paragraphs if they contain significant text
            for tag in soup.find_all(['div', 'span']):
                text_content = tag.get_text().strip()
                if text_content and len(text_content) > 20:
                    # Only convert if it's not already containing block elements
                    if not tag.find(['p', 'div', 'ul', 'ol', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                        tag.name = 'p'
                elif not text_content:
                    # Remove empty divs/spans
                    tag.unwrap()
            
            # Remove truly empty elements but keep structural ones
            for elem in soup.find_all():
                if not elem.get_text().strip() and elem.name not in ['br', 'hr', 'img'] and not elem.find_all():
                    elem.decompose()
            
            # Get the cleaned HTML string
            cleaned_html = str(soup)
            
            # Remove excessive whitespace while preserving HTML
            cleaned_html = re.sub(r'\n\s*\n', '\n', cleaned_html)
            cleaned_html = re.sub(r'>\s+<', '><', cleaned_html)
            
            # Ensure we return valid HTML - if we lost all HTML structure, wrap in div
            if not re.search(r'<[^>]+>', cleaned_html):
                cleaned_html = f'<div>{cleaned_html}</div>'
            
            return cleaned_html.strip()
            
        except Exception as e:
            logger.warning(f"HTML cleaning failed: {e}")
            # Return original HTML content as fallback
            return html_content
    
    def extract_skills_from_description(self, description: str, title: str = '') -> tuple[str, str]:
        """
        Extract skills and preferred skills from job description.
        Based on patterns from other scrapers like scrape_hays.py, scrape_coles.py etc.
        """
        # Comprehensive skills list
        technical_skills = [
            'python', 'java', 'javascript', 'react', 'node.js', 'sql', 'mysql', 'postgresql',
            'mongodb', 'redis', 'aws', 'azure', 'docker', 'kubernetes', 'git', 'linux',
            'excel', 'powerpoint', 'word', 'office', 'sharepoint', 'salesforce', 'sap',
            'tableau', 'power bi', 'analytics', 'data analysis', 'project management',
            'agile', 'scrum', 'jira', 'confluence', 'adobe', 'photoshop', 'illustrator',
            'autocad', 'solidworks', 'matlab', 'r programming', 'machine learning',
            'artificial intelligence', 'cybersecurity', 'network security', 'cisco',
            'vmware', 'active directory', 'windows server', 'unix', 'shell scripting'
        ]
        
        business_skills = [
            'budgeting', 'financial analysis', 'accounting', 'bookkeeping', 'payroll',
            'invoicing', 'cost control', 'roi analysis', 'strategic planning',
            'business development', 'market research', 'competitive analysis',
            'stakeholder management', 'vendor management', 'contract negotiation',
            'procurement', 'supply chain', 'inventory management', 'quality assurance',
            'process improvement', 'lean six sigma', 'change management',
            'performance management', 'talent acquisition', 'employee relations'
        ]
        
        soft_skills = [
            'communication', 'teamwork', 'leadership', 'problem solving', 'critical thinking',
            'analytical thinking', 'attention to detail', 'time management', 'multitasking',
            'adaptability', 'flexibility', 'creativity', 'innovation', 'customer service',
            'interpersonal skills', 'presentation skills', 'public speaking', 'writing',
            'research', 'organization', 'planning', 'prioritization', 'decision making',
            'conflict resolution', 'negotiation', 'mentoring', 'coaching', 'collaboration'
        ]
        
        # NRM/Environmental specific skills
        nrm_skills = [
            'environmental management', 'natural resource management', 'conservation',
            'biodiversity', 'ecology', 'environmental assessment', 'gis', 'arcgis',
            'qgis', 'remote sensing', 'spatial analysis', 'field work', 'data collection',
            'research methodology', 'statistical analysis', 'environmental monitoring',
            'compliance', 'environmental law', 'policy development', 'grant writing',
            'community engagement', 'stakeholder consultation', 'project coordination',
            'land management', 'water management', 'forestry', 'agriculture',
            'sustainability', 'climate change', 'carbon management', 'environmental science',
            'botany', 'zoology', 'marine science', 'fisheries', 'wildlife management'
        ]
        
        all_skills = technical_skills + business_skills + soft_skills + nrm_skills
        
        found_skills = []
        preferred_found = []
        
        # Split description into lines for section analysis
        lines = description.split('\n')
        
        preferred_section = False
        essential_section = False
        
        for line in lines:
            line_lower = line.lower()
            
            # Check for section headers
            if any(word in line_lower for word in ['preferred', 'desirable', 'nice to have', 'bonus', 'advantageous', 'ideal']):
                preferred_section = True
                essential_section = False
                continue
            elif any(word in line_lower for word in ['essential', 'required', 'must have', 'mandatory', 'key skills', 'core skills', 'qualifications']):
                essential_section = True
                preferred_section = False
                continue
            elif line.strip() == '':
                preferred_section = False
                essential_section = False
                continue
            
            # Extract skills from current line
            line_text = line.lower()
            for skill in all_skills:
                if skill in line_text:
                    if preferred_section and skill.title() not in preferred_found:
                        preferred_found.append(skill.title())
                    elif essential_section and skill.title() not in found_skills:
                        found_skills.append(skill.title())
                    elif not preferred_section and not essential_section and skill.title() not in found_skills:
                        found_skills.append(skill.title())
        
        # Remove duplicates and limit results
        found_skills = list(dict.fromkeys(found_skills))[:15]
        preferred_found = list(dict.fromkeys(preferred_found))[:15]
        
        # If no preferred skills found, use some essential skills as preferred
        if not preferred_found and found_skills:
            # Split skills - put later ones in preferred
            split_point = len(found_skills) // 2 if len(found_skills) > 4 else len(found_skills) - 2
            if split_point > 0:
                preferred_found = found_skills[split_point:]
                found_skills = found_skills[:split_point]
        
        # If no skills found at all, provide intelligent defaults based on NRM context
        if not found_skills:
            if any(term in description.lower() for term in ['environmental', 'ecology', 'conservation', 'natural resource']):
                found_skills = ['Environmental Management', 'GIS', 'Field Work', 'Data Analysis', 'Report Writing']
                preferred_found = ['Environmental Science', 'Project Management', 'Stakeholder Engagement']
            else:
                found_skills = ['Communication', 'Research', 'Analysis', 'Report Writing', 'Project Management']
                preferred_found = ['Field Experience', 'Environmental Knowledge', 'Technical Skills']
        
        # Convert to comma-separated strings with length limits
        skills_str = ', '.join(found_skills)[:200]
        preferred_str = ', '.join(preferred_found)[:200]
        
        return skills_str, preferred_str
    
    def extract_closing_date(self, page_content: str) -> str:
        """
        Extract job closing date from page content.
        Based on patterns from nsw_government_scraper_advanced.py
        """
        closing_date = ''
        
        # Common patterns for closing dates
        patterns = [
            r'closing date[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
            r'applications close[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
            r'deadline[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
            r'([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4}).*close',
            r'close[\s\w]*[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, page_content, re.IGNORECASE)
            if match:
                closing_date = match.group(1).strip()
                break
        
        return closing_date
    
    def extract_posted_date(self, page_content: str) -> str:
        """
        Extract job posted date from page content.
        Based on patterns from healthjobs and chandlermacleod scrapers.
        """
        posted_date = ''
        
        # Common patterns for posted dates
        patterns = [
            r'date published[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
            r'posted[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
            r'published[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
            r'job posting[:\s]*([\d]{1,2}[\s/.-][\d]{1,2}[\s/.-][\d]{2,4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, page_content, re.IGNORECASE)
            if match:
                posted_date = match.group(1).strip()
                break
        
        return posted_date
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse date string into datetime object.
        Handles various date formats commonly found in job postings.
        """
        if not date_str:
            return None
            
        # Clean the date string
        date_str = re.sub(r'[^\d/.-]', ' ', date_str).strip()
        
        # Common date formats
        formats = [
            '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
            '%d/%m/%y', '%d-%m-%y', '%d.%m.%y',
            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
            '%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def extract_company_logo(self, page) -> str:
        """
        Extract company logo from the job page.
        Looks for company logos in various common locations.
        """
        logo_url = ''
        
        # Common selectors for company logos
        logo_selectors = [
            'img[class*="logo"]',
            'img[class*="company-logo"]', 
            'img[class*="employer-logo"]',
            'img[class*="advertiser-logo"]',
            '.logo img',
            '.company-logo img',
            '.employer-logo img',
            '.company-header img',
            '.job-header img',
            'header img[src*="logo"]',
            'img[alt*="logo" i]',
            'img[alt*="company" i]'
        ]
        
        for selector in logo_selectors:
            try:
                logo_element = page.query_selector(selector)
                if logo_element:
                    src = logo_element.get_attribute('src')
                    if src:
                        # Convert relative URLs to absolute URLs
                        if src.startswith('//'):
                            logo_url = f"https:{src}"
                        elif src.startswith('/'):
                            logo_url = f"https://nrmjobs.com.au{src}"
                        elif not src.startswith('http'):
                            logo_url = f"https://nrmjobs.com.au/{src}"
                        else:
                            logo_url = src
                        
                        # Validate it's actually an image
                        if any(ext in logo_url.lower() for ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp']):
                            logger.info(f"✅ Found company logo: {logo_url}")
                            return logo_url
            except Exception as e:
                logger.warning(f"Error extracting logo with selector {selector}: {e}")
                continue
        
        return logo_url
    
    def is_job_content(self, text: str) -> bool:
        """
        Check if the extracted text appears to be actual job content 
        and not navigation, search, or other page elements.
        """
        if not text or len(text.strip()) < 100:
            return False
        
        # Exclude content that looks like navigation or page elements
        exclude_keywords = [
            'search jobs', 'find jobs', 'job search', 'register', 'login',
            'home', 'about us', 'contact', 'privacy policy', 'terms',
            'copyright', 'all rights reserved', 'navigation', 'menu',
            'breadcrumb', 'footer', 'header', 'sidebar'
        ]
        
        text_lower = text.lower()
        for keyword in exclude_keywords:
            if keyword in text_lower and len(text.strip()) < 500:
                return False
        
        # Look for job-related content indicators
        job_indicators = [
            'position', 'role', 'responsibilities', 'requirements', 
            'qualifications', 'experience', 'skills', 'duties',
            'about the role', 'job description', 'key responsibilities',
            'what you will do', 'about you', 'salary', 'location',
            'organisation', 'opportunity', 'apply', 'application'
        ]
        
        indicator_count = sum(1 for indicator in job_indicators if indicator in text_lower)
        
        # If we have multiple job indicators, it's likely job content
        return indicator_count >= 3
    
    def clean_job_description_html(self, html_content: str) -> str:
        """
        Clean job description HTML while preserving structure.
        More aggressive cleaning specifically for job content.
        """
        if not html_content:
            return html_content
            
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove unwanted elements completely
            unwanted_selectors = [
                'script', 'style', 'meta', 'link', 'noscript',
                # Navigation and page elements
                'nav', 'header', 'footer', '.navigation', '.nav',
                '.breadcrumb', '.sidebar', '.menu', '.search',
                # Social sharing and ads
                '.social', '.share', '.ad', '.advertisement', '.promo',
                # Common unwanted classes
                '[class*="nav"]', '[class*="menu"]', '[class*="header"]',
                '[class*="footer"]', '[class*="sidebar"]', '[class*="search"]'
            ]
            
            for selector in unwanted_selectors:
                for element in soup.select(selector):
                    element.decompose()
            
            # Remove elements with unwanted text content
            unwanted_text = [
                'search jobs', 'find jobs', 'register', 'login', 'home',
                'privacy policy', 'terms of service', 'copyright',
                'all rights reserved', 'back to search', 'return to search'
            ]
            
            for element in soup.find_all(text=True):
                if any(unwanted in element.lower() for unwanted in unwanted_text):
                    if element.parent:
                        element.parent.decompose()
            
            # Clean and normalize HTML structure
            for img in soup.find_all('img'):
                # Keep images but clean attributes
                new_attrs = {}
                if img.get('src'):
                    src = img.get('src')
                    if src.startswith('//'):
                        new_attrs['src'] = f"https:{src}"
                    elif src.startswith('/'):
                        new_attrs['src'] = f"https://nrmjobs.com.au{src}"
                    elif not src.startswith('http'):
                        new_attrs['src'] = f"https://nrmjobs.com.au/{src}"
                    else:
                        new_attrs['src'] = src
                if img.get('alt'):
                    new_attrs['alt'] = img.get('alt')
                img.attrs = new_attrs
            
            # Clean links
            for a in soup.find_all('a'):
                if a.get('href'):
                    href = a.get('href')
                    if href.startswith('/') and not href.startswith('//'):
                        a['href'] = f"https://nrmjobs.com.au{href}"
                # Remove other attributes
                new_attrs = {'href': a.get('href')} if a.get('href') else {}
                a.attrs = new_attrs
            
            # Clean structural elements
            for tag in soup.find_all():
                if tag.name in ['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'strong', 'em', 'b', 'i']:
                    tag.attrs = {}
            
            # Normalize headings
            for h in soup.find_all(['h1', 'h2', 'h4', 'h5', 'h6']):
                h.name = 'h3'
            
            # Remove empty elements
            for elem in soup.find_all():
                if not elem.get_text().strip() and elem.name not in ['br', 'hr', 'img'] and not elem.find_all():
                    elem.decompose()
            
            # Get the final HTML
            cleaned_html = str(soup)
            
            # Final cleanup
            cleaned_html = re.sub(r'\n\s*\n', '\n', cleaned_html)
            cleaned_html = re.sub(r'>\s+<', '><', cleaned_html)
            
            # Ensure valid HTML structure
            if not re.search(r'<[^>]+>', cleaned_html):
                cleaned_html = f'<div>{cleaned_html}</div>'
            
            return cleaned_html.strip()
            
        except Exception as e:
            logger.warning(f"Job HTML cleaning failed: {e}")
            return html_content

    def extract_job_from_detail(self, page, job_url: str) -> Optional[dict]:
        try:
            try:
                page.goto(job_url, wait_until="domcontentloaded", timeout=40000)
            except Exception:
                page.goto(job_url, wait_until="load", timeout=60000)
            self.human_like_delay(0.9, 1.7)

            body_text = ''
            try:
                body_text = page.inner_text('body')
            except Exception:
                pass

            def extract_labeled(label: str) -> str:
                pat = rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)"
                m = re.search(pat, body_text, re.IGNORECASE)
                if m:
                    return re.sub(r'\s+', ' ', m.group(1).strip())
                m = re.search(rf"{re.escape(label)}\s*:\s*(.+?)\s*(?:\n|$)", body_text, re.IGNORECASE)
                if m:
                    return re.sub(r'\s+', ' ', m.group(1).strip())
                return ''

            title = extract_labeled('Title')
            advertiser = extract_labeled('Advertiser') or extract_labeled('Employer') or extract_labeled('Company')
            location_text = extract_labeled('Location')
            salary_labeled = extract_labeled('Salary etc') or extract_labeled('Salary')

            if not title:
                for sel in ['h1', 'h2']:
                    try:
                        el = page.query_selector(sel)
                    except Exception:
                        el = None
                    if el:
                        t = (el.inner_text() or '').strip()
                        if t:
                            title = t
                            break

            # Extract HTML description for better formatting
            description_html = ''
            description_text = ''
            final_description = ''
            
            # Extract company logo first
            company_logo_url = self.extract_company_logo(page)
            
            # Target specific job content areas only (not navigation, search, etc.)
            job_content_selectors = [
                # NRM-specific job content areas
                'div[class*="job-content"]',
                'div[class*="job-description"]', 
                'div[class*="job-detail"]',
                'div[class*="position-detail"]',
                'article[class*="job"]',
                'section[class*="job"]',
                # Generic content areas but be more specific
                'main .content',
                'main article',
                '.main-content',
                '#main-content',
                '.job-posting-content',
                '.job-details-container'
            ]
            
            # Try to get HTML content from job-specific areas only
            for sel in job_content_selectors:
                try:
                    el = page.query_selector(sel)
                    if el:
                        html_content = (el.inner_html() or '').strip()
                        description_text = (el.inner_text() or '').strip()
                        
                        # Check if this contains actual job content (not navigation)
                        if self.is_job_content(description_text):
                            # Store the cleaned HTML version
                            description_html = self.clean_job_description_html(html_content)
                            # Use HTML as the final description for storage
                            final_description = description_html
                            logger.info(f"✅ FOUND job content from selector: {sel}")
                            break
                except Exception as e:
                    logger.warning(f"Error with selector {sel}: {e}")
                    continue
            
            # Fallback to targeted NRM body extraction if HTML extraction completely failed
            if not final_description:
                logger.info("❌ No HTML content found, falling back to text extraction")
                description_text = self.extract_description_from_nrm_body(body_text)
                
                # Try to wrap the text content in basic HTML structure
                if description_text and len(description_text) > 50:
                    # Convert text to basic HTML format
                    html_lines = []
                    text_lines = description_text.split('\n')
                    current_para = []
                    
                    for line in text_lines:
                        line = line.strip()
                        if line:
                            current_para.append(line)
                        else:
                            if current_para:
                                html_lines.append(f"<p>{'<br>'.join(current_para)}</p>")
                                current_para = []
                    
                    # Add any remaining paragraph
                    if current_para:
                        html_lines.append(f"<p>{'<br>'.join(current_para)}</p>")
                    
                    final_description = '\n'.join(html_lines)
                    logger.info("✅ Converted text to HTML format")
                else:
                    final_description = f"<p>{description_text}</p>" if description_text else ""
                
                # Final fallback if still no content
                if len(final_description) < 20:
                    for sel in ['.job-description', '.job-details', 'article', 'main', '[class*="content"]']:
                        try:
                            el = page.query_selector(sel)
                            if el:
                                txt = (el.inner_text() or '').strip()
                                if txt and len(txt) > 50:
                                    description_text = txt
                                    final_description = f"<p>{txt.replace(chr(10), '</p><p>')}</p>"
                                    logger.info(f"✅ Final fallback HTML from {sel}")
                                    break
                        except Exception:
                            continue
            
            # Use text version for skills analysis if we have HTML, otherwise use final description
            analysis_text = description_text if description_text else final_description
            
            # Debug logging to see what we're storing
            logger.info(f"HTML Description type: {type(final_description)}")
            logger.info(f"HTML Description preview: {final_description[:200] if final_description else 'None'}...")
            logger.info(f"Contains HTML tags: {bool(re.search(r'<[^>]+>', final_description or ''))}")

            salary_text = salary_labeled
            if not salary_text:
                try:
                    m = re.search(r"\$[0-9,].{0,60}(?:per\s+(?:annum|year|hour)|p\.?a\.|\+\s*super)", body_text, re.IGNORECASE)
                    if m:
                        salary_text = m.group(0)
                except Exception:
                    pass

            # Extract skills from description using text version
            skills, preferred_skills = self.extract_skills_from_description(analysis_text, title)
            
            # Extract dates
            closing_date = self.extract_closing_date(body_text)
            posted_date_str = self.extract_posted_date(body_text)
            parsed_posted_date = self.parse_date(posted_date_str) if posted_date_str else timezone.now()
            
            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(body_text)
            location_obj = self.get_or_create_location(location_text)

            job_category = JobCategorizationService.categorize_job(title, analysis_text)

            external_id = ''
            m = re.search(r"/jobs/(?:20\d{2})/([0-9]{5,})", urlparse(job_url).path)
            if m:
                external_id = m.group(1)

            if not title or not final_description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            return {
                'title': title[:200],
                'description': final_description[:8000],
                'company_name': advertiser,
                'company_logo': company_logo_url,
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': parsed_posted_date,
                'external_url': job_url,
                'external_id': f"nrm_{external_id}" if external_id else f"nrm_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': posted_date_str or '',
                'skills': skills,
                'preferred_skills': preferred_skills,
                'job_closing_date': closing_date,
            }
        except Exception as e:
            logger.error(f"Detail extraction error: {e}")
            return None

    def save_job(self, data: dict) -> Optional[JobPosting]:
        try:
            with transaction.atomic():
                safe = self.sanitize_for_model(data)
                existing = JobPosting.objects.filter(external_url=safe['external_url']).first()
                if existing:
                    logger.info(f"Already exists, skipping: {existing.title}")
                    return existing
                company = self.get_or_create_company(safe.get('company_name'), safe.get('company_logo', ''))
                job = JobPosting.objects.create(
                    title=safe['title'],
                    description=safe['description'],
                    company=company,
                    posted_by=self.scraper_user,
                    location=safe['location'],
                    job_category=safe['job_category'],
                    job_type=safe['job_type'],
                    experience_level='',
                    work_mode=safe['work_mode'],
                    salary_min=safe['salary_min'],
                    salary_max=safe['salary_max'],
                    salary_currency=safe['salary_currency'],
                    salary_type=safe['salary_type'],
                    salary_raw_text=safe['salary_raw_text'],
                    external_source='nrmjobs.com.au',
                    external_url=safe['external_url'],
                    external_id=safe['external_id'],
                    status='active',
                    posted_ago=safe['posted_ago'],
                    date_posted=safe['date_posted'],
                    skills=safe.get('skills', ''),
                    preferred_skills=safe.get('preferred_skills', ''),
                    job_closing_date=safe.get('job_closing_date', ''),
                    tags='',
                    additional_info={'scraped_from': 'nrmjobs', 'scraper_version': '1.0'}
                )
                logger.info(f"Saved job: {job.title}")
                return job
        except Exception as e:
            logger.error(f"DB save error: {e}")
        return None

    def scrape(self) -> int:
        logger.info("Starting NRMjobs scraping...")
        self.setup_user()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            try:
                links = self.extract_job_links(page)
                logger.info(f"Found {len(links)} job detail links")
                for job_url in links:
                    if self.max_jobs and self.scraped_count >= self.max_jobs:
                        break
                    detail_page = context.new_page()
                    job_data = None
                    try:
                        job_data = self.extract_job_from_detail(detail_page, job_url)
                    finally:
                        try:
                            detail_page.close()
                        except Exception:
                            pass
                    if job_data:
                        saved = self.save_job(job_data)
                        if saved:
                            self.scraped_count += 1
                    self.human_like_delay(0.6, 1.2)
            finally:
                browser.close()

        connections.close_all()
        logger.info(f"Completed. Jobs processed: {self.scraped_count}")
        return self.scraped_count


def main():
    max_jobs = None
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid max_jobs argument. Provide an integer.")
            sys.exit(1)

    scraper = NRMJobsScraper(max_jobs=max_jobs, headless=True)
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


def run(max_jobs=None, headless=True):
    """Automation entrypoint for NRMjobs scraper."""
    try:
        scraper = NRMJobsScraper(max_jobs=max_jobs, headless=headless)
        count = scraper.scrape()
        return {
            'success': True,
            'jobs_scraped': count,
            'message': f'NRMjobs scraping completed, saved {count} jobs'
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
