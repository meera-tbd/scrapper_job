#!/usr/bin/env python
"""
Chandler Macleod Job Scraper using Playwright

This script scrapes job postings from Chandler Macleod's careers pages and stores
them in the professional database structure with `JobPosting`, `Company`, and `Location` models.

References:
- Search page: https://www.chandlermacleod.com/job-results#/
- Example detail page: https://www.chandlermacleod.com/job-details/staffing-administrator-in-human-resources-jobs-1274352

Usage:
    python script/scrape_chandlermacleod.py [max_jobs]
    
Optionally seed specific job detail URLs via env CHANDLER_START_URLS (comma-separated).
"""

import os
import sys
import re
import time
import random
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from base64 import b64decode
import json
import html as html_lib
from typing import Optional, List, Union

# Django setup (mirror voyages script conventions)
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


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_chandlermacleod.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class ChandlerMacleodScraper:
    """Playwright-based scraper for Chandler Macleod job postings."""

    def __init__(self, max_jobs=None, headless=True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.chandlermacleod.com"
        self.search_url = f"{self.base_url}/job-results#/"
        self.company = None
        self.scraper_user = None
        self.scraped_count = 0

    def human_like_delay(self, min_s=0.8, max_s=2.0):
        time.sleep(random.uniform(min_s, max_s))

    def extract_company_logo(self, page) -> str:
        """Extract company logo URL from the website."""
        try:
            # Navigate to home page to get logo
            page.goto(self.base_url, wait_until="domcontentloaded", timeout=30000)
            self.human_like_delay(1, 2)
            
            # Common logo selectors
            logo_selectors = [
                'img[alt*="Chandler Macleod" i]',
                'img[alt*="logo" i]',
                '.logo img',
                '.header-logo img',
                '.navbar-brand img',
                '.site-logo img',
                '[class*="logo"] img',
                'header img',
                '.header img:first-child'
            ]
            
            for selector in logo_selectors:
                try:
                    logo_element = page.query_selector(selector)
                    if logo_element:
                        logo_src = logo_element.get_attribute('src')
                        if logo_src:
                            # Convert relative URL to absolute
                            if logo_src.startswith('//'):
                                logo_url = f"https:{logo_src}"
                            elif logo_src.startswith('/'):
                                logo_url = f"{self.base_url}{logo_src}"
                            elif logo_src.startswith('http'):
                                logo_url = logo_src
                            else:
                                logo_url = f"{self.base_url}/{logo_src}"
                            
                            # Validate it's an image URL
                            if any(ext in logo_url.lower() for ext in ['.png', '.jpg', '.jpeg', '.svg', '.gif', '.webp']):
                                logger.info(f"Found company logo: {logo_url}")
                                return logo_url
                except Exception as e:
                    logger.debug(f"Error checking selector {selector}: {e}")
                    continue
            
            logger.warning("Could not find company logo on website")
            return ''
        except Exception as e:
            logger.error(f"Error extracting company logo: {e}")
            return ''

    def setup_database_objects(self):
        # First check if company already exists and has logo
        existing_company = Company.objects.filter(name="Chandler Macleod").first()
        
        self.company, created = Company.objects.get_or_create(
            name="Chandler Macleod",
            defaults={
                'description': "Chandler Macleod is a leading recruitment agency in Australia and New Zealand.",
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': ''
            }
        )
        
        # If company exists but doesn't have logo, or if new company was created, extract logo
        if created or not self.company.logo:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=self.headless)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                try:
                    logo_url = self.extract_company_logo(page)
                    if logo_url:
                        self.company.logo = logo_url
                        self.company.save(update_fields=['logo'])
                        logger.info(f"Updated company logo: {logo_url}")
                finally:
                    browser.close()
        self.scraper_user, _ = User.objects.get_or_create(
            username='chandler_scraper',
            defaults={
                'email': 'scraper@chandlermacleod.local',
                'first_name': 'Chandler',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: Optional[str]) -> Optional[Location]:
        if not location_text:
            return None
        text = location_text.strip()
        city = ''
        state = ''
        # Normalize common state abbreviations
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
        # Expand abbreviations if used
        for k, v in abbrev.items():
            text = re.sub(rf'\b{k}\b', v, text, flags=re.IGNORECASE)

        if ',' in text:
            parts = [p.strip() for p in text.split(',')]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        else:
            # May be only a state
            state = text

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
        if 'contract' in t:
            return 'contract'
        if 'temp' in t or 'temporary' in t:
            return 'temporary'
        if 'intern' in t:
            return 'internship'
        if 'free' in t:
            return 'freelance'
        # Treat permanent/full time as full_time
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
        # Determine period
        if re.search(r'hour', text, re.IGNORECASE):
            result['salary_type'] = 'hourly'
        elif re.search(r'week', text, re.IGNORECASE):
            result['salary_type'] = 'weekly'
        elif re.search(r'month', text, re.IGNORECASE):
            result['salary_type'] = 'monthly'
        elif re.search(r'year|annum|pa|p\.a\.', text, re.IGNORECASE):
            result['salary_type'] = 'yearly'

        # Extract numbers (e.g., $0.00 - $36.00)
        nums = re.findall(r'\$?\s*([0-9]{1,3}(?:[,][0-9]{3})*(?:\.[0-9]{1,2})?)', text)
        values = []
        for n in nums:
            try:
                values.append(float(n.replace(',', '')))
            except Exception:
                continue
        if values:
            # Filter out zeros if a higher number exists
            non_zero = [v for v in values if v > 0]
            if len(non_zero) >= 2:
                result['salary_min'] = min(non_zero)
                result['salary_max'] = max(non_zero)
            elif len(non_zero) == 1:
                result['salary_min'] = non_zero[0]
                result['salary_max'] = non_zero[0]
        return result

    def parse_posted_date(self, text: Optional[str]):
        if not text:
            return None
        t = text.strip()
        # Common formats like 12-Aug-2025 or 12 August 2025
        fmts = ["%d-%b-%Y", "%d-%B-%Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y"]
        for fmt in fmts:
            try:
                return datetime.strptime(t, fmt)
            except Exception:
                continue
        # Try to extract date-like token from a longer string
        m = re.search(r'(\d{1,2}[-/ ](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[-/ ]\d{4})', t, re.IGNORECASE)
        if m:
            for fmt in ["%d-%b-%Y", "%d %b %Y", "%d/%b/%Y"]:
                try:
                    return datetime.strptime(m.group(1), fmt)
                except Exception:
                    pass
        return None
    
    def clean_description(self, text: str) -> str:
        """Remove UI noise like 'Apply now', 'Share this job', etc., and compress whitespace."""
        if not text:
            return text

        ui_markers_exact = {
            'Apply now',
            'Share this job',
            'Interested in this job?',
            'Save Job',
            'Create as alert',
            'Similar Jobs',
            'Back to job search',
        }
        ui_markers_prefix = (
            'Apply now',
            'Share this job',
            'Interested in this job',
            'Create as alert',
            'Save Job',
        )

        lines = [ln.strip() for ln in text.split('\n')]
        cleaned_lines = []
        for ln in lines:
            if not ln:
                continue
            # Drop short CTA lines and social labels
            if ln in ui_markers_exact:
                continue
            if any(ln.startswith(pfx) for pfx in ui_markers_prefix):
                # Avoid dropping legitimate sentences by requiring short length
                if len(ln) <= 40:
                    continue
            # Drop lone social icons/text
            if ln.lower() in {'linkedin', 'facebook', 'twitter', 'x', 'email'}:
                continue
            cleaned_lines.append(ln)

        # Remove repeated CTA chunks that might still exist inline
        cleaned = '\n'.join(cleaned_lines)
        cleaned = re.sub(r"\n(?:Apply now|Share this job|Interested in this job\?|Create as alert|Save Job)(?:.*)?", '', cleaned, flags=re.IGNORECASE)
        # Normalize whitespace
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
        # Hard-stop at the first footer/navigation marker if present
        footer_markers = [
            'at chandler macleod',
            'what we do', 'our brands', 'workplace health & safety', 'diversity & inclusion',
            'offices', 'privacy policy', 'quality policy', 'modern slavery',
            'human rights workplace policy', 'whs', 'csr policy', 'terms & conditions',
            'you can read more about our commitment', 'you can read about our commitment',
            'all rights reserved', 'sitemap', 'site configuration', 'whistleblower policy'
        ]
        low = cleaned.lower()
        cut = None
        for m in footer_markers:
            idx = low.find(m)
            if idx != -1:
                cut = idx if cut is None else min(cut, idx)
        if cut is not None and cut > 0:
            cleaned = cleaned[:cut].rstrip()
        return cleaned

    def clean_html_description(self, html: str) -> str:
        """Clean HTML description while preserving structure for proper HTML format."""
        if not html:
            return ''
        
        # Remove UI noise but keep HTML structure
        ui_markers_exact = {
            'Apply now',
            'Share this job',
            'Interested in this job?',
            'Save Job',
            'Create as alert',
            'Similar Jobs',
            'Back to job search',
        }
        
        # Remove script and style tags
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
        
        # Convert to text temporarily to clean UI markers, then convert back to HTML-like structure
        text = self.html_to_text(html)
        cleaned_text = self.clean_description(text)
        
        # Convert back to simple HTML structure
        html_lines = []
        lines = cleaned_text.split('\n')
        in_list = False
        
        for line in lines:
            line = line.strip()
            if not line:
                if in_list:
                    html_lines.append('</ul>')
                    in_list = False
                html_lines.append('<br>')
                continue
                
            # Check if it's a list item
            if line.startswith('- '):
                if not in_list:
                    html_lines.append('<ul>')
                    in_list = True
                html_lines.append(f'<li>{line[2:]}</li>')
            else:
                if in_list:
                    html_lines.append('</ul>')
                    in_list = False
                html_lines.append(f'<p>{line}</p>')
        
        if in_list:
            html_lines.append('</ul>')
            
        # Join and clean up
        result = '\n'.join(html_lines)
        result = re.sub(r'<br>\s*<br>', '<br>', result)
        result = re.sub(r'<p></p>', '', result)
        
        return result.strip()

    def generate_skills_from_description(self, description: str) -> tuple[str, str]:
        """Extract skills and preferred skills from job description."""
        if not description:
            return '', ''
        
        # Convert HTML to text for analysis
        text = self.html_to_text(description).lower()
        
        # Common skill keywords and technologies
        technical_skills = [
            # Programming languages
            'python', 'java', 'javascript', 'typescript', 'c#', 'c++', 'ruby', 'php', 'go', 'kotlin', 'swift',
            'scala', 'rust', 'dart', 'r', 'matlab', 'sql', 'html', 'css', 'sass', 'less',
            
            # Frameworks and libraries
            'react', 'angular', 'vue', 'nodejs', 'express', 'django', 'flask', 'spring', 'laravel',
            'rails', 'asp.net', '.net', 'bootstrap', 'jquery', 'webpack', 'babel',
            
            # Databases
            'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'oracle', 'sqlite', 'cassandra',
            
            # Cloud and DevOps
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'gitlab', 'github', 'terraform',
            'ansible', 'chef', 'puppet', 'nginx', 'apache',
            
            # Data and Analytics
            'tableau', 'power bi', 'excel', 'powerpoint', 'word', 'outlook', 'sharepoint', 'salesforce',
            'hubspot', 'google analytics', 'seo', 'sem', 'adwords',
            
            # Other technical
            'api', 'rest', 'graphql', 'microservices', 'agile', 'scrum', 'kanban', 'jira', 'confluence',
            'git', 'svn', 'linux', 'windows', 'macos', 'unix'
        ]
        
        soft_skills = [
            'communication', 'leadership', 'teamwork', 'problem solving', 'analytical thinking',
            'project management', 'time management', 'customer service', 'negotiation', 'presentation',
            'training', 'mentoring', 'strategic planning', 'business analysis', 'stakeholder management',
            'change management', 'risk management', 'budget management', 'vendor management',
            'cross-functional collaboration', 'attention to detail', 'multitasking', 'adaptability',
            'innovation', 'creativity', 'critical thinking', 'decision making'
        ]
        
        industry_skills = [
            'healthcare', 'finance', 'banking', 'insurance', 'retail', 'manufacturing', 'construction',
            'mining', 'logistics', 'transportation', 'hospitality', 'education', 'government',
            'legal', 'marketing', 'sales', 'hr', 'recruitment', 'accounting', 'audit', 'compliance',
            'safety', 'quality assurance', 'procurement', 'supply chain', 'operations'
        ]
        
        all_skills = technical_skills + soft_skills + industry_skills
        
        # Find skills mentioned in the description
        found_skills = []
        for skill in all_skills:
            if skill in text:
                found_skills.append(skill.title())
        
        # Remove duplicates and sort
        found_skills = sorted(list(set(found_skills)))
        
        # Split into required and preferred based on context
        required_skills = []
        preferred_skills = []
        
        # Look for sections that indicate required vs preferred
        required_sections = [
            'required', 'must have', 'essential', 'mandatory', 'key requirements',
            'minimum requirements', 'qualifications', 'you will need'
        ]
        
        preferred_sections = [
            'preferred', 'nice to have', 'desirable', 'advantageous', 'bonus',
            'ideal candidate', 'would be great', 'plus'
        ]
        
        # Simple heuristic: if we can identify sections, categorize accordingly
        # Otherwise, put first 60% in required, rest in preferred
        text_lines = text.split('\n')
        current_section = 'required'  # default
        
        for line in text_lines:
            line_lower = line.lower().strip()
            
            # Check if this line indicates a section change
            if any(phrase in line_lower for phrase in preferred_sections):
                current_section = 'preferred'
            elif any(phrase in line_lower for phrase in required_sections):
                current_section = 'required'
            
            # Find skills in this line
            for skill in found_skills:
                if skill.lower() in line_lower:
                    if current_section == 'required' and skill not in required_skills:
                        required_skills.append(skill)
                    elif current_section == 'preferred' and skill not in preferred_skills:
                        preferred_skills.append(skill)
        
        # If we couldn't categorize any skills using sections, use a simple split
        if not required_skills and not preferred_skills:
            split_point = max(1, len(found_skills) * 2 // 3)  # 2/3 required, 1/3 preferred
            required_skills = found_skills[:split_point]
            preferred_skills = found_skills[split_point:]
        
        # Ensure skills don't appear in both lists
        preferred_skills = [s for s in preferred_skills if s not in required_skills]
        
        # Join with commas and limit length
        required_str = ', '.join(required_skills[:10])  # Limit to 10 skills
        preferred_str = ', '.join(preferred_skills[:8])   # Limit to 8 skills
        
        return required_str, preferred_str

    def html_to_text(self, html: str) -> str:
        if not html:
            return ''
        # Convert common block/line-break tags to newlines to avoid word merging
        html = re.sub(r'(?i)<br\s*/?>', '\n', html)
        html = re.sub(r'(?i)</p>', '\n\n', html)
        html = re.sub(r'(?i)<p[^>]*>', '', html)
        html = re.sub(r'(?i)</div>', '\n', html)
        html = re.sub(r'(?i)<div[^>]*>', '', html)
        html = re.sub(r'(?i)</section>', '\n', html)
        html = re.sub(r'(?i)<section[^>]*>', '', html)
        html = re.sub(r'(?i)</article>', '\n', html)
        html = re.sub(r'(?i)<article[^>]*>', '', html)
        html = re.sub(r'(?i)</ul>', '\n', html)
        html = re.sub(r'(?i)<ul[^>]*>', '', html)
        html = re.sub(r'(?i)</ol>', '\n', html)
        html = re.sub(r'(?i)<ol[^>]*>', '', html)
        html = re.sub(r'(?i)<li[^>]*>', '\n- ', html)
        html = re.sub(r'(?i)</li>', '', html)
        html = re.sub(r'(?i)</h[1-6]>', '\n\n', html)
        html = re.sub(r'(?i)<h[1-6][^>]*>', '', html)
        text = re.sub(r'<[^>]+>', '', html)
        text = html_lib.unescape(text)
        text = re.sub(r'\u00a0', ' ', text)
        text = re.sub(r'\r', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def extract_description_from_dynamic_json(self, page) -> str:
        """Use the site's embedded base64 JSON (window.Parameters.DynamicPageInfo.base64JsonRowData)."""
        try:
            base64_json = page.evaluate(
                "() => (window && window.Parameters && window.Parameters.DynamicPageInfo && window.Parameters.DynamicPageInfo.base64JsonRowData) || ''"
            )
        except Exception:
            base64_json = ''
        if not base64_json:
            return ''

        # Decode and return the HTML description from the embedded JSON
        try:
            raw = b64decode(base64_json).decode('utf-8', errors='ignore')
            data = json.loads(raw)
            html_desc = (
                data.get('fullDescription')
                or data.get('SEOMetaDescription')
                or data.get('pSEOMetaDescription')
                or ''
            )
            # Clean HTML but preserve structure
            return self.clean_html_description(html_desc)
        except Exception:
            return ''

    def normalize_category_choice(self, raw_text: Optional[str]) -> str:
        """Map raw category text from site to our JOB_CATEGORY_CHOICES."""
        if not raw_text:
            return 'other'
        t = raw_text.strip().lower()
        if not t:
            return 'other'
        # Normalize separators
        t = t.replace('&amp;', '&').replace(' and ', ' & ').replace('---', '-').replace('_', ' ')
        t = re.sub(r'\s+', ' ', t)
        # Direct maps
        mapping = {
            'office support': 'office_support',
            'drivers & operators': 'drivers_operators',
            'drivers and operators': 'drivers_operators',
            'technical & engineering': 'technical_engineering',
            'technical and engineering': 'technical_engineering',
            'production workers': 'production_workers',
            'transport & logistics': 'transport_logistics',
            'transport and logistics': 'transport_logistics',
            'mining & resources': 'mining_resources',
            'mining and resources': 'mining_resources',
            'sales & marketing': 'sales_marketing',
            'sales and marketing': 'sales_marketing',
            'executive': 'executive',
            'accounting & finance': 'finance',
            'accounting and finance': 'finance',
            'human resources': 'hr',
            'hr & personnel': 'hr',
            'legal': 'legal',
            'technology': 'technology',
            'manufacturing': 'manufacturing',
            'construction': 'construction',
            'retail': 'retail',
            'hospitality': 'hospitality',
            'education': 'education',
            'healthcare': 'healthcare',
            'sales': 'sales',
            'marketing': 'marketing',
        }
        # Exact key match
        if t in mapping:
            return mapping[t]
        # Hyphen/slug style like 'transport---logistics' or 'technical-engineering'
        slug = t.replace('-', ' ').replace('/', ' ')
        if slug in mapping:
            return mapping[slug]
        # Fuzzy contains for composites
        for key, val in mapping.items():
            key_simple = key.replace(' & ', ' ').replace('-', ' ')
            if key_simple in slug:
                return val
        return 'other'

    def map_category_strict(self, raw_text: Optional[str]) -> str:
        """Strict category mapper. Only returns a known key if there is a clear, direct match.

        If no exact mapping is found, returns 'other' to avoid wrong categorization.
        """
        if not raw_text:
            return 'other'
        t = raw_text.strip().lower()
        if not t:
            return 'other'
        t = t.replace('&amp;', '&').replace(' and ', ' & ').replace('---', '-').replace('_', ' ')
        t = re.sub(r"\s+", " ", t)

        mapping = {
            'office support': 'office_support',
            'drivers & operators': 'drivers_operators',
            'drivers and operators': 'drivers_operators',
            'technical & engineering': 'technical_engineering',
            'technical and engineering': 'technical_engineering',
            'production workers': 'production_workers',
            'transport & logistics': 'transport_logistics',
            'transport and logistics': 'transport_logistics',
            'mining & resources': 'mining_resources',
            'mining and resources': 'mining_resources',
            'sales & marketing': 'sales_marketing',
            'sales and marketing': 'sales_marketing',
            'executive': 'executive',
            'accounting & finance': 'finance',
            'accounting and finance': 'finance',
            'human resources': 'hr',
            'hr & personnel': 'hr',
            'legal': 'legal',
            'technology': 'technology',
            'manufacturing': 'manufacturing',
            'construction': 'construction',
            'retail': 'retail',
            'hospitality': 'hospitality',
            'education': 'education',
            'healthcare': 'healthcare',
            'sales': 'sales',
            'marketing': 'marketing',
            'trades & labour': 'trades_labour',
            'it & digital': 'it_digital',
            'it and digital': 'it_digital',
            'engineering': 'engineering',
            'engineering and construction': 'engineering_construction',
            'accounting & finance': 'finance',
            'accounting and finance': 'finance',
            'accounting': 'finance',
            'finance': 'finance',
            'accounting and financial': 'finance',
            'accounting and financial services': 'finance',
            'accounting and financial services': 'finance',
        }
        if t in mapping:
            return mapping[t]
        slug = t.replace('-', ' ').replace('/', ' ')
        if slug in mapping:
            return mapping[slug]
        return 'other'

    def ensure_category_choice(self, display_text: str) -> str:
        """Ensure a category choice exists for a new display string; return its slug.

        This appends to `JobPosting.JOB_CATEGORY_CHOICES` at runtime so admin/forms
        can render the value. No migration is needed since the DB stores plain text.
        """
        if not display_text:
            return 'other'
        # Build slug-like key within 50 chars
        key = slugify(display_text).replace('-', '_')[:50] or 'other'
        if not any(choice[0] == key for choice in JobPosting.JOB_CATEGORY_CHOICES):
            # Append new dynamic choice
            JobPosting.JOB_CATEGORY_CHOICES.append((key, display_text.strip()))
        return key

    def extract_job_links_from_search(self, page) -> list[str]:
        links = set()
        try:
            # SPA often never reaches strict "networkidle"; use domcontentloaded
            page.goto(self.search_url, wait_until="domcontentloaded", timeout=30000)
            self.human_like_delay(2, 3)

            # Try to wait for any potential job link to appear
            try:
                page.wait_for_selector('a[href*="/job-details/"]', timeout=15000)
            except Exception:
                # Try to stimulate lazy loading by scrolling
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    self.human_like_delay(1, 2)
                    page.wait_for_selector('a[href*="/job-details/"]', timeout=5000)
                except Exception:
                    pass

            # Collect all anchors and filter job-details links (exclude mailto and other schemes)
            anchors = page.query_selector_all('a[href]')
            for a in anchors:
                href = a.get_attribute('href')
                if not href:
                    continue
                href_lower = href.lower()
                # Skip mailto/share links
                if href_lower.startswith('mailto:') or href_lower.startswith('tel:') or href_lower.startswith('javascript:'):
                    continue
                # Only accept http(s) or relative paths that actually point to job-details
                if '/job-details/' in href_lower:
                    if href_lower.startswith('http'):
                        full = href
                    elif href_lower.startswith('/'):
                        full = urljoin(self.base_url, href)
                    else:
                        continue
                    links.add(full)

            # Also capture explicit Read more links in case their hrefs are not caught above
            try:
                read_links = page.query_selector_all('a:has-text("Read more"), a:has-text("Read More")')
                for a in read_links:
                    href = a.get_attribute('href') or ''
                    low = href.lower()
                    if not href or low.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    if '/job-details/' in low:
                        full = href if low.startswith('http') else urljoin(self.base_url, href)
                        links.add(full)
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Search page extraction warning: {e}")
        return list(links)

    def collect_job_links_all_pages(self, page, target_count: Optional[int]) -> List[str]:
        """Collect links across numeric pagination by clicking pager buttons.

        This does not rely on hrefs on the pager; it clicks visible elements whose text is a page number.
        """
        def get_links() -> list[str]:
            anchors = page.query_selector_all('a[href]')
            found = []
            for a in anchors:
                href = a.get_attribute('href') or ''
                low = href.lower()
                if not href or low.startswith(('mailto:', 'tel:', 'javascript:')):
                    continue
                if '/job-details/' in low:
                    found.append(href if low.startswith('http') else urljoin(self.base_url, href))
            # Include Read more anchors explicitly
            try:
                read_links = page.query_selector_all('a:has-text("Read more"), a:has-text("Read More")')
                for a in read_links:
                    href = a.get_attribute('href') or ''
                    low = href.lower()
                    if not href or low.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    if '/job-details/' in low:
                        found.append(href if low.startswith('http') else urljoin(self.base_url, href))
            except Exception:
                pass
            return list(sorted(set(found)))

        def signature() -> str:
            try:
                return '|'.join(get_links())
            except Exception:
                return ''

        collected: list[str] = []
        seen = set()

        # Ensure first page loaded
        page.goto(self.search_url, wait_until="domcontentloaded", timeout=30000)
        self.human_like_delay(1.2, 2.0)
        try:
            page.wait_for_selector('a[href*="/job-details/"]', timeout=8000)
        except Exception:
            pass

        # Add from page 1
        for l in get_links():
            if l not in seen:
                seen.add(l)
                collected.append(l)
        logger.info(f"Page 1: found {len(collected)} unique so far")
        if target_count and len(collected) >= target_count:
            return collected

        # Try to detect all numeric page buttons
        pager_candidates = page.query_selector_all('a, button, [role="button"], li a')
        numbers = set()
        for el in pager_candidates:
            try:
                txt = (el.inner_text() or '').strip()
            except Exception:
                continue
            if re.fullmatch(r"\d+", txt):
                try:
                    num = int(txt)
                    if num > 1:
                        numbers.add(num)
                except Exception:
                    continue
        numbers = sorted(numbers)

        # Click each number sequentially
        for num in numbers:
            if target_count and len(collected) >= target_count:
                break
            before = signature()
            clicked = False
            # Prefer the most specific locator by exact text
            try:
                page.locator(f"a:has-text(\"{num}\")").first.click(timeout=2000)
                clicked = True
            except Exception:
                try:
                    page.locator(f"button:has-text(\"{num}\")").first.click(timeout=2000)
                    clicked = True
                except Exception:
                    pass
            if not clicked:
                # Hash fallback
                try:
                    page.evaluate(f"window.location.hash = '#/pg-{num}'; window.dispatchEvent(new HashChangeEvent('hashchange'));")
                except Exception:
                    pass
            self.human_like_delay(0.8, 1.5)
            # Wait for change
            try:
                page.wait_for_function("prev => Array.from(document.querySelectorAll('a[href*=/job-details/]')).map(a=>a.href).sort().join('|') !== prev", before, timeout=5000)
            except Exception:
                self.human_like_delay(0.8, 1.2)
            # Add links from this page
            new_links = 0
            for l in get_links():
                if l not in seen:
                    seen.add(l)
                    collected.append(l)
                    new_links += 1
            logger.info(f"Page {num}: added {new_links}, total {len(collected)}")
        return collected

    def extract_field_by_label(self, body_text: str, label: str, stop_labels: list[str]) -> str:
        pattern = rf"{re.escape(label)}\s*\n?\s*(.*?)\s*(?:\n|$|" + "|".join(map(re.escape, stop_labels)) + ")"
        m = re.search(pattern, body_text, re.IGNORECASE | re.DOTALL)
        if m:
            val = m.group(1).strip()
            # Trim trailing label bleed
            for sl in stop_labels:
                idx = val.lower().find(sl.lower())
                if idx > 0:
                    val = val[:idx].strip()
            return re.sub(r'\s+', ' ', val)
        return ''

    def extract_job_from_detail(self, page, job_url: str) -> Optional[dict]:
        try:
            # Many sites keep background connections open; avoid networkidle here
            try:
                page.goto(job_url, wait_until="domcontentloaded", timeout=35000)
            except Exception:
                # Retry once with a longer timeout and then continue even if load state isn't perfect
                page.goto(job_url, wait_until="load", timeout=60000)
            self.human_like_delay(1.0, 2.0)

            # Wait for key selectors to ensure content is present
            try:
                page.wait_for_selector('h1, .job-details, .job-description, .description', timeout=15000)
            except Exception:
                # As a fallback, wait a bit more; continue anyway
                self.human_like_delay(1.0, 1.5)

            title = ''
            # Prefer h1 titles
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title = (h1.inner_text() or '').strip()
            except Exception:
                pass

            # Fallback: derive from URL slug
            if not title:
                slug = urlparse(job_url).path.split('/')[-1]
                slug_no_id = re.sub(r'-\d+$', '', slug)
                words = [w for w in slug_no_id.split('-') if w and w.lower() not in {'in', 'jobs'}]
                title = ' '.join(w.capitalize() for w in words)[:200]

            body_text = ''
            try:
                body_text = page.inner_text('body')
            except Exception:
                body_text = ''

            # Description: try dynamic JSON first, then DOM containers - preserve HTML format
            description = ''
            dyn_desc = self.extract_description_from_dynamic_json(page)
            if dyn_desc and len(dyn_desc) > 100:
                description = dyn_desc
            else:
                # Try to get HTML content from DOM containers
                for sel in ['.job-description', '.description', '.job-details', '.content', 'main', 'article', '[class*="description"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            # Get innerHTML to preserve HTML structure
                            html_content = el.inner_html()
                            if html_content and len(html_content) > 150:
                                description = self.clean_html_description(html_content)
                                break
                            else:
                                # Fallback to inner_text and convert to simple HTML
                                txt = (el.inner_text() or '').strip()
                                if txt and len(txt) > 150:
                                    cleaned_text = self.clean_description(txt)
                                    # Convert to simple HTML
                                    html_lines = []
                                    for line in cleaned_text.split('\n'):
                                        line = line.strip()
                                        if line:
                                            if line.startswith('- '):
                                                html_lines.append(f'<li>{line[2:]}</li>')
                                            else:
                                                html_lines.append(f'<p>{line}</p>')
                                    description = '\n'.join(html_lines)
                                    break
                    except Exception:
                        continue
                        
                if not description and body_text:
                    # Heuristic: slice between responsibilities/what and similar/footer
                    start_idx = 0
                    for s in ['Key Responsibilities', 'What We', 'About The Role', 'The Role', 'Position Summary']:
                        i = body_text.lower().find(s.lower())
                        if i != -1:
                            start_idx = i
                            break
                    end_idx = len(body_text)
                    for e in ['Similar Jobs', 'Apply now', 'Create as alert', 'Privacy Policy', 'All rights reserved', 'Share this job', 'Interested in this job?']:
                        j = body_text.lower().find(e.lower(), start_idx + 50)
                        if j != -1:
                            end_idx = j
                            break
                    chunk = body_text[start_idx:end_idx].strip()
                    if chunk and len(chunk) > 150:
                        cleaned_text = self.clean_description(chunk)
                        # Convert to simple HTML
                        html_lines = []
                        for line in cleaned_text.split('\n'):
                            line = line.strip()
                            if line:
                                if line.startswith('- '):
                                    html_lines.append(f'<li>{line[2:]}</li>')
                                else:
                                    html_lines.append(f'<p>{line}</p>')
                        description = '\n'.join(html_lines)

            # Metadata fields commonly shown in sidebar/summary
            stop_labels = ['Category', 'Salary', 'Posted', 'Work type', 'Work Type', 'Contact', 'Reference']
            location_text = self.extract_field_by_label(body_text, 'Location', stop_labels) if body_text else ''
            salary_text = self.extract_field_by_label(body_text, 'Salary', stop_labels) if body_text else ''
            posted_text = self.extract_field_by_label(body_text, 'Posted', stop_labels) if body_text else ''
            work_type_text = self.extract_field_by_label(body_text, 'Work type', stop_labels) or self.extract_field_by_label(body_text, 'Work Type', stop_labels)
            category_text = self.extract_field_by_label(body_text, 'Category', stop_labels) if body_text else ''

            # Parse fields
            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(work_type_text or description)
            date_posted = self.parse_posted_date(posted_text)
            location_obj = self.get_or_create_location(location_text)

            # Prefer the visible on-page Category first; then fall back to JSON-derived values
            job_category = 'other'
            category_raw_value = ''
            # Strict mapping first to avoid wrong categories
            page_category_mapped = self.map_category_strict(category_text)
            if page_category_mapped != 'other':
                job_category = page_category_mapped
                category_raw_value = (category_text or '').strip()
            elif category_text and category_text.strip():
                job_category = self.ensure_category_choice(category_text)
                category_raw_value = category_text.strip()
            else:
                try:
                    base64_json = page.evaluate("() => (window && window.Parameters && window.Parameters.DynamicPageInfo && window.Parameters.DynamicPageInfo.base64JsonRowData) || ''")
                    if base64_json:
                        raw = b64decode(base64_json).decode('utf-8', errors='ignore')
                        data = json.loads(raw)
                        raw_candidates = [
                            data.get('subCategory'),
                            data.get('roleSeo'),
                            data.get('professionSeo'),
                            data.get('jobCategory'),
                        ]
                        for rc in raw_candidates:
                            jc = self.map_category_strict(rc)
                            if jc != 'other':
                                job_category = jc
                                category_raw_value = (rc or '').strip()
                                break
                        if job_category == 'other':
                            # If we have a raw category string, add it as a dynamic choice
                            for rc in raw_candidates:
                                if rc and rc.strip():
                                    job_category = self.ensure_category_choice(rc)
                                    category_raw_value = rc.strip()
                                    break
                            if job_category == 'other':
                                job_category = JobCategorizationService.categorize_job(title, description)
                    else:
                        job_category = JobCategorizationService.categorize_job(title, description)
                except Exception:
                    job_category = JobCategorizationService.categorize_job(title, description)

            external_id = ''
            m = re.search(r'-(\d{5,})$', urlparse(job_url).path)
            if m:
                external_id = m.group(1)

            # Ensure title and description present
            if not title or not description:
                logger.warning(f"Skipping (insufficient content): {job_url}")
                return None

            # Generate skills and preferred skills from description
            skills, preferred_skills = self.generate_skills_from_description(description)

            return {
                'title': title.strip(),
                'description': description.strip()[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': date_posted or timezone.now(),
                'external_url': job_url,
                'external_id': f"chandler_{external_id}" if external_id else f"chandler_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': category_raw_value,
                'skills': skills,
                'preferred_skills': preferred_skills,
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    def save_job(self, data: dict) -> Optional[JobPosting]:
        try:
            with transaction.atomic():
                existing = JobPosting.objects.filter(external_url=data['external_url']).first()
                if existing:
                    logger.info(f"Already exists, skipping: {existing.title}")
                    return existing
                # If we carried the original category text, ensure it's preserved in additional_info
                job = JobPosting.objects.create(
                    title=data['title'],
                    description=data['description'],
                    company=self.company,
                    posted_by=self.scraper_user,
                    location=data['location'],
                    job_category=data['job_category'],
                    job_type=data['job_type'],
                    experience_level='',
                    work_mode=data['work_mode'],
                    salary_min=data['salary_min'],
                    salary_max=data['salary_max'],
                    salary_currency=data['salary_currency'],
                    salary_type=data['salary_type'],
                    salary_raw_text=data['salary_raw_text'],
                    external_source='chandlermacleod.com',
                    external_url=data['external_url'],
                    external_id=data['external_id'],
                    status='active',
                    posted_ago=data['posted_ago'],
                    date_posted=data['date_posted'],
                    tags='',
                    skills=data.get('skills', ''),
                    preferred_skills=data.get('preferred_skills', ''),
                    additional_info={
                        'scraped_from': 'chandler_macleod',
                        'scraper_version': '1.0'
                    }
                )
                # Merge category_raw into additional_info if present
                if data.get('category_raw'):
                    info = job.additional_info or {}
                    info['category_raw'] = data['category_raw']
                    job.additional_info = info
                    job.save(update_fields=['additional_info'])
                logger.info(f"Saved job: {job.title}")
                return job
        except Exception as e:
            logger.error(f"DB save error: {e}")
        return None
    
    def scrape(self) -> int:
        logger.info("Starting Chandler Macleod scraping...")
        self.setup_database_objects()

        # Seed from env if provided
        seed_urls = []
        env_urls = os.getenv('CHANDLER_START_URLS')
        if env_urls:
            seed_urls = [u.strip() for u in env_urls.split(',') if u.strip()]

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            try:
                # Try full multipage collector first
                links = self.collect_job_links_all_pages(page, self.max_jobs)
                # Always merge single-page results in case pager missed items
                base_links = self.extract_job_links_from_search(page)
                for bl in base_links:
                    if bl not in links:
                        links.append(bl)
                # Merge seeds
                for s in seed_urls:
                    if s not in links:
                        links.append(s)

                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning("No job links found from search; provide CHANDLER_START_URLS to seed specific jobs.")

                for i, job_url in enumerate(links):
                    if self.max_jobs and self.scraped_count >= self.max_jobs:
                        break
                    # Use a fresh page per job to avoid stuck requests
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
                    self.human_like_delay(0.5, 1.2)
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

    scraper = ChandlerMacleodScraper(max_jobs=max_jobs, headless=True)
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


def run(max_jobs=None):
    """Automation entrypoint for Chandler Macleod scraper."""
    try:
        scraper = ChandlerMacleodScraper(max_jobs=max_jobs, headless=True)
        count = scraper.scrape()
        return {
            'success': True,
            'jobs_scraped': count,
            'message': f'Chandler Macleod scraping completed, saved {count} jobs'
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



