#!/usr/bin/env python
"""
Simple Michael Page Australia Job Scraper using HTML parsing

This script provides a simpler alternative approach that parses the HTML content
to extract job information without relying on complex browser automation.

Features:
- Direct HTML parsing approach
- Less likely to be blocked by anti-bot measures
- Faster execution
- Uses the same database structure as other scrapers

Usage:
    python michaelpage_simple_scraper.py [max_jobs]

Examples:
    python michaelpage_simple_scraper.py 20
"""

import os
import sys
import re
import time
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, quote
import logging
from decimal import Decimal
import requests
from bs4 import BeautifulSoup
import concurrent.futures

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.utils import timezone
from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils.text import slugify

# Import our professional models
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService

User = get_user_model()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Back to INFO level since job types are working correctly
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('michaelpage_simple_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SimpleMichaelPageScraper:
    """
    Simple HTTP-based Michael Page Australia scraper.
    """
    
    def __init__(self, job_limit=None):
        """Initialize the simple scraper."""
        self.base_url = "https://www.michaelpage.com.au"
        self.job_limit = job_limit
        
        self.scraped_count = 0
        self.duplicate_count = 0
        self.error_count = 0
        
        # Set up session with realistic headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-AU,en;q=0.9,en-US;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Get or create system user for job posting
        self.system_user = self.get_or_create_system_user()
        
    def get_or_create_system_user(self):
        """Get or create system user for posting jobs."""
        try:
            user, created = User.objects.get_or_create(
                username='michaelpage_simple_scraper',
                defaults={
                    'email': 'system@michaelpagesimplescraper.com',
                    'first_name': 'Michael Page Simple',
                    'last_name': 'Scraper',
                    'is_staff': True,
                    'is_active': True
                }
            )
            if created:
                logger.info("Created system user for job posting")
            return user
        except Exception as e:
            logger.error(f"Error creating system user: {str(e)}")
            return None
    
    def human_delay(self, min_seconds=0.1, max_seconds=0.3):
        """Add minimal delay between requests (optimized for speed)."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def parse_date(self, date_string):
        """Parse relative date strings into datetime objects."""
        if not date_string:
            return None
            
        date_string = date_string.lower().strip()
        now = timezone.now()
        
        # Handle "today" and "yesterday"
        if 'today' in date_string:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        elif 'yesterday' in date_string:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Extract number and unit from strings like "2 days ago"
        match = re.search(r'(\d+)\s*(day|week|month|hour)s?\s*ago', date_string)
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
    
    def fetch_full_description_html_and_text(self, job_url):
        """Fetch and return the job description as sanitized HTML and plain text.

        Returns a tuple: (html_description, plain_text_description, meta_dict).
        meta_dict may include: location, phone, email extracted from the Job summary panel.
        """
        try:
            if not job_url:
                return "", "", {}

            response = self.session.get(job_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Prefer well-known Michael Page blocks if present
            preferred_selectors = [
                'div.job-description',
                'div.job_advert__description',
                'div.job-advert__description',
                'div.job-advert',
                'article.job',
                'main',
                'div[role="main"]'
            ]

            def sanitize_container_to_html(container):
                if not container:
                    return ""
                # Remove clearly irrelevant elements (buttons, forms, nav, scripts)
                for sel in [
                    'script', 'style', 'form', 'nav', 'header', 'footer',
                    '.apply', '.save-job', '.apply-link', '.save-links', '.share',
                    '[class*="apply"]', '[class*="save"]', '[class*="share"]'
                ]:
                    for tag in container.select(sel):
                        tag.decompose()
                # Drop links that are just actions
                for a in container.find_all('a'):
                    text = a.get_text(strip=True).lower()
                    if any(k in text for k in ['apply', 'save job', 'refer']):
                        a.decompose()
                # Remove top-of-page navigation lists like Back to Search / Summary / Similar Jobs / FIFO
                nav_phrases = [
                    'back to search', 'job description', 'summary', 'similar jobs',
                    'newly created position', 'fifo to png'
                ]
                for lst in container.find_all(['ul', 'ol']):
                    lst_text = ' '.join([li.get_text(' ', strip=True).lower() for li in lst.find_all('li')])
                    if lst_text:
                        hits = sum(1 for p in nav_phrases if p in lst_text)
                        if hits >= 2:
                            lst.decompose()
                # Explicitly remove any Job summary blocks
                for hd in container.find_all(['h2', 'h3']):
                    if 'job summary' in hd.get_text(strip=True).lower():
                        parent_block = hd.find_parent(['section', 'div']) or hd
                        try:
                            parent_block.decompose()
                        except Exception:
                            pass
                # Remove any Diversity & Inclusion boilerplate sections entirely
                def remove_section_from_heading(heading_tag):
                    # Remove bullets directly above the heading (often empty teasers)
                    prev = heading_tag.previous_sibling
                    while prev is not None and getattr(prev, 'name', None) in ['ul', 'ol', 'br']:
                        try:
                            tmp = prev.previous_sibling
                            prev.decompose()
                            prev = tmp
                        except Exception:
                            break
                    # Remove the heading and everything until the next heading
                    node = heading_tag
                    while node is not None:
                        nxt = node.next_sibling
                        try:
                            node.decompose()
                        except Exception:
                            break
                        if getattr(nxt, 'name', None) in ['h2', 'h3']:
                            break
                        node = nxt
                for hd in list(container.find_all(['h2', 'h3'])):
                    txt = (hd.get_text(strip=True) or '').lower()
                    if 'diversity' in txt and 'inclusion' in txt:
                        remove_section_from_heading(hd)
                # If there is any content before the first meaningful heading, remove it
                allowed_headings = [
                    'about our client', 'job description', 'the successful applicant',
                    "what's on offer", 'requirements', 'responsibilities', 'skills and experience'
                ]
                first_heading = None
                for hd in container.find_all(['h2', 'h3']):
                    ht = hd.get_text(strip=True).lower()
                    if any(h in ht for h in allowed_headings):
                        first_heading = hd
                        break
                if first_heading is not None:
                    sib = first_heading.previous_sibling
                    # Remove all siblings before the first meaningful heading
                    while sib is not None:
                        prev = sib.previous_sibling
                        try:
                            sib.extract()
                        except Exception:
                            pass
                        sib = prev
                    # Also remove any lists that appear before the first heading anywhere in the container
                    for pre_list in list(container.find_all(['ul', 'ol'])):
                        # If the previous heading before the list is None or comes after the list, drop it
                        prev_heading = pre_list.find_previous(['h2', 'h3'])
                        if prev_heading is None or prev_heading is not first_heading and prev_heading in first_heading.find_all_previous(['h2','h3']):
                            try:
                                pre_list.decompose()
                            except Exception:
                                pass
                # Allow only a small set of tags, unwrap the rest
                allowed_tags = {'p', 'ul', 'ol', 'li', 'strong', 'b', 'em', 'i', 'h2', 'h3', 'br'}
                for tag in list(container.find_all(True)):
                    if tag.name not in allowed_tags:
                        tag.unwrap()
                # Remove list items that are empty or contain footer contact metadata
                contact_phrases = ['quote job ref', 'phone number']
                for li in list(container.find_all('li')):
                    txt = (li.get_text(' ', strip=True) or '').lower()
                    if not txt:
                        li.decompose()
                        continue
                    if any(p in txt for p in contact_phrases) or re.match(r'^contact\b', txt):
                        li.decompose()
                for p in list(container.find_all('p')):
                    txt = (p.get_text(' ', strip=True) or '').lower()
                    if any(pht in txt for pht in contact_phrases) or re.match(r'^contact\b', txt):
                        p.decompose()
                # Remove any empty UL/OL created by the cleanup
                for lst in list(container.find_all(['ul', 'ol'])):
                    if not lst.find('li'):
                        lst.decompose()
                html = str(container)
                # Light cleanup for excessive whitespace
                html = re.sub(r"\n\s*\n+", "\n\n", html)
                return html.strip()

            def collect_text(container):
                if not container:
                    return ""
                parts = []
                # Capture structured sections first (common MP headings)
                headings = container.find_all(['h2', 'h3'])
                if headings:
                    # Allowed and ignored headings on Michael Page
                    allowed_headings = [
                        'about our client', 'job description', 'the successful applicant',
                        "what's on offer", 'benefits', 'requirements', 'responsibilities',
                        'key responsibilities', 'skills and experience', 'your profile', 'the role'
                    ]
                    ignored_headings = [
                        'job summary', 'save job', 'apply', 'diversity & inclusion',
                        'other users applied'
                    ]
                    # Normalizer used for deduplication
                    def norm_text(t):
                        t = re.sub(r'\s+', ' ', (t or '')).strip().lower()
                        t = re.sub(r'[^a-z0-9\-&\s]', '', t)
                        return t
                    section_map = {}
                    section_title_for_key = {}
                    section_order = []
                    global_seen = set()
                    for heading in headings:
                        heading_text = heading.get_text(strip=True)
                        if not heading_text:
                            continue
                        heading_norm = heading_text.lower()
                        if any(h in heading_norm for h in ignored_headings):
                            continue
                        if not any(h in heading_norm for h in allowed_headings):
                            # Skip unknown/side headings to avoid noise blocks
                            continue
                        # Use the allowed heading as a stable key to merge duplicates
                        key = None
                        for ah in allowed_headings:
                            if ah in heading_norm:
                                key = ah
                                break
                        if key is None:
                            key = heading_norm
                        if key not in section_map:
                            section_map[key] = []
                            section_title_for_key[key] = heading_text
                            section_order.append(key)
                        section_lines = []
                        for sib in heading.find_all_next():
                            # Stop at the next heading at the same or higher level
                            if sib.name in ['h2', 'h3']:
                                break
                            if sib.name in ['p', 'div']:
                                text = sib.get_text(" ", strip=True)
                                if text:
                                    section_lines.append(text)
                            elif sib.name in ['ul', 'ol']:
                                for li in sib.find_all('li'):
                                    li_text = li.get_text(" ", strip=True)
                                    if li_text:
                                        section_lines.append(f"- {li_text}")
                        # Deduplicate within section and globally; drop contact details
                        unique_lines = []
                        seen_local = set()
                        for ln in section_lines:
                            n = norm_text(ln)
                            if not n:
                                continue
                            if any(k in n for k in [
                                'consultant name', 'consultant phone', 'job reference',
                                'phone number', 'contact '
                            ]):
                                continue
                            if n in seen_local or n in global_seen:
                                continue
                            seen_local.add(n)
                            global_seen.add(n)
                            unique_lines.append(ln)
                        if unique_lines:
                            section_map[key].extend(unique_lines)
                    # If we built sections, format them once per heading in order
                    if any(section_map.values()):
                        for key in section_order:
                            body = section_map.get(key) or []
                            if not body:
                                continue
                            parts.append(section_title_for_key.get(key, key.title()))
                            parts.append('\n'.join(body))
                # Fallback: longest paragraph/list text from container
                if not parts:
                    texts = []
                    for tag in container.find_all(['p', 'li']):
                        t = tag.get_text(" ", strip=True)
                        if t:
                            texts.append(t)
                    if texts:
                        parts.append('\n'.join(texts))
                text_joined = '\n\n'.join([p for p in parts if p])
                # Global cleanups to remove unwanted boilerplate
                remove_phrases = [
                    'job summary', 'save job', 'apply',
                    'diversity & inclusion at michael page',
                    'other users applied', 'contact ', 'quote job ref', 'phone number'
                ]
                lines = []
                for line in text_joined.splitlines():
                    ln = line.strip()
                    low = ln.lower()
                    if not ln:
                        continue
                    if any(ph in low for ph in remove_phrases):
                        continue
                    # Skip consultant/contact metadata
                    if any(k in low for k in [
                        'consultant name', 'consultant phone', 'job reference',
                        'function', 'specialisation', "what is your industry?", 'location', 'job type'
                    ]):
                        continue
                    # Skip bare bullets that are just Save/Apply duplicates
                    if ln in ['- Save Job', '- Apply']:
                        continue
                    lines.append(ln)
                # Deduplicate globally while preserving order
                cleaned = []
                seen = set()
                for ln in lines:
                    n = re.sub(r'\s+', ' ', ln.strip().lower())
                    if n in seen:
                        continue
                    seen.add(n)
                    cleaned.append(ln)
                return '\n'.join(cleaned)

            # Try preferred selectors first
            for sel in preferred_selectors:
                container = soup.select_one(sel)
                text = collect_text(container)
                html = sanitize_container_to_html(container) if container else ""
                if text and len(text) > 200:  # ensure it's substantive
                    if html and len(BeautifulSoup(html, 'html.parser').get_text(" ", strip=True)) > 80:
                        meta = self.extract_job_page_meta(soup)
                        return html, text, meta
                    meta = self.extract_job_page_meta(soup)
                    return "" if not html else html, text, meta

            # Generic fallback: use the largest text block inside main/article
            candidates = soup.select('main, article, div[role="main"], div.content, div.region-content')
            best_text = ""
            best_html = ""
            for c in candidates:
                text = collect_text(c)
                html = sanitize_container_to_html(c)
                if len(text) > len(best_text):
                    best_text = text
                    best_html = html
            # Cut off at known tail boilerplates if still present
            tail_cuts = [
                'diversity & inclusion at michael page',
                'other users applied'
            ]
            bt_low = best_text.lower()
            cut_index = None
            for cut in tail_cuts:
                idx = bt_low.find(cut)
                if idx != -1:
                    cut_index = idx if cut_index is None else min(cut_index, idx)
            if cut_index is not None:
                best_text = best_text[:cut_index]
            # If we still don't have meaningful HTML, build minimal paragraphs/ul from text
            best_html_text = best_text.strip()
            html_built = ""
            if best_html:
                html_built = best_html
            elif best_html_text:
                lines = [ln.strip() for ln in best_html_text.splitlines() if ln.strip()]
                bullet_lines = [ln[2:].strip() for ln in lines if ln.startswith('- ')]
                non_bullets = [ln for ln in lines if not ln.startswith('- ')]
                html_parts = []
                if non_bullets:
                    html_parts.extend([f"<p>{re.sub(r'<[^>]+>', '', p)}</p>" for p in non_bullets])
                if bullet_lines:
                    html_parts.append('<ul>' + ''.join([f"<li>{re.sub(r'<[^>]+>', '', b)}</li>" for b in bullet_lines]) + '</ul>')
                html_built = '\n'.join(html_parts)
            meta = self.extract_job_page_meta(soup)
            return html_built, best_html_text, meta
        except Exception as e:
            logger.debug(f"Failed to fetch full description: {e}")
            return "", "", {}

    def extract_job_page_meta(self, soup):
        """Extract simple metadata from the job detail page (Job summary panel)."""
        meta = {'location': '', 'phone': '', 'email': ''}
        try:
            # Locate a section that contains 'Job summary'
            container = None
            for tag in soup.find_all(['section', 'div']):
                text = tag.get_text(' ', strip=True).lower()
                if 'job summary' in text and any(k in text for k in ['function', 'location', 'consultant', 'phone']):
                    container = tag
                    break
            if not container:
                return meta
            text = container.get_text(' ', strip=True)
            # Location
            m = re.search(r'Location\s*([A-Za-z0-9,\-/()\s]+?)(?=(Function|Specialisation|Job\s*Type|Consultant|Phone|Job\s*reference|What\'s on Offer|$))', text, re.I)
            if m:
                meta['location'] = m.group(1).strip()
            # Email
            m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
            if m:
                meta['email'] = m.group(0)
            # Phone
            m = re.search(r"\+?\d[\d\s()\-]{7,}\d", text)
            if m:
                meta['phone'] = m.group(0)
            # Fallbacks: sometimes contact phone appears at the bottom of the description
            # as a tel: link or a line labelled "Phone number" rather than inside Job summary.
            if not meta['phone']:
                # 1) Look for tel: links anywhere on the page
                try:
                    tel_links = soup.select('a[href^="tel:"]')
                    if tel_links:
                        tel_val = tel_links[-1].get('href', '')  # prefer the last tel link on the page
                        tel_val = re.sub(r'^tel:\s*', '', tel_val).strip()
                        if re.search(r"\+?\d[\d\s()\-]{7,}\d", tel_val):
                            meta['phone'] = re.search(r"\+?\d[\d\s()\-]{7,}\d", tel_val).group(0)
                except Exception:
                    pass
            if not meta['phone']:
                # 2) Scan whole page text for a labelled phone pattern; choose the last occurrence
                try:
                    full_text = soup.get_text(' ', strip=True)
                    # Prefer numbers that are explicitly labelled with Phone/Phone number
                    labelled_matches = list(re.finditer(r"(?:phone(?:\s*number)?\s*[:\-]?\s*)(\+?\d[\d\s()\-]{7,}\d)", full_text, re.I))
                    if labelled_matches:
                        meta['phone'] = labelled_matches[-1].group(1)
                    else:
                        # As a final fallback, pick the last phone-like number on the page
                        generic_matches = list(re.finditer(r"\+?\d[\d\s()\-]{7,}\d", full_text))
                        if generic_matches:
                            meta['phone'] = generic_matches[-1].group(0)
                except Exception:
                    pass
        finally:
            return meta

    def extract_skills_from_text(self, text, max_items=12):
        """Keyword fallback skill extractor from plain text only (broad).

        Returns tuple (skills_csv, preferred_csv).
        """
        if not text:
            return "", ""
        normalized = re.sub(r"[^a-z0-9\s\+\.#/&-]", " ", text.lower())
        skill_keywords = [
            'python', 'java', 'c#', 'c++', 'javascript', 'typescript', 'node', 'react', 'angular', 'vue',
            'django', 'flask', 'spring', 'dotnet', '.net', 'sql', 'mysql', 'postgresql', 'oracle',
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'linux', 'git', 'terraform',
            'excel', 'power bi', 'tableau', 'sap', 'salesforce', 'xero', 'netsuite',
            'project management', 'agile', 'scrum', 'jira', 'confluence',
            'communication', 'stakeholder management', 'leadership', 'problem solving',
            'customer service', 'food hygiene', 'safety', 'environmental', 'cleaning standards'
        ]
        found = []
        for kw in skill_keywords:
            pattern = r"\b" + re.escape(kw.replace('.', '\\.')) + r"\b"
            if re.search(pattern, normalized):
                found.append(kw)
        dedup = []
        seen = set()
        for kw in found:
            if kw not in seen:
                seen.add(kw)
                dedup.append(kw)
        if not dedup:
            return "", ""
        dedup = dedup[:max_items]
        # Split across both fields to maximize capacity
        skills_list = []
        preferred_list = []
        char_limit = 200
        # pack into skills first, then preferred
        for item in dedup:
            csv_try = (", ".join(skills_list + [item])).strip(', ')
            if len(csv_try) <= char_limit:
                skills_list.append(item)
            else:
                csv_try2 = (", ".join(preferred_list + [item])).strip(', ')
                if len(csv_try2) <= char_limit:
                    preferred_list.append(item)
        return ", ".join(skills_list), ", ".join(preferred_list)

    def extract_skills_from_description(self, html_description, plain_text):
        """Primary extractor: parse bullets under relevant headings and pack across both fields.

        We collect all <li> items under headings like 'The Successful Applicant',
        'Skills and Experience', 'Requirements', or 'Key Responsibilities'.
        Then we distribute them across `skills` and `preferred_skills` fields
        honoring each field's 200 character limit so we keep as much as possible.
        If no bullets are found, fall back to keyword extraction from plain text.
        """
        items = []
        try:
            if html_description:
                soup = BeautifulSoup(html_description, 'html.parser')
                target_headings = [
                    'the successful applicant', 'skills and experience', 'requirements',
                    'key responsibilities', 'responsibilities', 'your profile'
                ]
                headings = soup.find_all(['h2', 'h3'])
                for h in headings:
                    htxt = (h.get_text(strip=True) or '').lower()
                    if any(t in htxt for t in target_headings):
                        for sib in h.find_all_next():
                            if sib.name in ['h2', 'h3']:
                                break
                            if sib.name in ['ul', 'ol']:
                                for li in sib.find_all('li'):
                                    t = li.get_text(' ', strip=True)
                                    if t:
                                        items.append(t)
            # fallback scan of plain text for lines beginning with '- '
            if not items and plain_text:
                for ln in plain_text.splitlines():
                    ln = ln.strip()
                    if ln.startswith('- '):
                        items.append(ln[2:].strip())
        except Exception:
            pass

        # Deduplicate and pack into two CSVs within limits
        def pack(items_list):
            seen = set()
            unique = []
            for it in items_list:
                n = re.sub(r'\s+', ' ', it.strip())
                if not n:
                    continue
                if n.lower() in seen:
                    continue
                seen.add(n.lower())
                unique.append(n)
            char_limit = 200
            s1, s2 = [], []
            for it in unique:
                try1 = (', '.join(s1 + [it])).strip(', ')
                if len(try1) <= char_limit:
                    s1.append(it)
                else:
                    try2 = (', '.join(s2 + [it])).strip(', ')
                    if len(try2) <= char_limit:
                        s2.append(it)
                    else:
                        break
            return ', '.join(s1), ', '.join(s2)

        if items:
            return pack(items)
        # Fallback
        return self.extract_skills_from_text(plain_text or '')

    def fetch_company_details(self, target_city_hint=None):
        """Scrape Michael Page logo and a contact method from the website.

        Returns dict with keys: logo, email, phone, details_url, address_line1, city, state, postcode.
        """
        details = {
            'logo': '', 'email': '', 'phone': '', 'details_url': '',
            'address_line1': '', 'city': '', 'state': '', 'postcode': ''
        }
        # Try to get logo from home page
        try:
            resp = self.session.get(self.base_url, timeout=30)
            resp.raise_for_status()
            home = BeautifulSoup(resp.text, 'html.parser')
            logo_img = home.select_one('img[alt*="Michael Page" i]')
            if logo_img and logo_img.get('src'):
                details['logo'] = urljoin(self.base_url, logo_img['src'])
        except Exception:
            pass
        # Try to get contact details from contact page
        try:
            contact_url = urljoin(self.base_url, '/contact')
            resp = self.session.get(contact_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            details['details_url'] = contact_url
            # Find office cards
            office_cards = soup.select('div[class*="card"], div[class*="contact"], div[class*="office"], section div')
            selected = None
            target_city = (target_city_hint or '').lower()
            for card in office_cards:
                heading = card.find(['h2', 'h3'])
                heading_text = heading.get_text(strip=True) if heading else ''
                if not heading_text:
                    continue
                ht_low = heading_text.lower()
                if target_city and target_city in ht_low:
                    selected = card
                    break
                if 'sydney' in ht_low and selected is None:
                    selected = card  # default to Sydney if nothing better
            selected = selected or (office_cards[0] if office_cards else None)
            if selected:
                # email and phone patterns
                text = selected.get_text(" ", strip=True)
                email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
                phone_match = re.search(r"\+?\d[\d\s()\-]{7,}\d", text)
                if email_match:
                    details['email'] = email_match.group(0)
                if phone_match:
                    details['phone'] = phone_match.group(0)
                # Follow details link if present to fetch street address
                link = selected.find('a', href=True)
                if link:
                    details['details_url'] = urljoin(self.base_url, link['href'])
                    try:
                        r2 = self.session.get(details['details_url'], timeout=30)
                        r2.raise_for_status()
                        s2 = BeautifulSoup(r2.text, 'html.parser')
                        addr = s2.find('address') or s2.select_one('[class*="address" i]')
                        if addr:
                            addr_text = addr.get_text(" ", strip=True)
                            details['address_line1'] = addr_text
                    except Exception:
                        pass
        except Exception:
            pass
        return details

    def parse_location(self, location_string):
        """Parse location string into normalized location data."""
        if not location_string:
            return None, "", "", "Australia"
            
        location_string = location_string.strip()
        
        # Australian state abbreviations and full names
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
        
        # Split by comma or other delimiters
        parts = [part.strip() for part in re.split(r'[,\-]', location_string)]
        
        city = ""
        state = ""
        country = "Australia"
        
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1]
            # Check if state part contains a known state abbreviation
            for abbrev, full_name in states.items():
                if abbrev in state_part.upper():
                    state = full_name
                    break
            else:
                # Look for full state names
                for abbrev, full_name in states.items():
                    if full_name.lower() in state_part.lower():
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
        """Parse salary information into structured data."""
        if not salary_text:
            return None, None, "AUD", "yearly", ""
            
        salary_text = salary_text.strip()
        
        # Common patterns for salary extraction
        patterns = [
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*-\s*AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'AU\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'\$(\d{1,3}(?:,\d{3})*)\s*per\s*(year|month|week|day|hour)',
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80-100k"
            r'(\d{1,3}(?:,\d{3})*)\s*k',  # e.g., "80k"
        ]
        
        salary_min = None
        salary_max = None
        currency = "AUD"
        salary_type = "yearly"
        
        for pattern in patterns:
            match = re.search(pattern, salary_text.lower().replace(',', ''))
            if match:
                groups = match.groups()
                if len(groups) == 3:  # Range with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    salary_max = Decimal(groups[1].replace(',', ''))
                    salary_type = groups[2]
                    break
                elif len(groups) == 2 and 'k' in salary_text.lower():  # Range in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_max = Decimal(groups[1].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
                elif len(groups) == 2:  # Single amount with period
                    salary_min = Decimal(groups[0].replace(',', ''))
                    salary_type = groups[1]
                    break
                elif len(groups) == 1 and 'k' in salary_text.lower():  # Single amount in thousands
                    salary_min = Decimal(groups[0].replace(',', '')) * 1000
                    salary_type = "yearly"
                    break
        
        return salary_min, salary_max, currency, salary_type, salary_text
    
    def extract_jobs_from_html(self, html_content):
        """Extract job data from HTML content using BeautifulSoup."""
        jobs = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Based on the HTML structure you provided, find job tiles specifically
            job_tiles = soup.find_all('div', class_='job-tile')
            
            logger.info(f"Found {len(job_tiles)} job tiles in the HTML")
            
            for tile in job_tiles:
                job_data = self.extract_job_from_tile(tile)
                if job_data:
                    jobs.append(job_data)
            
            # Fallback method if job tiles not found
            if not jobs:
                logger.warning("No job tiles found, trying alternative methods...")
                
                # Look for list items with views-row class (based on your HTML)
                job_rows = soup.find_all('li', class_='views-row')
                for row in job_rows:
                    job_data = self.extract_job_from_row(row)
                    if job_data:
                        jobs.append(job_data)
            
            # Remove duplicates based on job_url
            seen_urls = set()
            unique_jobs = []
            for job in jobs:
                if job['job_url'] not in seen_urls:
                    seen_urls.add(job['job_url'])
                    unique_jobs.append(job)
            
            logger.info(f"Extracted {len(unique_jobs)} unique jobs from HTML")
            return unique_jobs if self.job_limit is None else unique_jobs[:self.job_limit]
            
        except Exception as e:
            logger.error(f"Error extracting jobs from HTML: {str(e)}")
            return []
    
    def extract_job_from_tile(self, tile):
        """Extract job data from a job-tile div element based on the provided HTML structure."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Extract job title and URL from h3 > a
            title_element = tile.find('h3')
            if title_element:
                title_link = title_element.find('a')
                if title_link:
                    job_data['job_title'] = title_link.get_text(strip=True)
                    href = title_link.get('href')
                    if href:
                        job_data['job_url'] = urljoin(self.base_url, href)
            
            # Extract location from job-location div
            location_element = tile.find('div', class_='job-location')
            if location_element:
                job_data['location_text'] = location_element.get_text(strip=True).replace('', '').strip()
            
            # Extract salary from job-salary div
            salary_element = tile.find('div', class_='job-salary')
            if salary_element:
                job_data['salary_text'] = salary_element.get_text(strip=True).replace('', '').strip()
            
            # Extract job type from job-contract-type div
            contract_element = tile.find('div', class_='job-contract-type')
            if contract_element:
                # Remove icon and get clean text
                contract_text = contract_element.get_text(strip=True).replace('', '').strip()
                # Clean up any extra whitespace and normalize
                if contract_text:
                    # Remove common icon characters and clean up
                    contract_clean = contract_text.replace('🕒', '').replace('⏰', '').strip()
                    if contract_clean:
                        job_data['keywords'].append(contract_clean)
            
            # Extract work mode from job-nature div
            nature_element = tile.find('div', class_='job-nature')
            if nature_element:
                nature_text = nature_element.get_text(strip=True).replace('', '').strip()
                job_data['keywords'].append(nature_text)
            
            # Extract summary from job-summary div
            summary_element = tile.find('div', class_='job-summary')
            if summary_element:
                summary_text_elem = summary_element.find('div', class_='job_advert__job-summary-text')
                if summary_text_elem:
                    job_data['summary'] = summary_text_elem.get_text(strip=True)
            
            # Extract bullet points
            bullet_element = tile.find('div', class_='bullet_points')
            if bullet_element:
                bullet_list = bullet_element.find('ul')
                if bullet_list:
                    bullets = [li.get_text(strip=True) for li in bullet_list.find_all('li')]
                    job_data['keywords'].extend(bullets)
            
            # Only return if we have at least a title and URL
            if job_data['job_title'] and job_data['job_url']:
                logger.info(f"[EXTRACTED] {job_data['job_title']}")
                logger.info(f"   Location: {job_data['location_text']}")
                logger.info(f"   Salary: {job_data['salary_text']}")
                logger.info(f"   Keywords: {job_data['keywords']}")
                return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from tile: {str(e)}")
        
        return None

    def extract_job_from_row(self, row):
        """Extract job data from a views-row li element based on the provided HTML structure."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Skip job alert rows
            if row.find('div', class_='job-alert-wrap'):
                return None
            
            # Find the job-tile div within the row
            job_tile = row.find('div', class_='job-tile')
            if job_tile:
                return self.extract_job_from_tile(job_tile)
            
        except Exception as e:
            logger.error(f"Error extracting job from row: {str(e)}")
        
        return None

    def extract_job_from_container(self, container):
        """Extract job data from a job container element."""
        try:
            job_data = {
                'job_title': '',
                'job_url': '',
                'company_name': 'Michael Page',
                'location_text': '',
                'summary': '',
                'salary_text': '',
                'posted_ago': '',
                'badges': [],
                'keywords': []
            }
            
            # Find job title and URL
            title_link = container.find('a', href=True)
            if title_link:
                job_data['job_title'] = title_link.get_text(strip=True)
                job_data['job_url'] = urljoin(self.base_url, title_link['href'])
            
            # Find location if available
            location_element = container.find(class_=re.compile(r'location', re.I))
            if location_element:
                job_data['location_text'] = location_element.get_text(strip=True)
            
            # Find description/summary
            desc_element = container.find(class_=re.compile(r'description|summary', re.I))
            if desc_element:
                job_data['summary'] = desc_element.get_text(strip=True)
            
            # Find salary information
            salary_element = container.find(class_=re.compile(r'salary|pay|wage', re.I))
            if salary_element:
                job_data['salary_text'] = salary_element.get_text(strip=True)
            
            # Only return if we have at least a title and URL
            if job_data['job_title'] and job_data['job_url']:
                return job_data
            
        except Exception as e:
            logger.error(f"Error extracting job from container: {str(e)}")
        
        return None
    
    def save_job_to_database_sync(self, job_data):
        """Synchronous database save function."""
        try:
            connections.close_all()
            
            with transaction.atomic():
                # Check for duplicates
                job_url = job_data['job_url']
                job_title = job_data['job_title']
                company_name = job_data['company_name']
                
                if job_url and JobPosting.objects.filter(external_url=job_url).exists():
                    logger.info(f"[DUPLICATE SKIPPED] (URL): {job_title}")
                    self.duplicate_count += 1
                    return False
                
                if JobPosting.objects.filter(title=job_title, company__name=company_name).exists():
                    logger.info(f"[DUPLICATE SKIPPED] (Title+Company): {job_title}")
                    self.duplicate_count += 1
                    return False
                
                # Create location
                location_name, city, state, country = self.parse_location(job_data.get('location_text', ''))
                location_obj = None
                if location_name:
                    location_obj, created = Location.objects.get_or_create(
                        name=location_name,
                        defaults={'city': city, 'state': state, 'country': country}
                    )
                
                # Create company
                company_slug = slugify(company_name)
                company_obj, created = Company.objects.get_or_create(
                    slug=company_slug,
                    defaults={
                        'name': company_name,
                        'description': f'{company_name} - Jobs from Michael Page Australia',
                        'website': 'https://www.michaelpage.com.au',
                        'company_size': 'large'
                    }
                )
                # Enrich company with logo and contact if missing
                try:
                    needs_update = any([
                        not company_obj.logo,
                        not company_obj.email,
                        not company_obj.phone,
                        not company_obj.address_line1,
                    ])
                    if needs_update:
                        _, city, state, _ = self.parse_location(job_data.get('location_text', ''))
                        info = self.fetch_company_details(target_city_hint=city or state)
                        updated = False
                        if info.get('logo') and not company_obj.logo:
                            company_obj.logo = info['logo']
                            updated = True
                        if info.get('email') and not company_obj.email:
                            company_obj.email = info['email']
                            updated = True
                        if info.get('phone') and not company_obj.phone:
                            company_obj.phone = info['phone']
                            updated = True
                        if info.get('address_line1') and not company_obj.address_line1:
                            company_obj.address_line1 = info['address_line1']
                            updated = True
                        if info.get('details_url') and not company_obj.details_url:
                            company_obj.details_url = info['details_url']
                            updated = True
                        if updated:
                            company_obj.save(update_fields=['logo', 'email', 'phone', 'address_line1', 'details_url', 'updated_at'])
                except Exception as e:
                    logger.debug(f"Could not enrich company details: {e}")
                
                # Parse salary
                salary_min, salary_max, currency, salary_type, raw_text = self.parse_salary(
                    job_data.get('salary_text', '')
                )
                
                # Parse date
                date_posted = self.parse_date(job_data.get('posted_ago', ''))
                
                # Determine job details from keywords
                job_type = "full_time"  # Default
                work_mode = ""
                experience_level = ""
                
                keywords = job_data.get('keywords', [])
                logger.info(f"[PROCESSING KEYWORDS] for '{job_data.get('job_title', '')}': {keywords}")
                
                for keyword in keywords:
                    keyword_lower = keyword.lower().strip()
                    
                    # Map website job types to database job types
                    if keyword_lower == 'permanent':
                        job_type = "permanent"  # Keep as permanent instead of converting to full_time
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif keyword_lower == 'temporary':
                        job_type = "temporary"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif keyword_lower == 'contract':
                        job_type = "contract"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'part-time' in keyword_lower or 'part time' in keyword_lower:
                        job_type = "part_time"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'casual' in keyword_lower:
                        job_type = "casual"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'internship' in keyword_lower:
                        job_type = "internship"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    elif 'freelance' in keyword_lower:
                        job_type = "freelance"
                        logger.info(f"   [JOB_TYPE] Set job_type: {job_type} (from: {keyword})")
                    # Work modes
                    elif 'hybrid' in keyword_lower or 'work from home' in keyword_lower or 'remote' in keyword_lower:
                        work_mode = keyword
                        logger.info(f"   [WORK_MODE] Set work_mode: {work_mode}")
                    # Experience levels
                    elif any(level in keyword_lower for level in ['senior', 'junior', 'graduate', 'executive', 'lead', 'manager']):
                        experience_level = keyword
                        logger.info(f"   [EXPERIENCE] Set experience_level: {experience_level}")
                
                # Automatic job categorization
                job_category = JobCategorizationService.categorize_job(
                    title=job_data.get('job_title', ''),
                    description=BeautifulSoup(job_data.get('summary_html', '') or job_data.get('summary', ''), 'html.parser').get_text(' ', strip=True)
                )
                
                # Create unique slug
                base_slug = slugify(job_data.get('job_title', 'job'))
                unique_slug = base_slug
                counter = 1
                while JobPosting.objects.filter(slug=unique_slug).exists():
                    unique_slug = f"{base_slug}-{counter}"
                    counter += 1
                
                # Create the JobPosting
                # Extract skills/preferred skills from description (prefer bullet items under headings)
                plain_text_for_skills = BeautifulSoup(job_data.get('summary_html', '') or job_data.get('summary', ''), 'html.parser').get_text(' ', strip=True)
                skills_csv, preferred_csv = self.extract_skills_from_description(job_data.get('summary_html', ''), plain_text_for_skills)

                job_posting = JobPosting.objects.create(
                    title=job_data.get('job_title', ''),
                    slug=unique_slug,
                    description=job_data.get('summary_html', '') or job_data.get('summary', 'No description available'),
                    company=company_obj,
                    posted_by=self.system_user,
                    location=location_obj,
                    job_category=job_category,
                    job_type=job_type,
                    experience_level=experience_level,
                    work_mode=work_mode,
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=currency,
                    salary_type=salary_type,
                    salary_raw_text=raw_text,
                    external_source='michaelpage.com.au',
                    external_url=job_data.get('job_url', ''),
                    status='active',
                    posted_ago=job_data.get('posted_ago', ''),
                    date_posted=date_posted,
                    additional_info=job_data,
                    skills=skills_csv,
                    preferred_skills=preferred_csv
                )
                
                logger.info(f"[SAVED TO DATABASE]")
                logger.info(f"   Title: {job_posting.title}")
                logger.info(f"   Company: {job_posting.company.name}")
                logger.info(f"   Location: {job_posting.location.name if job_posting.location else 'Not specified'}")
                logger.info(f"   Job Type: {job_posting.job_type}")
                logger.info(f"   Work Mode: {job_posting.work_mode}")
                logger.info(f"   Salary: {job_posting.salary_display}")
                logger.info(f"   Category: {job_posting.job_category}")
                logger.info(f"   URL: {job_posting.external_url}")
                self.scraped_count += 1
                return True
                
        except Exception as e:
            logger.error(f"Error saving job to database: {str(e)}")
            self.error_count += 1
            return False
    
    def extract_pagination_url(self, html_content):
        """Extract the next page URL from the 'Show more Jobs' pagination.
        
        Based on the HTML structure:
        <ul class="js-pager__items pager__items pager-show-more">
            <li class="pager__item">
                <a href="/jobs?page=1" title="Show more" rel="next">Show more Jobs</a>
            </li>
        </ul>
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Primary method: Look for the exact pagination structure from Michael Page
            pager_container = soup.find('ul', class_='js-pager__items pager__items pager-show-more')
            if pager_container:
                pager_item = pager_container.find('li', class_='pager__item')
                if pager_item:
                    show_more_link = pager_item.find('a', href=True)
                    if show_more_link:
                        # Check if it's the correct "Show more Jobs" link
                        link_text = show_more_link.get_text(strip=True)
                        if 'Show more' in link_text and 'Jobs' in link_text:
                            next_url = show_more_link['href']
                            # Convert relative URL to absolute URL
                            if next_url.startswith('/'):
                                next_url = urljoin(self.base_url, next_url)
                            logger.info(f"Found pagination URL (primary method): {next_url}")
                            return next_url
            
            # Secondary method: Look for any "Show more Jobs" link
            show_more_links = soup.find_all('a', href=True)
            for link in show_more_links:
                link_text = link.get_text(strip=True)
                if 'Show more' in link_text and 'Jobs' in link_text:
                    next_url = link['href']
                    if next_url.startswith('/'):
                        next_url = urljoin(self.base_url, next_url)
                    logger.info(f"Found pagination URL (secondary method): {next_url}")
                    return next_url
            
            # Fallback pagination patterns
            pagination_selectors = [
                'a[rel="next"]',
                'a[title*="Show more"]',
                '.pager-show-more a',
                '.js-pager__items a',
                'a[href*="page="]'
            ]
            
            for selector in pagination_selectors:
                next_link = soup.select_one(selector)
                if next_link and next_link.get('href'):
                    next_url = next_link['href']
                    if next_url.startswith('/'):
                        next_url = urljoin(self.base_url, next_url)
                    logger.info(f"Found pagination URL (fallback): {next_url}")
                    return next_url
            
            logger.info("No pagination URL found")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting pagination URL: {str(e)}")
            return None
    
    def debug_pagination_structure(self, html_content):
        """Debug method to understand the pagination structure."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for any pagination-related elements
            pagination_elements = []
            
            # Check for common pagination classes and IDs
            common_selectors = [
                'ul[class*="pager"]',
                'div[class*="pager"]',
                'nav[class*="pagination"]',
                'div[class*="pagination"]',
                'ul[class*="pagination"]',
                '[class*="show-more"]',
                '[class*="load-more"]',
                'a[rel="next"]',
                'a[href*="page="]'
            ]
            
            for selector in common_selectors:
                elements = soup.select(selector)
                for elem in elements:
                    pagination_elements.append({
                        'selector': selector,
                        'element': str(elem)[:200] + '...' if len(str(elem)) > 200 else str(elem),
                        'text': elem.get_text(strip=True)[:100]
                    })
            
            if pagination_elements:
                logger.debug(f"Found {len(pagination_elements)} pagination-related elements:")
                for i, elem in enumerate(pagination_elements[:5]):  # Limit to first 5
                    logger.debug(f"  {i+1}. Selector: {elem['selector']}")
                    logger.debug(f"     Text: {elem['text']}")
                    logger.debug(f"     HTML: {elem['element']}")
            else:
                logger.debug("No pagination elements found in HTML")
                
        except Exception as e:
            logger.debug(f"Error in debug_pagination_structure: {str(e)}")
    
    def run(self):
        """Main method to run the scraping process with pagination support."""
        logger.info("Starting Simple Michael Page Australia job scraper...")
        logger.info(f"Job limit: {self.job_limit or 'No limit'}")
        logger.info("Note: Now supports pagination with 'Show more Jobs' functionality")
        
        try:
            current_url = "https://www.michaelpage.com.au/jobs"
            page_number = 0
            total_jobs_processed = 0
            
            while current_url and (self.job_limit is None or self.scraped_count < self.job_limit):
                page_number += 1
                logger.info(f"Fetching page {page_number}: {current_url}")
                
                # Add delay between page requests
                if page_number > 1:
                    self.human_delay(1, 2)  # Longer delay between pages
                
                response = self.session.get(current_url, timeout=30)
                response.raise_for_status()
                
                logger.info(f"Successfully fetched page {page_number} (status: {response.status_code})")
                
                # Extract jobs from the HTML
                jobs = self.extract_jobs_from_html(response.text)
                
                if not jobs:
                    logger.warning(f"No jobs found on page {page_number}")
                    break
                
                logger.info(f"Found {len(jobs)} jobs on page {page_number}")
                total_jobs_processed += len(jobs)
                
                # Process jobs from current page
                jobs_saved_this_page = 0
                for i, job_data in enumerate(jobs):
                    if self.job_limit is not None and self.scraped_count >= self.job_limit:
                        logger.info(f"Reached job limit of {self.job_limit}")
                        break
                    
                    logger.info(f"Processing job {i+1}/{len(jobs)} from page {page_number}: {job_data['job_title']}")
                    
                    # Quick duplicate check before processing (saves time)
                    job_url = job_data.get('job_url', '')
                    job_title = job_data.get('job_title', '')
                    if job_url:
                        try:
                            if JobPosting.objects.filter(external_url=job_url).exists():
                                logger.info(f"DUPLICATE SKIPPED (Quick Check): {job_title}")
                                self.duplicate_count += 1
                                continue
                        except Exception as e:
                            logger.debug(f"Quick duplicate check failed: {e}")
                            pass  # Continue with normal processing if quick check fails

                    # Enrich summary with full description (HTML + text) from the detail page
                    try:
                        html_desc, full_text, meta = self.fetch_full_description_html_and_text(job_url)
                        if html_desc or full_text:
                            # Store HTML for description and keep plain text as backup
                            if html_desc:
                                job_data['summary_html'] = html_desc
                            if full_text:
                                job_data['summary'] = full_text
                            # Capture any meta (more accurate location/phone/email)
                            # Respect request to ignore Job summary section for description,
                            # but it's safe to use its location/contact for data accuracy.
                            if meta.get('location'):
                                job_data['location_text'] = meta['location']
                            if meta.get('phone'):
                                job_data['contact_phone'] = meta['phone']
                            if meta.get('email'):
                                job_data['contact_email'] = meta['email']
                    except Exception as e:
                        logger.debug(f"Could not enrich description: {e}")
                    
                    if self.save_job_to_database_sync(job_data):
                        jobs_saved_this_page += 1
                    
                    # Add minimal delay between saves
                    self.human_delay(0.1, 0.3)
                
                logger.info(f"Page {page_number} completed: {jobs_saved_this_page} jobs saved")
                
                # Check if we've reached the limit
                if self.job_limit is not None and self.scraped_count >= self.job_limit:
                    logger.info(f"Reached job limit of {self.job_limit}, stopping pagination")
                    break
                
                # Extract next page URL for pagination
                # Debug: Log pagination structure for troubleshooting
                if page_number <= 2:  # Only debug first couple pages
                    self.debug_pagination_structure(response.text)
                next_url = self.extract_pagination_url(response.text)
                if next_url and next_url != current_url:
                    current_url = next_url
                    logger.info(f"Moving to next page: {current_url}")
                    
                    # Safety check to prevent infinite loops
                    if page_number > 50:  # Reasonable limit
                        logger.warning(f"Safety limit reached ({page_number} pages), stopping pagination")
                        break
                else:
                    logger.info("No more pages found or reached the end of pagination")
                    break
            
            # Final statistics
            logger.info("="*50)
            logger.info("MICHAEL PAGE SCRAPING COMPLETED!")
            logger.info(f"Pages scraped: {page_number}")
            logger.info(f"Total jobs found: {total_jobs_processed}")
            logger.info(f"Jobs saved to database: {self.scraped_count}")
            logger.info(f"Duplicate jobs skipped: {self.duplicate_count}")
            logger.info(f"Errors encountered: {self.error_count}")
            
            try:
                total_jobs_in_db = JobPosting.objects.count()
                logger.info(f"Total job postings in database: {total_jobs_in_db}")
            except:
                logger.info("Total job postings in database: (count unavailable)")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise


def main():
    """Main function to run the simple scraper."""
    print("🔍 Simple Michael Page Australia Job Scraper")
    print("="*50)
    
    # Parse command line arguments
    max_jobs = None  # Default (unlimited)
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            print("Invalid number of jobs. Using unlimited.")
    
    print(f"Target: {max_jobs} jobs from Michael Page Australia")
    print("Method: Direct HTML parsing with 'Show more Jobs' pagination support")
    print("Database: Professional structure with JobPosting, Company, Location")
    print("="*50)
    
    # Create scraper instance
    scraper = SimpleMichaelPageScraper(job_limit=max_jobs)
    
    try:
        # Run the scraping process
        scraper.run()
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        raise


def run(job_limit=None):
    """Automation entrypoint for Michael Page simple scraper.

    Creates the scraper and runs it without CLI args; returns a summary dict.
    """
    try:
        scraper = SimpleMichaelPageScraper(job_limit=job_limit)
        scraper.run()
        return {
            'success': True,
            'jobs_scraped': scraper.scraped_count,
            'duplicate_count': scraper.duplicate_count,
            'error_count': scraper.error_count,
            'message': f'Successfully scraped {scraper.scraped_count} Michael Page jobs'
        }
    except Exception as e:
        logger.error(f"Scraping failed in run(): {e}")
        return {
            'success': False,
            'error': str(e),
            'message': f'Scraping failed: {e}'
        }

if __name__ == "__main__":
    main()
