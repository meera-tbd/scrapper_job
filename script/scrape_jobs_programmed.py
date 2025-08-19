#!/usr/bin/env python
"""
Programmed (PERSOL Programmed) Job Scraper using Playwright

Scrapes job detail pages from `https://www.jobs.programmed.com.au/jobs/`,
captures original description, salary, location, work type and category,
and stores them in the Django `JobPosting` model.

Usage:
  python script/scrape_jobs_programmed.py [max_jobs]
"""

import os
import sys
import re
import time
import random
import logging
from urllib.parse import urljoin, urlparse

# Django setup (same convention as other scrapers in this repo)
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
        logging.FileHandler('scraper_programmed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class ProgrammedScraper:
    """Playwright-based scraper for Programmed job postings."""

    def __init__(self, max_jobs: int | None = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://www.jobs.programmed.com.au"
        self.search_url = f"{self.base_url}/jobs/"
        self.company: Company | None = None
        self.scraper_user: User | None = None
        self.scraped_count = 0

    def human_like_delay(self, min_s=0.8, max_s=2.0):
        time.sleep(random.uniform(min_s, max_s))

    def setup_database_objects(self):
        self.company, _ = Company.objects.get_or_create(
            name="Programmed",
            defaults={
                'description': "Programmed | PERSOL Programmed",
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': ''
            }
        )
        self.scraper_user, _ = User.objects.get_or_create(
            username='programmed_scraper',
            defaults={
                'email': 'scraper@programmed.local',
                'first_name': 'Programmed',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: str | None) -> Location | None:
        """Return an existing `Location` or create one from a noisy string.

        Accepts inputs that may contain extra UI text. Extracts a best-effort
        "City, State" or "City STATE [postcode]" and expands state abbreviations.
        """
        if not location_text:
            return None
        text = (location_text or '').strip()
        if not text:
            return None

        # Remove obvious UI noise that sometimes leaks into the location value
        lowered = text.lower()
        noise = (
            'see more results', 'similar jobs', 'recommended jobs', 'more jobs',
            'register', 'visit faqs', 'apply now', 'share', 'print'
        )
        for token in noise:
            lowered = lowered.replace(token, ' ')
        text = re.sub(r'\s+', ' ', lowered).strip()

        # Helper to nice-case a city
        def titleish(s: str) -> str:
            return ' '.join(w.capitalize() for w in re.split(r'\s+', s) if w)

        # Abbreviation expansion
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

        city = ''
        state = ''

        # 1) City, Full State
        m = re.search(r"([a-z][a-z\-\s']+),\s*(new south wales|victoria|queensland|south australia|western australia|tasmania|northern territory|australian capital territory)\b", text, re.IGNORECASE)
        if m:
            city = titleish(m.group(1))
            state = titleish(m.group(2))
        # 2) City STATE [postcode]
        if not city:
            m = re.search(r"([a-z][a-z\-\s']+)\s+(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b(?:\s+\d{4})?", text, re.IGNORECASE)
            if m:
                city = titleish(m.group(1))
                state = abbrev.get(m.group(2).upper(), m.group(2).upper())
        # 3) STATE - City
        if not city:
            m = re.search(r"(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\s*-\s*([a-z][a-z\-\s']+)", text, re.IGNORECASE)
            if m:
                city = titleish(m.group(2))
                state = abbrev.get(m.group(1).upper(), m.group(1).upper())
        # 4) Parenthetical hint e.g. (Epping, VIC)
        if not city:
            m = re.search(r"\(([^)]+)\)", text)
            if m:
                inner = m.group(1)
                # Re-run 1/2 on inner
                m1 = re.search(r"([A-Za-z\-\s']+),\s*(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)", inner, re.IGNORECASE)
                if m1:
                    city = titleish(m1.group(1))
                    state = abbrev.get(m1.group(2).upper(), m1.group(2).upper())

        # 5) Bare state or city
        if not city and not state and text:
            tok = text.strip()
            st = abbrev.get(tok.upper())
            if st:
                state = st
            else:
                city = titleish(tok)

        # Build canonical name
        if state:
            # Expand any residual short form in state
            for k, v in abbrev.items():
                if re.search(rf"\b{k}\b", state, re.IGNORECASE):
                    state = v
                    break
        name = f"{city}, {state}" if city and state else (state or city)
        if not name:
            return None

        location, _ = Location.objects.get_or_create(
            name=name,
            defaults={'city': city, 'state': state, 'country': 'Australia'}
        )
        return location

    def normalize_job_type(self, text: str | None) -> str:
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

    def parse_salary(self, raw: str | None) -> dict:
        """Parse salary amounts robustly and avoid picking postcodes or unrelated numbers.

        Strategy:
        - Prefer tokens with a currency symbol (AU$ or $) near them
        - Support ranges and single values, with optional period suffix (/hr, per year, etc.)
        - If no explicit money tokens, leave numeric fields empty and only keep raw text when safe
        """
        result = {
            'salary_min': None,
            'salary_max': None,
            'salary_type': 'yearly',
            'salary_currency': 'AUD',
            'salary_raw_text': ''
        }
        if not raw:
            return result
        text = ' '.join(raw.strip().split())

        # Determine cadence from keywords
        if re.search(r'(hour|/\s*hr|\bhr\b|hourly)', text, re.IGNORECASE):
            result['salary_type'] = 'hourly'
        elif re.search(r'(week|weekly|/\s*w[k]?)', text, re.IGNORECASE):
            result['salary_type'] = 'weekly'
        elif re.search(r'(month|monthly|/\s*mo[n]?)', text, re.IGNORECASE):
            result['salary_type'] = 'monthly'
        elif re.search(r'(year|annum|pa|p\.a\.|annually|/\s*y[r]?)', text, re.IGNORECASE):
            result['salary_type'] = 'yearly'

        # Patterns that include currency symbols
        currency_num = r'(?:AU\$|\$)\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)'
        range_pat = re.compile(rf'{currency_num}\s*[-–]\s*{currency_num}', re.IGNORECASE)
        single_pat = re.compile(currency_num, re.IGNORECASE)

        m = range_pat.search(text)
        if m:
            lo = float(m.group(1).replace(',', ''))
            hi = float(m.group(2).replace(',', ''))
            result['salary_min'] = min(lo, hi)
            result['salary_max'] = max(lo, hi)
            result['salary_raw_text'] = m.group(0)
            return result

        # Fallback to a single currency value
        m = single_pat.search(text)
        if m:
            val = float(m.group(1).replace(',', ''))
            result['salary_min'] = val
            result['salary_max'] = val
            result['salary_raw_text'] = m.group(0)
            return result

        # As a last resort, scan for numbers followed closely by per-period tokens (still avoid postcodes)
        near_period_pat = re.compile(r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)\s*(?:per\s+(hour|week|month|year|annum)|/\s*(hr|wk|mo|yr))', re.IGNORECASE)
        m = near_period_pat.search(text)
        if m:
            val = float(m.group(1).replace(',', ''))
            result['salary_min'] = val
            result['salary_max'] = val
            result['salary_raw_text'] = m.group(0)
            return result

        # No confident salary token
        return result

    def ensure_category_choice(self, display_text: str) -> str:
        if not display_text:
            return 'other'
        key = slugify(display_text).replace('-', '_')[:50] or 'other'
        if not any(choice[0] == key for choice in JobPosting.JOB_CATEGORY_CHOICES):
            JobPosting.JOB_CATEGORY_CHOICES.append((key, display_text.strip()))
        return key

    def map_category(self, raw: str | None) -> str:
        if not raw:
            return 'other'
        t = raw.strip().lower().replace('&amp;', '&')
        t = re.sub(r'\s+', ' ', t)
        mapping = {
            'transport & logistics': 'transport_logistics',
            'warehouse & distribution': 'transport_logistics',
            'manufacturing & fmcg': 'manufacturing',
            'retail & commercial': 'retail',
            'health, aged & community care': 'healthcare',
            'mining & gas': 'mining_resources',
            'trades': 'construction',
            'business support': 'office_support',
            'telecommunications': 'technology',
        }
        if t in mapping:
            return mapping[t]
        # Fallback to dynamic choice then service categorization
        return self.ensure_category_choice(JobCategorizationService.normalize_display_category(raw))

    def extract_job_links_from_search(self, page) -> list[str]:
        links = set()
        
        def collect_links_current_page() -> None:
            """Collect job detail links from the current DOM, with a small scroll."""
            last_height_local = 0
            for _ in range(10):
                anchors_local = page.query_selector_all('a[href]')
                for a_local in anchors_local:
                    href_local = a_local.get_attribute('href') or ''
                    low_local = href_local.lower()
                    if not href_local or low_local.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    if '/jobview/' in low_local or '/jobview' in low_local:
                        full_local = href_local if low_local.startswith('http') else urljoin(self.base_url, href_local)
                        links.add(full_local)
                # Gentle scroll to reveal any lazy content
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    break
                self.human_like_delay(0.5, 0.9)
                try:
                    new_h_local = page.evaluate("() => document.body.scrollHeight")
                    if new_h_local == last_height_local:
                        break
                    last_height_local = new_h_local
                except Exception:
                    break

        try:
            # Page 1
            page.goto(self.search_url, wait_until="domcontentloaded", timeout=35000)
            self.human_like_delay(1.0, 1.8)
            collect_links_current_page()

            # If the UI has a Next control, try it first (covers accessibility-only arrows)
            tried_next_clicking = False
            for _ in range(3):
                if self.max_jobs and len(links) >= self.max_jobs:
                    break
                try:
                    next_btn = page.locator('a[aria-label="Next"], button[aria-label="Next"], a:has-text("Next"), button:has-text("Next")').first
                    if not next_btn or not next_btn.is_visible():
                        break
                    before_click = len(links)
                    next_btn.click(timeout=2500)
                    tried_next_clicking = True
                    self.human_like_delay(0.8, 1.4)
                    page.wait_for_load_state('domcontentloaded', timeout=6000)
                    collect_links_current_page()
                    if len(links) == before_click:
                        break
                except Exception:
                    break

            # Fallback: explicitly visit numeric pagination URLs /jobs/page_2/, /page_3/, ...
            # This site uses 20 results per page; keep going until no new links or max requested reached.
            page_index = 2
            consecutive_empty_pages = 0
            while (not self.max_jobs or len(links) < self.max_jobs) and consecutive_empty_pages < 2:
                try:
                    paged_url = f"{self.base_url}/jobs/page_{page_index}/"
                    page.goto(paged_url, wait_until="domcontentloaded", timeout=35000)
                    self.human_like_delay(0.9, 1.5)
                    before_len = len(links)
                    collect_links_current_page()
                    if len(links) == before_len:
                        consecutive_empty_pages += 1
                    else:
                        consecutive_empty_pages = 0
                    page_index += 1
                except Exception:
                    break
        except Exception as e:
            logger.warning(f"Search extraction warning: {e}")
        return list(sorted(links))

    def extract_field_by_label(self, body_text: str, label: str, stop_labels: list[str]) -> str:
        pattern = rf"{re.escape(label)}\s*:\s*(.*?)\s*(?:\n|$|" + "|".join(map(re.escape, stop_labels)) + ")"
        m = re.search(pattern, body_text, re.IGNORECASE)
        if m:
            return re.sub(r'\s+', ' ', m.group(1).strip())
        # Fallback to line starting with label
        pattern2 = rf"^\s*{re.escape(label)}\s*(?:-|:)\s*(.+)$"
        for ln in body_text.splitlines():
            m2 = re.search(pattern2, ln, re.IGNORECASE)
            if m2:
                return re.sub(r'\s+', ' ', m2.group(1).strip())
        return ''

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
        lines = [ln.strip() for ln in text.split('\n')]
        cleaned = []
        drop_exact = {
            'Apply Now', 'Save', 'Share', 'Print', 'Share via', 'Email', 'Post', 'Tweet',
            'Register', 'Visit FAQs', 'See more results'
        }
        drop_contains = (
            'Ready to make your next move? Apply now',
            'To learn more about life at PERSOL',
            'follow us on LinkedIn',
            'Reach your potential',
            'We’re finding workers for hundreds of immediately available jobs',
            'Powered by',
        )
        for ln in lines:
            if not ln or ln in drop_exact:
                continue
            if ln.lower().startswith(('apply now', 'save job', 'share job')) and len(ln) <= 40:
                continue
            if any(s.lower() in ln.lower() for s in drop_contains):
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        # Hard cut at first sign of similar/related jobs to avoid polling in description
        cut_markers = [
            'Similar jobs', 'Similar Jobs', 'Recommended jobs', 'More jobs', 'You might also like',
            'Related jobs', 'Other opportunities'
        ]
        low = text.lower()
        cut_at = None
        for m in cut_markers:
            i = low.find(m.lower())
            if i != -1:
                cut_at = i if cut_at is None else min(cut_at, i)
        if cut_at is not None:
            text = text[:cut_at].rstrip()
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def sanitize_for_model(self, data: dict) -> dict:
        safe = dict(data)
        try:
            parsed = urlparse(safe.get('external_url') or '')
            canon = f"{parsed.scheme or 'https'}://{parsed.netloc}{parsed.path}"
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
        return safe

    def extract_job_from_detail(self, page, job_url: str) -> dict | None:
        try:
            try:
                page.goto(job_url, wait_until="domcontentloaded", timeout=40000)
            except Exception:
                page.goto(job_url, wait_until="load", timeout=60000)
            self.human_like_delay(0.9, 1.6)

            try:
                page.wait_for_selector('h1', timeout=12000)
            except Exception:
                pass

            title = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title = (h1.inner_text() or '').strip()
            except Exception:
                pass
            if not title:
                # Derive a readable title from slug
                slug = urlparse(job_url).path.split('/')[-1]
                slug = re.sub(r"_[0-9a-f-]{6,}$", '', slug)
                words = [w for w in re.split(r'[-_]', slug) if w]
                title = ' '.join(w.capitalize() for w in words)

            # Description container heuristics
            description = ''
            for sel in ['article', 'main', '.job-description', '.description', '.content', 'section[role="main"]', '[class*="description"]']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        txt = (el.inner_text() or '').strip()
                        if txt and len(txt) > 150:
                            description = self.clean_description(txt)
                            break
                except Exception:
                    continue
            if not description:
                try:
                    body = page.inner_text('body')
                except Exception:
                    body = ''
                if body and len(body) > 150:
                    description = self.clean_description(body)

            # Find metadata by label occurrence within the description/body
            try:
                body_text = page.inner_text('body')
            except Exception:
                body_text = description
            body_text = body_text or ''

            # Remove trailing sections that list similar jobs before any further parsing
            tail_markers = ['Similar jobs', 'Similar Jobs', 'Recommended jobs', 'More jobs', 'See more results']
            body_main = body_text
            low_body = body_text.lower()
            cut = None
            for m in tail_markers:
                idx = low_body.find(m.lower())
                if idx != -1:
                    cut = idx if cut is None else min(cut, idx)
            if cut is not None:
                body_main = body_text[:cut]

            stop_labels = ['Work Type', 'Work type', 'Salary', 'Location', 'Category', 'Classifications', 'Contact', 'Posted']
            # DOM-first attempt: Look for explicit location containers near header
            location_text = ''
            try:
                location_selectors = [
                    '.job-header [class*="location"]', '.position-header [class*="location"]',
                    '.job-summary [class*="location"]', '.position-summary [class*="location"]',
                    '.location', '[class*="location"]', '.job-info', '.summary'
                ]
                for sel in location_selectors:
                    el = page.query_selector(sel)
                    if not el:
                        continue
                    cand = (el.inner_text() or '').strip()
                    if not cand:
                        continue
                    # Accept if it contains a state abbrev or a comma separated pair
                    if re.search(r'\b(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b', cand, re.IGNORECASE) or ',' in cand:
                        location_text = cand
                        break
            except Exception:
                pass
            # Prefer a chunk around the title for location patterns
            header_chunk = ''
            try:
                title_idx = body_main.lower().find(title.lower()) if title else -1
                if title_idx != -1:
                    header_chunk = body_main[title_idx:title_idx + 800]
                else:
                    header_chunk = '\n'.join(body_main.splitlines()[:20])
            except Exception:
                header_chunk = '\n'.join(body_main.splitlines()[:20])
            if not location_text:
                header_match = re.search(r"([A-Za-z][A-Za-z\-\s']+\s+(?:ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b(?:\s+\d{4})?)", header_chunk)
                location_text = header_match.group(1).strip() if header_match else self.extract_field_by_label(body_main, 'Location', stop_labels)
            # Fallback: anywhere in the main body before similar jobs
            if not location_text:
                any_match = re.search(r"\b([A-Z][a-zA-Z\-\s']+),\s+(New South Wales|Victoria|Queensland|South Australia|Western Australia|Tasmania|Northern Territory|Australian Capital Territory)\b", body_main)
                if any_match:
                    location_text = f"{any_match.group(1)}, {any_match.group(2)}"
            if not location_text:
                any_match2 = re.search(r"\b([A-Za-z][A-Za-z\-\s']+)\s+(ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\b(?:\s+\d{4})?", body_main)
                if any_match2:
                    location_text = any_match2.group(0)
            work_type_text = self.extract_field_by_label(body_main, 'Work Type', stop_labels) or self.extract_field_by_label(body_main, 'Work type', stop_labels)
            salary_text = self.extract_field_by_label(body_main, 'Salary', stop_labels)
            category_text = self.extract_field_by_label(body_main, 'Category', stop_labels)
            if not category_text:
                category_text = self.extract_field_by_label(body_main, 'Classifications', stop_labels)

            # Also attempt breadcrumb-like links visible near the header for category
            if not category_text:
                try:
                    crumbs = page.query_selector_all('nav a, .breadcrumbs a, a[href*="/jobs/"]')
                except Exception:
                    crumbs = []
                texts = [((c.inner_text() or '').strip()) for c in crumbs]
                for tx in texts:
                    if re.search(r'transport|logistic|manufactur|health|retail|warehouse|mining|gas|utilities|telecom', tx, re.IGNORECASE):
                        category_text = tx
                        break
        
            # If salary label missing, try to detect an inline "Salary:" section in description/body
            if not salary_text:
                m_salary = re.search(r"Salary\s*[:\-]?\s*(.+)$", header_chunk, re.IGNORECASE | re.MULTILINE)
                if not m_salary:
                    m_salary = re.search(r"Salary\s*[:\-]?\s*(.+)$", body_main, re.IGNORECASE | re.MULTILINE)
                if m_salary:
                    salary_text = m_salary.group(1).strip()
            salary_parsed = self.parse_salary(salary_text or description)
            job_type = self.normalize_job_type(work_type_text or description)
            location_obj = self.get_or_create_location(location_text)
            job_category = self.map_category(category_text) if category_text else JobCategorizationService.categorize_job(title, description)

            external_id = ''
            m = re.search(r"/jobview/[^/]+/([0-9a-f-]{6,})", job_url, re.IGNORECASE)
            if m:
                external_id = m.group(1)

            if not title or not description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            return {
                'title': title[:200],
                'description': description[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f"programmed_{external_id}" if external_id else f"programmed_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': category_text or '',
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    def save_job(self, data: dict) -> JobPosting | None:
        try:
            with transaction.atomic():
                safe = self.sanitize_for_model(data)
                existing = JobPosting.objects.filter(external_url=safe['external_url']).first()
                if existing:
                    logger.info(f"Already exists, skipping: {existing.title}")
                    return existing
                job = JobPosting.objects.create(
                    title=safe['title'],
                    description=safe['description'],
                    company=self.company,
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
                    external_source='jobs.programmed.com.au',
                    external_url=safe['external_url'],
                    external_id=safe['external_id'],
                    status='active',
                    posted_ago=safe['posted_ago'],
                    date_posted=safe['date_posted'],
                    tags='',
                    additional_info={'scraped_from': 'programmed', 'scraper_version': '1.0'}
                )
                if safe.get('category_raw'):
                    info = job.additional_info or {}
                    info['category_raw'] = safe['category_raw']
                    job.additional_info = info
                    job.save(update_fields=['additional_info'])
                logger.info(f"Saved job: {job.title}")
                return job
        except Exception as e:
            logger.error(f"DB save error: {e}")
        return None
        
    def scrape(self) -> int:
        logger.info("Starting Programmed scraping...")
        self.setup_database_objects()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            try:
                links = self.extract_job_links_from_search(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning("No job links found on Programmed search page.")
                for i, job_url in enumerate(links):
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

    scraper = ProgrammedScraper(max_jobs=max_jobs, headless=True)
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    

if __name__ == "__main__":
    main()


