#!/usr/bin/env python
"""
IAG Careers Job Scraper (Playwright)

Target: https://careers.iag.com.au/global/en/search-results

Key functionality:
- Collect jobs from the search results list
- For each job, open the detail page and extract full description
- Handle the "Available in X locations" modal to capture all locations
- When a single location is shown on the card, capture it directly
- Parse salary (if present), job type/work mode (Hybrid/On-site/Remote), and metadata
- Save to Django models `JobPosting`, `Company`, `Location` creating one JobPosting per location

Run:
    python script/iag_australia_scraper.py [max_jobs]

Notes:
- Designed to be resilient to small structure changes; selectors are defensive.
- Optimised for Australian locations and the IAG UI elements in the screenshots.
"""

import os
import sys
import re
import time
import logging
from typing import List, Optional
from urllib.parse import urljoin
from datetime import datetime

# Django setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

import django
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_iag.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


STATE_ABBREV = {
    'NSW': 'New South Wales',
    'VIC': 'Victoria',
    'QLD': 'Queensland',
    'SA': 'South Australia',
    'WA': 'Western Australia',
    'TAS': 'Tasmania',
    'NT': 'Northern Territory',
    'ACT': 'Australian Capital Territory',
}


def normalize_location(name: str) -> str:
    if not name:
        return ''
    # Remove leading label like "Location:" if present
    text = re.sub(r'^\s*Location\s*:\s*', '', name, flags=re.IGNORECASE).strip()
    text = re.sub(r'\s+', ' ', text)
    for abbr, full in STATE_ABBREV.items():
        text = re.sub(rf'\b{abbr}\b', full, text, flags=re.IGNORECASE)
    return text


def get_or_create_location(location_text: str | None) -> Optional[Location]:
    if not location_text:
        return None
    text = normalize_location(location_text)
    city = ''
    state = ''
    if ',' in text:
        parts = [p.strip() for p in text.split(',')]
        if len(parts) >= 2:
            city, state = parts[0], ', '.join(parts[1:])
        else:
            city = text
    else:
        # Sometimes card shows only state
        if any(s in text for s in STATE_ABBREV.values()):
            state = text
        else:
            city = text
    name = f"{city}, {state}".strip(', ')
    loc, _ = Location.objects.get_or_create(
        name=name,
        defaults={'city': city, 'state': state, 'country': 'Australia'}
    )
    return loc


def parse_salary(raw: str | None) -> dict:
    res = {
        'salary_min': None,
        'salary_max': None,
        'salary_type': 'yearly',
        'salary_currency': 'AUD',
        'salary_raw_text': raw or ''
    }
    if not raw:
        return res
    text = raw.strip()
    if re.search(r'hour', text, re.IGNORECASE):
        res['salary_type'] = 'hourly'
    elif re.search(r'week', text, re.IGNORECASE):
        res['salary_type'] = 'weekly'
    elif re.search(r'month', text, re.IGNORECASE):
        res['salary_type'] = 'monthly'
    # Numbers like $80,000 - $100,000 or $45.50 per hour
    nums = re.findall(r'\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)', text)
    vals: List[float] = []
    for n in nums:
        try:
            vals.append(float(n.replace(',', '')))
        except Exception:
            continue
    if vals:
        non_zero = [v for v in vals if v > 0]
        if len(non_zero) >= 2:
            res['salary_min'] = min(non_zero)
            res['salary_max'] = max(non_zero)
        elif len(non_zero) == 1:
            res['salary_min'] = non_zero[0]
            res['salary_max'] = non_zero[0]
    return res


def normalize_job_type(text: str | None) -> str:
    if not text:
        return 'full_time'
    t = text.lower()
    if 'permanent' in t:
        return 'permanent'
    if 'contract' in t or 'fixed term' in t or 'max-term' in t:
        return 'contract'
    if 'part' in t:
        return 'part_time'
    if 'temp' in t or 'temporary' in t:
        return 'temporary'
    if 'casual' in t:
        return 'casual'
    if 'intern' in t:
        return 'internship'
    return 'full_time'


class IAGScraper:
    base = 'https://careers.iag.com.au'
    search_url = f'{base}/global/en/search-results'

    def __init__(self, max_jobs: int | None = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.company: Optional[Company] = None
        self.user: Optional[User] = None

    def setup_entities(self):
        self.company, _ = Company.objects.get_or_create(
            name='Insurance Australia Group',
            defaults={
                'website': self.base,
                'description': 'IAG Careers (Australia & New Zealand)'
            }
        )
        self.user, _ = User.objects.get_or_create(
            username='iag_scraper',
            defaults={'email': 'iag.scraper@local'}
        )

    def open_browser(self):
        self.play = sync_playwright().start()
        self.browser = self.play.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36'
        )
        self.page = self.context.new_page()

    def close_browser(self):
        try:
            self.context.close()
            self.browser.close()
            self.play.stop()
        except Exception:
            pass

    def list_cards(self):
        self.page.goto(self.search_url, wait_until='domcontentloaded', timeout=60000)
        # Allow dynamic list to render
        self.page.wait_for_selector('text=Showing 1', timeout=20000)
        cards = self.page.query_selector_all('div[role="listitem"], .job, .job-results-list article, .search-results-list article')
        if not cards:
            # fallback: each result row container
            cards = self.page.query_selector_all('section[aria-label*="results"] article, section[aria-label*="results"] div[class*="result"]')
        return cards

    def find_cards_on_current_page(self):
        """Return job card elements from the currently loaded search-results page (restricted to results section)."""
        cards = self.page.query_selector_all('section[aria-label*="results"] article, section[aria-label*="results"] div[role="listitem"]')
        if not cards:
            cards = self.page.query_selector_all('div[role="listitem"]')
        return cards

    def collect_all_cards(self):
        """Collect job card data across all result pages using the `from` query pagination."""
        jobs = []
        seen_urls = set()
        page_size = 10
        offset = 0
        max_pages = 100
        pages_visited = 0

        while pages_visited < max_pages:
            url = f"{self.search_url}?from={offset}&s=1"
            self.page.goto(url, wait_until='domcontentloaded', timeout=60000)
            try:
                self.page.wait_for_selector('text=Showing', timeout=15000)
            except Exception:
                pass
            cards = self.find_cards_on_current_page()
            new_on_page = 0
            for card in cards:
                data = self.extract_card(card)
                u = data.get('url')
                if u and '/global/en/job/' in u and u not in seen_urls:
                    seen_urls.add(u)
                    jobs.append(data)
                    new_on_page += 1
            pages_visited += 1
            # Determine next offset
            if new_on_page == 0:
                break
            offset += page_size
        return jobs

    def extract_card(self, card) -> dict:
        # Title
        title = ''
        try:
            title = (card.query_selector('a, h3, .title') or card).inner_text().strip()
        except Exception:
            pass
        # Link - prefer real job detail links, ignore Apply/recruiting/policy links
        job_href = ''
        try:
            anchors = card.query_selector_all('a[href]')
            preferred = ''
            fallback = ''
            for a in anchors:
                href = a.get_attribute('href') or ''
                if not href:
                    continue
                low = href.lower()
                # Skip apply and external tracking links
                if ('apply' in low or 'job-reference' in low or 'recruiting.com' in low or
                    'emailpersonalinfo' in low or 'cookiesettings' in low):
                    continue
                if '/job/' in low:
                    preferred = href
                    break
                if not fallback:
                    fallback = href
            job_href = preferred or fallback or ''
        except Exception:
            pass
        if job_href and not job_href.startswith('http'):
            job_href = urljoin(self.base, job_href)

        # Location shorthand on card (either single location or text like "Available in 5 locations")
        short_loc = ''
        try:
            # Common badge area contains icons; look for pin icon sibling text
            loc_el = None
            for sel in ['[class*="location"]', 'svg[aria-hidden="true"] ~ span', 'span:has(svg)']:
                loc_el = card.query_selector(sel)
                if loc_el:
                    txt = (loc_el.inner_text() or '').strip()
                    if txt:
                        short_loc = txt
                        break
        except Exception:
            pass

        return {
            'title': title.strip(),
            'url': job_href,
            'short_location': short_loc
        }

    def open_locations_modal_and_collect(self) -> List[str]:
        """If an "Available in X locations" link exists, click and capture the list."""
        locations: List[str] = []
        try:
            # The trigger link often contains text 'Available in'
            trigger = self.page.query_selector('a:has-text("Available in")')
            if not trigger:
                return locations
            trigger.click()
            self.page.wait_for_selector('role=dialog', timeout=8000)
            modal = self.page.query_selector('role=dialog') or self.page.query_selector('[role="dialog"]')
            if modal:
                items = modal.query_selector_all('li, [class*="location"]')
                for li in items:
                    txt = (li.inner_text() or '').strip()
                    if txt:
                        locations.append(txt)
            # Close modal
            try:
                (self.page.query_selector('role=dialog button[aria-label="Close"]') or self.page.query_selector('role=dialog button') or self.page.query_selector('button[aria-label="Close"]')).click()
            except Exception:
                self.page.keyboard.press('Escape')
        except Exception:
            pass
        return [normalize_location(x) for x in locations if x.strip()]

    def extract_detail(self, job_url: str) -> dict:
        # Use full load then ensure key content appears
        self.page.goto(job_url, wait_until='load', timeout=60000)
        try:
            self.page.wait_for_selector('h1, [data-automation*="jobTitle"], [data-automation*="jobDescription"], main, article', timeout=15000)
        except Exception:
            time.sleep(1.0)
        # Title
        try:
            title_el = (self.page.query_selector('h1') or self.page.query_selector('[data-automation*="jobTitle"]') or self.page.query_selector('header h1'))
            title = title_el.inner_text().strip() if title_el else ''
        except Exception:
            title = ''
        # Description
        description = ''
        for sel in ['[data-automation*="jobDescription"]', '.job-description', '.description', 'main', 'article']:
            try:
                el = self.page.query_selector(sel)
                if el:
                    txt = (el.inner_text() or '').strip()
                    if len(txt) > 100:
                        description = txt
                        break
            except Exception:
                continue
        if not description:
            # Fallback to body text if specific containers failed
            try:
                bt = (self.page.inner_text('body') or '').strip()
                if len(bt) > 80:
                    # Trim obvious UI noise
                    bt = re.sub(r'Apply Now.*', '', bt, flags=re.IGNORECASE | re.DOTALL)
                    description = bt.strip()
            except Exception:
                description = ''

        # Remove UI phrases that should not be stored in description
        if description:
            ui_phrases = [
                r"Learn more about who IAG is here\.?",
                r"Apply now", r"Save", r"Show map", r"Get notified for similar jobs",
                r"Sign up to receive job alerts", r"Email address", r"Submit", r"Manage Alerts",
                r"Get tailored job recommendations based on your interests\.?", r"Get Started",
                r"Share the opportunity", r"Share via linkedin", r"Share via twitter",
                r"Share via instagram", r"Share via email",
                r"Back to search results",
                r"Available in\s*\d+\s*locations", r"See all"
            ]
            pattern = re.compile('|'.join(ui_phrases), re.IGNORECASE)
            description = pattern.sub('', description)
            # Normalize whitespace: remove carriage returns, trim lines, drop empty lines
            description = description.replace('\r', '')
            lines = [ln.strip() for ln in description.split('\n')]
            lines = [ln for ln in lines if ln]
            description = '\n'.join(lines).strip()
        # Metadata badges (work mode, job type, salary, job id)
        badges_text = ''
        try:
            badges_text = self.page.inner_text('header, .job-hero, .job-header')
        except Exception:
            pass
        work_mode = 'Hybrid' if re.search(r'hybrid', badges_text, re.IGNORECASE) else ('Remote' if re.search(r'remote|work from home|wfh', badges_text, re.IGNORECASE) else 'On-site')
        job_type = normalize_job_type(badges_text)
        # Salary if visible on header/body
        salary_text = ''
        try:
            sal_el = self.page.query_selector(r'text=/\$\d|salary|remuneration/i')
            if sal_el:
                salary_text = sal_el.inner_text().strip()
        except Exception:
            pass
        if not salary_text:
            try:
                salary_text = self.page.inner_text('main')
                m = re.search(r'\$\s?\d[\d,]*(?:\.\d{1,2})?(?:\s*-\s*\$?\d[\d,]*(?:\.\d{1,2})?)?(?:\s*(?:per|/)?\s*(?:hour|week|month|year|annum))?', salary_text, re.IGNORECASE)
                salary_text = m.group(0) if m else ''
            except Exception:
                salary_text = ''

        # Card location on detail hero (single location when no modal)
        single_loc = ''
        for sel in ['.job-hero [class*="location"]', 'header [class*="location"]', r'text=/,\s*(?:New South Wales|Victoria|Queensland|Western Australia|South Australia|Tasmania|Northern Territory|Australian Capital Territory)/i']:
            try:
                el = self.page.query_selector(sel)
                if el:
                    t = (el.inner_text() or '').strip()
                    if t:
                        single_loc = t
                        break
            except Exception:
                continue

        # Multi-location via modal if present
        locations = self.open_locations_modal_and_collect()
        if not locations and single_loc:
            locations = [single_loc]

        return {
            'title': title,
            'description': description,
            'work_mode': work_mode,
            'job_type': job_type,
            'salary_parsed': parse_salary(salary_text),
            'locations': locations
        }

    def save_job(self, job_url: str, detail: dict):
        # Require title only; if description missing, save with a minimal fallback
        if not detail.get('title'):
            logger.warning(f"Skipping due to missing title: {job_url}")
            return 0
        saved = 0
        locs = detail.get('locations') or ['']
        for loc_text in locs:
            location_obj = get_or_create_location(loc_text) if loc_text else None
            try:
                with transaction.atomic():
                    exists = JobPosting.objects.filter(external_url=job_url, location=location_obj).exists()
                    if exists:
                        continue
                    # Guard max lengths and ensure slug uniqueness using job id suffix
                    title_val = (detail['title'] or '')[:200]
                    m = re.search(r'/job/(\d+)/', job_url)
                    job_id_suffix = (m.group(1) if m else str(abs(hash(job_url)))[:6])
                    base_slug = slugify(detail['title'])[:230]
                    slug_val = f"{base_slug}-{job_id_suffix}"[:250]
                    desc_val = (detail.get('description') or 'No description available')[:8000]
                    work_mode_val = (detail.get('work_mode', '') or '')[:50]
                    salary_raw_val = (detail['salary_parsed']['salary_raw_text'] or '')[:200]
                    external_url_val = (job_url or '')[:200]
                    external_source_val = 'iag.com.au'[:100]
                    # Derive external_id from numeric code in URL if present
                    m = re.search(r'/job/(\d+)/', job_url)
                    external_id_val = (m.group(1) if m else '')[:100]
                    JobPosting.objects.create(
                        title=title_val,
                        slug=slug_val,
                        description=desc_val,
                        company=self.company,
                        posted_by=self.user,
                        location=location_obj,
                        job_category='other',
                        job_type=detail.get('job_type', 'full_time'),
                        work_mode=work_mode_val,
                        salary_min=detail['salary_parsed']['salary_min'],
                        salary_max=detail['salary_parsed']['salary_max'],
                        salary_currency=detail['salary_parsed']['salary_currency'],
                        salary_type=detail['salary_parsed']['salary_type'],
                        salary_raw_text=salary_raw_val,
                        external_source=external_source_val,
                        external_url=external_url_val,
                        external_id=external_id_val,
                        status='active',
                        tags='',
                        additional_info={'scraped_from': 'iag_careers'}
                    )
                    saved += 1
            except Exception as e:
                logger.error(f"Save error for {job_url}: {e}")
        return saved

    def run(self, max_jobs: int | None = None):
        self.setup_entities()
        self.open_browser()
        saved_total = 0
        try:
            # Collect across all pages
            jobs = self.collect_all_cards()
            logger.info(f"Found {len(jobs)} jobs across all pages")
            if max_jobs:
                jobs = jobs[:max_jobs]
            logger.info(f"Processing {len(jobs)} jobs")
            for idx, job in enumerate(jobs, 1):
                logger.info(f"{idx}/{len(jobs)} - {job['url']}")
                detail = self.extract_detail(job['url'])
                saved = self.save_job(job['url'], detail)
                saved_total += saved
                time.sleep(0.5)
        finally:
            self.close_browser()
        logger.info(f"Saved {saved_total} postings (counting each location)")
        return saved_total


def main():
    max_jobs = None
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except Exception:
            max_jobs = None
    scraper = IAGScraper(max_jobs=max_jobs, headless=True)
    scraper.run(max_jobs)


if __name__ == '__main__':
    main()


