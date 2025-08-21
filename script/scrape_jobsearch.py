#!/usr/bin/env python
"""
JobSearch.com.au scraper using Playwright.

Features:
- Collect all categories from the categories index, or restrict to specific
  letters and/or category names.
- For each category, paginates through the job list. For each job card, it
  opens the right-side details pane and extracts title, company, location,
  employment type, salary, category, posted text, and full description.
- Saves into Django `JobPosting`, creating `Company` and `Location` records
  as needed.

Usage examples:
  python script/scrape_jobsearch.py               # scrape ALL categories
  python script/scrape_jobsearch.py --letters A   # only categories under A
  python script/scrape_jobsearch.py --categories "Accounting,Construction"
  python script/scrape_jobsearch.py --max-pages 3 --headful

Notes:
- This scraper reads only the internal right-hand job panel on the category
  listing page. For uniqueness, if an external "Open" link exists, we store
  its URL; otherwise we synthesize a deterministic hash from title + category
  + page number + index.
"""

import os
import sys
import re
import time
import random
import logging
import argparse
from hashlib import sha1
from urllib.parse import urljoin, urlparse

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

try:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    PROJECT_ROOT = os.getcwd()

sys.path.append(PROJECT_ROOT)

import django  # noqa: E402
django.setup()

from django.db import transaction, connections  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402
from django.utils import timezone  # noqa: E402
from django.utils.text import slugify  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

from apps.companies.models import Company  # noqa: E402
from apps.core.models import Location  # noqa: E402
from apps.jobs.models import JobPosting  # noqa: E402
from apps.jobs.services import JobCategorizationService  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_jobsearch.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class JobSearchScraper:
    def __init__(self,
                 only_letters: list[str] | None = None,
                 only_categories: list[str] | None = None,
                 max_pages: int | None = None,
                 headless: bool = True,
                 max_jobs_per_category: int | None = None):
        self.base_url = 'https://www.jobsearch.com.au'
        self.categories_url = f'{self.base_url}/categories'
        self.only_letters = [s.upper() for s in (only_letters or [])]
        self.only_categories = [s.strip().lower() for s in (only_categories or [])]
        self.max_pages = max_pages
        self.headless = headless
        self.max_jobs_per_category = max_jobs_per_category
        self.scraper_user: User | None = None
        self.scraped_count = 0

    # -------------------- General helpers --------------------
    def human_like_delay(self, min_s=0.2, max_s=0.6):
        time.sleep(random.uniform(min_s, max_s))

    def safe_click(self, page, locator) -> bool:
        try:
            locator.click(timeout=1200, no_wait_after=True)
            self.human_like_delay(0.15, 0.35)
            return True
        except Exception:
            # Try a forced click
            try:
                locator.click(timeout=1200, force=True, no_wait_after=True)
                self.human_like_delay(0.15, 0.35)
                return True
            except Exception:
                pass
            try:
                locator.focus(timeout=600)
                page.keyboard.press('Enter')
                self.human_like_delay(0.15, 0.35)
                return True
            except Exception:
                return False

    def setup_database_objects(self):
        self.scraper_user, _ = User.objects.get_or_create(
            username='jobsearch_scraper',
            defaults={
                'email': 'scraper@jobsearch.local',
                'first_name': 'JobSearch',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_company(self, name: str | None) -> Company:
        company_name = (name or 'JobSearch Australia').strip() or 'JobSearch Australia'
        company, _ = Company.objects.get_or_create(
            name=company_name,
            defaults={
                'description': 'Source: jobsearch.com.au',
                'website': self.base_url,
                'company_size': 'medium',
                'logo': ''
            }
        )
        return company

    def get_or_create_location(self, location_text: str | None) -> Location | None:
        if not location_text:
            return None
        text = location_text.strip()
        if not text:
            return None
        city = ''
        state = ''
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
            text = re.sub(rf'\b{k}\b', v, text, flags=re.IGNORECASE)
        if ',' in text:
            parts = [p.strip() for p in text.split(',')]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        else:
            # Most chips look like "Sydney NSW", "Victoria VIC"
            m = re.search(r'^(.*?)\s+(New South Wales|Victoria|Queensland|South Australia|Western Australia|Tasmania|Northern Territory|Australian Capital Territory|NSW|VIC|QLD|SA|WA|TAS|NT|ACT)$', text, re.IGNORECASE)
            if m:
                city = m.group(1).strip()
                state = m.group(2).strip()
            else:
                state = text
        name = f"{city}, {state}" if city and state else (state or city)
        # Safety: enforce model max_length 100 for Location.name
        if name and len(name) > 100:
            name = name[:100]
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
        # External URL canonicalization/truncation
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
        return safe

    # -------------------- Site-specific extraction --------------------
    def extract_company_and_location(self, panel_text: str) -> tuple[str, str]:
        """Extract company and location from the right-side panel header line.

        Heuristics:
        - Look for a line containing a middle dot '·' or a hyphen ' - ' separating
          company and location; use the right part as location and left as company.
        - As a fallback, find a line that has a state abbreviation and split at the
          last separator.
        - Returns (company_name, location_text). Either may be empty if not found.
        """
        if not panel_text:
            return '', ''
        lines = [ln.strip() for ln in panel_text.split('\n') if ln.strip()]
        # Drop common noise lines
        noise = {'home', 'jobs', 'apply now', 'open', 'view & apply'}
        lines = [ln for ln in lines if ln.strip().lower() not in noise]
        # Usually title is first, the next line contains company · location
        for ln in lines[:8]:
            # Prefer line with middle dot
            if '·' in ln:
                parts = [p.strip(' -\u2022\u00b7\t ') for p in ln.split('·')]
                if len(parts) >= 2:
                    company = ' · '.join(parts[:-1]).strip()
                    location = parts[-1].strip()
                    return company, location
            # Try hyphen separator near the end (company - location)
            if ' - ' in ln:
                left, right = ln.rsplit(' - ', 1)
                # Prefer right part that looks like it ends with a state code
                if re.search(r'\b(ACT|NSW|VIC|QLD|SA|WA|TAS|NT)\b', right):
                    return left.strip(), right.strip()
        # Fallback: find any line with a state code; split at last bullet/dash/comma
        for ln in lines[:10]:
            if re.search(r'\b(ACT|NSW|VIC|QLD|SA|WA|TAS|NT)\b', ln):
                for sep in [' · ', ' - ', ' — ', ' | ', ', ']:
                    if sep in ln:
                        left, right = ln.rsplit(sep, 1)
                        return left.strip(), right.strip()
                # If no clear separator, assume trailing words form location
                m = re.search(r'(.*?)(\b[A-Za-z].*\b(?:ACT|NSW|VIC|QLD|SA|WA|TAS|NT)\b.*)$', ln)
                if m:
                    return m.group(1).strip(' -\t•'), m.group(2).strip()
        return '', ''

    def clean_panel_description(self, panel_text: str) -> str:
        if not panel_text:
            return ''
        lines = [ln.rstrip() for ln in panel_text.split('\n')]
        cleaned = []
        skip_exact = {'Home', 'Jobs', 'Apply Now', 'Open', 'View & Apply'}
        for ln in lines:
            if ln.strip() in skip_exact:
                continue
            cleaned.append(ln)
        # Collapse extra blank lines
        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def get_all_category_links(self, page) -> dict:
        """Return mapping {display_name -> absolute_url}.
        Respects self.only_letters and self.only_categories if provided.
        """
        links: dict[str, str] = {}
        page.goto(self.categories_url, wait_until='domcontentloaded', timeout=35000)
        self.human_like_delay(0.8, 1.4)

        # If letter filters requested, click each letter to load its section
        letters_to_visit = self.only_letters or []
        if not letters_to_visit:
            # Read all visible categories without letter filtering
            letters_to_visit = []

        def harvest_categories():
            anchors = page.query_selector_all('a[href]')
            for a in anchors:
                try:
                    href = a.get_attribute('href') or ''
                    text = (a.inner_text() or '').strip()
                    # Clean any arrow/newline artifacts seen on site buttons
                    if '\n' in text:
                        text = text.split('\n', 1)[0]
                    text = text.replace('→', ' ').strip()
                except Exception:
                    continue
                if not href or not text:
                    continue
                low = href.lower()
                if '-jobs' in low and low.startswith('/'):
                    abs_url = urljoin(self.base_url, href)
                    if self.only_categories and text.strip().lower() not in self.only_categories:
                        continue
                    links[text.strip()] = abs_url

        # First, harvest without any letter click
        harvest_categories()

        # Then optionally click each letter filter to surface categories scoped by letter
        for letter in letters_to_visit:
            try:
                # Buttons look like a grid of single-letter buttons.
                btn = page.query_selector(f'button:has-text("{letter}")')
                if btn:
                    btn.click()
                    self.human_like_delay(0.4, 0.9)
                    harvest_categories()
            except Exception:
                continue

        if not links:
            logger.warning('No categories discovered on the categories page.')
        return links

    def iterate_jobs_on_category(self, page, category_url: str):
        """Yield dictionaries with raw fields by visiting each page and job card.
        """
        current_page = 1
        page.goto(category_url, wait_until='domcontentloaded', timeout=40000)
        self.human_like_delay(1.0, 1.6)

        seen_hashes: set[str] = set()

        while True:
            # Wait for cards to render
            try:
                page.wait_for_selector('h3, h2', timeout=12000)
            except Exception:
                pass

            # Prefer selecting headings from the left column (list) rather than generic containers
            cards = []
            try:
                left_indices = page.evaluate('''() => {
                  const heads = Array.from(document.querySelectorAll('h3, h2'));
                  if (!heads.length) return [];
                  const aside = document.querySelector('aside');
                  const asideLeft = aside ? aside.getBoundingClientRect().left : (window.innerWidth * 0.65);
                  const list = [];
                  heads.forEach((el, idx) => {
                    const r = el.getBoundingClientRect();
                    const inAside = !!el.closest('aside');
                    // Left column: center x must be left of aside panel
                    const cx = r.left + r.width / 2;
                    if (!inAside && cx < asideLeft && r.height > 0 && r.width > 0) {
                      list.push(idx);
                    }
                  });
                  return list;
                }''') or []
                for i in left_indices:
                    try:
                        cards.append(page.locator('h3, h2').nth(int(i)))
                    except Exception:
                        continue
            except Exception:
                cards = []

            # If headings strategy yields nothing, fall back to container heuristics
            if not cards:
                for sel in [
                    'a:has(h3), a:has(h2)',
                    'article:has(h3), article:has(h2)',
                    '.MuiPaper-root:has(h3), .MuiPaper-root:has(h2)'
                ]:
                    try:
                        nodes = page.query_selector_all(sel)
                        if nodes:
                            cards = nodes
                            break
                    except Exception:
                        continue

            if not cards:
                logger.info('No job cards found on this page.')

            collected_in_category = 0
            for idx in range(len(cards)):
                # Re-query each time to avoid stale handles after DOM updates
                target = None
                try:
                    # When cards are heading locators, click their nearest clickable ancestor
                    loc = cards[idx]
                    target = loc
                except Exception:
                    target = None
                if not target:
                    continue

                # Click to display right-side panel
                if not self.safe_click(page, target):
                    # Try clickable ancestor: a, button, [role=button] using locator API only
                    try:
                        ancestor = target.locator('xpath=ancestor-or-self::*[self::a or self::button or @role="button"][1]')
                        if ancestor and ancestor.count() >= 1:
                            if not self.safe_click(page, ancestor.first):
                                continue
                        else:
                            continue
                    except Exception:
                        continue

                # Extract from right panel
                right_text = ''
                description = ''
                try:
                    # Support multiple possible wrappers of the details pane
                    aside = page.query_selector('aside, [data-testid*="details"], [class*="details"], [class*="sidebar"], [data-testid*="panel"]')
                    if aside:
                        right_text = (aside.inner_text() or '').strip()
                        description = self.clean_panel_description(right_text)
                except Exception:
                    pass

                # Fallback to main/article content if aside missing
                if not right_text:
                    try:
                        main = page.query_selector('main')
                        if main:
                            right_text = (main.inner_text() or '').strip()
                            description = self.clean_panel_description(right_text)
                    except Exception:
                        right_text = ''

                # Title
                title = ''
                for sel in ['aside h1', 'aside h2', 'aside h3', 'main h1', 'main h2', 'main h3']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            t = (el.inner_text() or '').strip()
                            if t:
                                title = t
                                break
                    except Exception:
                        continue

                # Company and location: prefer right panel header line
                company_name, location_text = self.extract_company_and_location(right_text)
                if not company_name or not location_text:
                    # Fallback to infer from the clicked card left-side text
                    try:
                        small_text = (target.inner_text() or '').strip()
                        lines = [ln.strip() for ln in small_text.split('\n') if ln.strip()]
                        if len(lines) >= 2 and not company_name:
                            company_name = lines[1]
                        if not location_text:
                            state_pat = r'(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)'
                            for ln in lines[:6]:
                                if re.search(state_pat, ln):
                                    location_text = ln
                                    break
                    except Exception:
                        pass

                # Summary fields on the right panel
                def find_labeled(label: str) -> str:
                    try:
                        body = right_text
                        m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", body, re.IGNORECASE)
                        if m:
                            return re.sub(r'\s+', ' ', m.group(1).strip())
                    except Exception:
                        return ''
                    return ''

                employment_type_text = find_labeled('Employment type')
                salary_text = find_labeled('Salary')
                category_display = find_labeled('Category')
                posted_text = find_labeled('Posted')

                if not title:
                    # Extract title directly from the clicked card
                    try:
                        for hsel in ['h3', 'h2']:
                            h = target.locator(hsel).first
                            if h and h.is_visible():
                                title = h.inner_text().strip()
                                break
                    except Exception:
                        pass

                # Build a synthetic external url/id
                external_url = ''
                try:
                    # If the selected card has an "Open" link, prefer it.
                    open_link = target.locator('a:has-text("Open")').first
                    if open_link and open_link.is_visible():
                        href = open_link.get_attribute('href') or ''
                        if href:
                            external_url = urljoin(self.base_url, href)
                except Exception:
                    pass
                if not external_url:
                    # Create a stable synthetic URI
                    digest = sha1(f"{category_url}|{current_page}|{idx}|{title}".encode('utf-8')).hexdigest()[:16]
                    external_url = f"{category_url}#js-{digest}"

                # Salary: prefer summary, else detect first $range in description
                salary_source = salary_text
                if not salary_source and description:
                    m = re.search(r'\$\s*[0-9][0-9,]*(?:\.[0-9]{1,2})?\s*[–-]\s*\$?\s*[0-9][0-9,]*(?:\.[0-9]{1,2})?', description)
                    if m:
                        salary_source = m.group(0)
                salary_parsed = self.parse_salary(salary_source)
                job_type = self.normalize_job_type(employment_type_text)
                location_obj = self.get_or_create_location(location_text)

                # Map/ensure category choice
                job_category = 'other'
                if category_display:
                    mapped = JobCategorizationService.normalize_display_category(category_display)
                    key = slugify(mapped).replace('-', '_')[:50] or 'other'
                    if any(c[0] == key for c in JobPosting.JOB_CATEGORY_CHOICES):
                        job_category = key
                    else:
                        job_category = self.ensure_category_choice(mapped)
                else:
                    job_category = JobCategorizationService.categorize_job(title, description)

                # Skip if insufficient data
                if not title or not description:
                    continue

                # De-dup within page loop
                digest = sha1(f"{external_url}|{title}".encode('utf-8')).hexdigest()
                if digest in seen_hashes:
                    continue
                seen_hashes.add(digest)

                yield {
                    'title': title.strip(),
                    'description': description.strip()[:8000],
                    'company_name': company_name.strip(),
                    'location': location_obj,
                    'job_type': job_type,
                    'job_category': job_category,
                    'date_posted': timezone.now(),
                    'external_url': external_url,
                    'external_id': f"jobsearch_{sha1(external_url.encode('utf-8')).hexdigest()[:20]}",
                    'salary_min': salary_parsed['salary_min'],
                    'salary_max': salary_parsed['salary_max'],
                    'salary_currency': salary_parsed['salary_currency'],
                    'salary_type': salary_parsed['salary_type'],
                    'salary_raw_text': salary_parsed['salary_raw_text'],
                    'work_mode': 'On-site',
                    'posted_ago': posted_text,
                    'category_raw': category_display,
                }

                collected_in_category += 1
                if self.max_jobs_per_category and collected_in_category >= self.max_jobs_per_category:
                    return

            # Pagination: click Next
            if self.max_pages and current_page >= self.max_pages:
                break
            next_clicked = False
            for sel in ['button:has-text("Next")', 'a:has-text("Next")', '[aria-label="Next page"]']:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_enabled():
                        if self.safe_click(page, el):
                            next_clicked = True
                            current_page += 1
                            break
                except Exception:
                    continue
            if not next_clicked:
                break

    # -------------------- Persistence --------------------
    def save_job(self, data: dict) -> JobPosting | None:
        try:
            with transaction.atomic():
                company = self.get_or_create_company(data.get('company_name'))
                safe = self.sanitize_for_model(data)
                # Deduplicate by external_url primarily; if synthetic URL, also try title+company same day
                existing = JobPosting.objects.filter(external_url=safe['external_url']).first()
                if existing:
                    logger.info(f"Already exists, skipping: {existing.title}")
                    return existing
                # Guard against overly long external_id (max 100)
                if safe.get('external_id') and len(safe['external_id']) > 100:
                    safe['external_id'] = safe['external_id'][:100]
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
                    external_source='jobsearch.com.au',
                    external_url=safe['external_url'],
                    external_id=safe['external_id'],
                    status='active',
                    posted_ago=safe['posted_ago'],
                    date_posted=safe['date_posted'],
                    tags='',
                    additional_info={'scraped_from': 'jobsearch', 'scraper_version': '1.0'}
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

    # -------------------- Orchestration --------------------
    def scrape(self) -> int:
        self.setup_database_objects()
        total_saved = 0

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            )
            # Index page for discovering categories
            index_page = context.new_page()
            index_page.set_default_timeout(2000)
            index_page.set_default_navigation_timeout(10000)

            categories = self.get_all_category_links(index_page)
            if not categories:
                logger.warning('No categories to process.')
                try:
                    index_page.close()
                except Exception:
                    pass
                browser.close()
                connections.close_all()
                return 0

            # Done with index page
            try:
                index_page.close()
            except Exception:
                pass

            for display, url in categories.items():
                logger.info(f"Processing category: {display} -> {url}")
                # Use a fresh page per category to avoid 'page closed' issues
                cat_page = context.new_page()
                # Tight default timeouts per page
                cat_page.set_default_timeout(2000)
                cat_page.set_default_navigation_timeout(10000)
                try:
                    for job in self.iterate_jobs_on_category(cat_page, url):
                        saved = self.save_job(job)
                        if saved:
                            total_saved += 1
                        self.human_like_delay(0.2, 0.5)
                except Exception as e:
                    logger.error(f"Error in category '{display}': {e}")
                finally:
                    try:
                        cat_page.close()
                    except Exception:
                        pass

            browser.close()

        connections.close_all()
        logger.info(f"Completed. Jobs processed: {total_saved}")
        return total_saved


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='JobSearch.com.au scraper')
    parser.add_argument('--letters', type=str, default='', help='Comma-separated letters, e.g., A,B,C')
    parser.add_argument('--categories', type=str, default='', help='Comma-separated category names to include')
    parser.add_argument('--max-pages', type=int, default=None, help='Max pages per category')
    parser.add_argument('--max-per-cat', type=int, default=None, help='Max jobs per category (speed limiter)')
    parser.add_argument('--headful', action='store_true', help='Run with a visible browser window')
    return parser.parse_args()


def main():
    args = parse_args()
    letters = [s.strip() for s in args.letters.split(',') if s.strip()]
    categories = [s.strip() for s in args.categories.split(',') if s.strip()]
    scraper = JobSearchScraper(
        only_letters=letters or None,
        only_categories=categories or None,
        max_pages=args.max_pages,
        headless=not args.headful,
        max_jobs_per_category=args.max_per_cat,
    )
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


