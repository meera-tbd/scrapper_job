#!/usr/bin/env python3
"""
Playwright scraper for Backpacker Job Board

Scrapes all job categories and saves each job detail page into Django models
(`Company`, `Location`, `JobPosting`).

- Collect category links from the homepage
- For each category, collect all job links
- Visit each job page and extract structured data
- Save to DB with duplicate check on `external_url`

Run:
  python script/scrape_backpacker.py
"""

import os
import sys
import re
import time
import random
import logging
from datetime import datetime, timedelta
import json
from typing import List, Dict, Optional, Tuple

# --- Django setup ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'

import django
django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_backpacker.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


def human_delay(a: float = 0.8, b: float = 2.2) -> None:
    time.sleep(random.uniform(a, b))


class BackpackerScraper:
    def __init__(self, headless: bool = True, job_limit: Optional[int] = None) -> None:
        self.base_url = 'https://www.backpackerjobboard.com.au/'
        self.headless = headless
        self.job_limit = job_limit  # stop after saving this many jobs; None = no limit
        self._pw = None
        self.browser = None
        self.context = None
        self.page = None
        self.scraper_user = self._get_or_create_user()
        self.saved_count = 0

    # ---- Browser lifecycle ----
    def _get_or_create_user(self):
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='backpacker_scraper_bot',
            defaults={'email': 'backpacker.bot@jobscraper.local', 'first_name': 'Backpacker', 'last_name': 'Bot'}
        )
        return user

    def setup(self) -> None:
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(headless=self.headless, args=['--no-sandbox', '--disable-dev-shm-usage'])
        self.context = self.browser.new_context(
            viewport={'width': 1420, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        self.page = self.context.new_page()

    def close(self) -> None:
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass

    # ---- Navigation & collection ----
    def goto_home(self) -> bool:
        try:
            logger.info(f'Opening home: {self.base_url}')
            self.page.goto(self.base_url, wait_until='domcontentloaded', timeout=90000)
            # dismiss cookie widget if present
            try:
                btn = self.page.query_selector("button:has-text('Accept') , button:has-text('I Agree'), .cookie-accept")
                if btn:
                    btn.click(timeout=2000)
            except Exception:
                pass
            human_delay()
            return True
        except Exception as e:
            logger.error(f'Home navigation failed: {e}')
            return False

    def collect_category_links(self) -> List[str]:
        """Collect all job category URLs from the homepage (header Job Categories modal and inline sections)."""
        patterns = [
            r"/jobs/[a-z0-9\-]+-jobs/?$",  # e.g., /jobs/au-pair-jobs/
            r"/[a-z0-9\-]+-jobs/?$"        # e.g., /second-year-visa-jobs/
        ]
        script = (
            "(() => {\n"
            "  const anchors = Array.from(document.querySelectorAll('a'));\n"
            "  return anchors.map(a => a.href).filter(Boolean);\n"
            "})()"
        )
        try:
            hrefs: List[str] = self.page.evaluate(script)
        except Exception:
            hrefs = []

        links: List[str] = []
        for h in hrefs:
            low = h.lower()
            if any(re.search(ptn, low) for ptn in patterns):
                # exclude NZ and other sites if ever present
                if 'backpackerjobboard.com.au' in low:
                    links.append(h.split('#')[0])
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for u in links:
            if u not in seen:
                unique.append(u)
                seen.add(u)
        logger.info(f'Found {len(unique)} category links')
        return unique

    def collect_job_links_from_category(self, category_url: str, max_pages: int = 30) -> List[str]:
        """Open a category page and collect job detail URLs. Tries to paginate/scroll."""
        job_links: List[str] = []
        try:
            logger.info(f'Category: {category_url}')
            self.page.goto(category_url, wait_until='domcontentloaded', timeout=90000)
            human_delay(1.2, 2.6)

            # Some categories show a filter dropdown; not critical. Ensure we scroll and collect anchors.
            pages = 0
            while pages < max_pages:
                # Gather anchors that point to job detail pages: /job/<id>/...
                try:
                    new_links = self.page.eval_on_selector_all(
                        'a',
                        "els => els.map(e => e.href).filter(h => h && /\\/job\\/\\d+\\//.test(h))"
                    ) or []
                except Exception:
                    new_links = []

                if new_links:
                    job_links.extend(new_links)

                # Try to click a pagination or load-more if present; else scroll
                clicked = False
                for sel in [
                    "a[rel='next']",
                    "a:has-text('Next')",
                    "button:has-text('Load more')",
                    ".pagination a[rel='next']",
                    "li.next a"
                ]:
                    try:
                        el = self.page.query_selector(sel)
                        if el:
                            el.scroll_into_view_if_needed()
                            human_delay(0.2, 0.6)
                            el.click()
                            self.page.wait_for_load_state('domcontentloaded', timeout=50000)
                            human_delay(0.8, 1.4)
                            clicked = True
                            break
                    except Exception:
                        continue

                if not clicked:
                    # Infinite scroll styles
                    try:
                        self.page.mouse.wheel(0, 1200)
                    except Exception:
                        pass
                    human_delay(0.6, 1.2)
                    pages += 1
                    # Heuristic: if page height no longer increases, break
                    # (Skip heavy checks; break if we haven't gained new links for 3 rounds)
                    if pages >= max_pages:
                        break
                else:
                    pages += 1

        except Exception as e:
            logger.warning(f'Failed to collect jobs from category {category_url}: {e}')

        # Deduplicate
        out: List[str] = []
        seen = set()
        for u in job_links:
            u = u.split('#')[0]
            if 'backpackerjobboard.com.au/job/' in u and u not in seen:
                out.append(u)
                seen.add(u)
        logger.info(f'Collected {len(out)} job links from category')
        return out

    # ---- Detail extraction ----
    def extract_job_detail(self, job_url: str) -> Optional[Dict]:
        try:
            self.page.goto(job_url, wait_until='domcontentloaded', timeout=90000)
            try:
                self.page.wait_for_selector('h1', timeout=20000)
            except PlaywrightTimeout:
                pass
            human_delay(0.8, 1.5)

            # Title
            title = ''
            try:
                h1 = self.page.query_selector('h1')
                if h1 and h1.text_content():
                    title = h1.text_content().strip()
            except Exception:
                pass
            if not title:
                try:
                    title = (self.page.title() or '').strip().split('|')[0]
                except Exception:
                    title = ''

            # Company
            company_name = ''
            for sel in [
                ".job-meta a[href*='/employer/']",
                "a[href*='/employer/']",
                "[class*='employer'] a",
                "[itemprop='hiringOrganization']",
            ]:
                try:
                    el = self.page.query_selector(sel)
                    if el and el.text_content():
                        company_name = el.text_content().strip()
                        break
                except Exception:
                    continue
            if not company_name:
                # Fallback: first block under title often contains company
                try:
                    meta = self.page.locator("xpath=//*[contains(@class,'job-meta') or contains(@class,'job-info')][1]")
                    if meta and meta.count() > 0:
                        txt = meta.first.inner_text().strip()
                        m = re.search(r"\n([^\n]+)\n", txt)
                        if m:
                            company_name = m.group(1).strip()
                except Exception:
                    pass
            if not company_name:
                company_name = 'Backpacker Employer'

            # Location (prefer the blue link right under the title)
            def normalize_location_text(text: str) -> str:
                if not text:
                    return ''
                t = text.strip()
                # If "2450 (Coffs Harbour NSW, Coffs Harbour)" -> pull out the paren content
                m = re.search(r"\(([^)]+)\)", t)
                if m and len(m.group(1)) > 5:
                    t = m.group(1)
                # Remove leading postcode digits
                t = re.sub(r"^\d{3,4}\s*", "", t)
                # Remove trailing country
                t = re.sub(r"\s*,\s*Australia$", "", t, flags=re.IGNORECASE)
                return t.strip()

            def looks_plausible_location(text: str) -> bool:
                if not text:
                    return False
                low = text.lower().strip()
                # Common non-location phrases that slipped in before
                bad_words = [
                    'visa', 'available', 'dates', 'apply', 'experience', 'year', 'years',
                    'week', 'day', 'hour', 'salary', 'not specified', 'job', 'jobs',
                    'contact', 'email', 'phone', 'website', 'www', 'http', 'https',
                    'function',
                ]
                if any(w in low for w in bad_words):
                    return False
                # Reject script-like or noisy strings
                if re.search(r"[(){}\[\]=<>]", text):
                    return False
                if re.search(r"[#/\\;:_@|]", text):
                    return False
                # Avoid common DOM/CSS words
                if re.search(r"\b(form|table|tbody|tr|td|div|span|header|footer|nav|ul|li)\b", low):
                    return False
                # Avoid sentences
                if ' or ' in low or ' and ' in low:
                    return False
                # Too many commas likely not a simple location
                if text.count(',') > 2:
                    return False
                # Must either contain a state, or a comma-separated locality/region
                state_abbrev = r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b"
                state_names = r"\b(New South Wales|Victoria|Queensland|South Australia|Western Australia|Tasmania|Northern Territory|Australian Capital Territory)\b"
                has_state = re.search(state_abbrev, text, re.IGNORECASE) or re.search(state_names, text, re.IGNORECASE)
                has_comma = ',' in text
                # Reasonable length
                if len(text) < 2 or len(text) > 80:
                    return False
                # Limit total words to avoid capturing long sentences
                if len(re.findall(r"[A-Za-z']+", text)) > 10:
                    return False
                if has_comma:
                    # Require the last comma segment to be a state (name or abbrev)
                    last = [p.strip() for p in text.split(',') if p.strip()]
                    if last:
                        tail = last[-1]
                        if not (re.search(state_abbrev, tail, re.IGNORECASE) or re.search(state_names, tail, re.IGNORECASE)):
                            return False
                        return True
                    return False
                else:
                    # Without comma, require explicit state mention
                    return bool(has_state)

            def read_location_near_title() -> str:
                try:
                    # Gather anchors that visually appear within 250px below the H1
                    anchors = self.page.evaluate(
                        """
                        () => {
                          const h1 = document.querySelector('h1');
                          if (!h1) return [];
                          const top = h1.getBoundingClientRect().top;
                          const limit = top + 250; // limit to the header zone
                          const all = Array.from(document.querySelectorAll('a'));
                          return all.map(el => ({
                            href: el.href || '',
                            text: (el.textContent || '').trim(),
                            y: el.getBoundingClientRect().top
                          })).filter(a => a.y > top && a.y < limit);
                        }
                        """
                    ) or []
                except Exception:
                    anchors = []

                def looks_like_location(text: str, href: str) -> bool:
                    if not text:
                        return False
                    if 'visa' in text.lower():
                        return False
                    # Exclude category links like "fruit picking jobs"
                    if 'jobs' in text.lower() or ('-jobs' in href.lower()):
                        return False
                    # Prefer strings that contain Aussie state abbreviations or a comma
                    if re.search(r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b", text, re.IGNORECASE):
                        return True
                    return ',' in text

                for a in anchors:
                    if looks_like_location(a.get('text',''), a.get('href','')):
                        return normalize_location_text(a['text'])
                return ''

            location_text = read_location_near_title()
            if not location_text:
                # Strong explicit XPath: first anchor after H1 that links to a location page under /jobs/,
                # excluding category/taxonomy anchors with 'jobs' in visible text
                try:
                    loc = self.page.locator(
                        "xpath=(//h1/following::a[contains(@href,'/jobs/') and not(contains(translate(normalize-space(.), 'JOBS', 'jobs'),'jobs'))][1])"
                    )
                    if loc and loc.count() > 0:
                        t = normalize_location_text(loc.first.text_content() or '')
                        # Keep only if it looks like a location
                        if looks_plausible_location(t):
                            location_text = t
                except Exception:
                    pass
            if not location_text:
                # Fallback explicit: anchors whose href contains /location/
                try:
                    el = self.page.query_selector("a[href*='/location/']")
                    if el and el.text_content():
                        t = normalize_location_text(el.text_content())
                        if looks_plausible_location(t):
                            location_text = t
                except Exception:
                    pass
            if not location_text:
                # Postcode line sometimes contains the canonical location in parentheses
                try:
                    node = self.page.locator("xpath=//*[contains(translate(., 'POSTCODE', 'postcode'),'postcode')]")
                    if node and node.count() > 0:
                        txt = node.first.inner_text().strip()
                        m = re.search(r"\(([^)]+)\)", txt)
                        if m:
                            t = normalize_location_text(m.group(1))
                            if looks_plausible_location(t):
                                location_text = t
                except Exception:
                    pass
            if not location_text:
                # JSON-LD structured data sometimes contains jobLocation
                try:
                    jsonld_texts = self.page.eval_on_selector_all("script[type='application/ld+json']", "els => els.map(e => e.textContent || '')") or []
                except Exception:
                    jsonld_texts = []
                for raw in jsonld_texts:
                    try:
                        data = json.loads(raw.strip())
                    except Exception:
                        continue
                    # JSON-LD may be a list
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        try:
                            if item.get('@type') and 'JobPosting' in str(item.get('@type')):
                                jl = item.get('jobLocation') or item.get('applicantLocationRequirements') or {}
                                if isinstance(jl, list) and jl:
                                    jl = jl[0]
                                if isinstance(jl, dict):
                                    addr = jl.get('address') or {}
                                    if isinstance(addr, dict):
                                        city = addr.get('addressLocality') or ''
                                        region = addr.get('addressRegion') or ''
                                        parts = [p for p in [city, region] if p]
                                        t = normalize_location_text(', '.join(parts))
                                        if looks_plausible_location(t):
                                            location_text = t
                                            break
                        except Exception:
                            continue
                    if location_text:
                        break
            if not location_text:
                # Elements whose class/id includes 'location' or label 'Location: ...'
                try:
                    candidates = self.page.evaluate(
                        """
                        () => {
                          const out = [];
                          const els = Array.from(document.querySelectorAll('*'))
                            .filter(el => !['SCRIPT','STYLE','NOSCRIPT','TEMPLATE'].includes(el.tagName));
                          for (const el of els) {
                            const name = `${el.className || ''} ${el.id || ''}`;
                            if (/location/i.test(name)) {
                              const t = (el.textContent || '').trim();
                              if (t) out.push(t);
                            }
                          }
                          // Label-value patterns
                          for (const el of els) {
                            const t = (el.textContent || '').trim();
                            if (/^\s*Location\s*[:\-]/i.test(t)) {
                              out.push(t.replace(/^\s*Location\s*[:\-]\s*/i, ''));
                            }
                          }
                          return out.slice(0, 50);
                        }
                        """
                    ) or []
                except Exception:
                    candidates = []
                for raw in candidates:
                    # Split on common separators to isolate the location portion
                    parts = re.split(r"[\n\|]+", raw)
                    for part in parts:
                        t = normalize_location_text(part)
                        if looks_plausible_location(t):
                            location_text = t
                            break
                    if location_text:
                        break
            if not location_text:
                # Global scan: anchors under /jobs/ that don't look like category (-jobs) and look like a city/state
                try:
                    all_as = self.page.eval_on_selector_all(
                        'a',
                        "els => els.map(e => ({href: e.href || '', text: (e.textContent || '').trim()}))"
                    ) or []
                except Exception:
                    all_as = []
                for a in all_as:
                    href = a.get('href','').lower()
                    text = a.get('text','').strip()
                    if '/jobs/' in href and '-jobs' not in href and text and 'jobs' not in text.lower():
                        if looks_plausible_location(text):
                            location_text = normalize_location_text(text)
                            break
            if location_text:
                location_text = re.sub(r"\s*,\s*Australia$", "", location_text, flags=re.IGNORECASE)
            # Final validation guard to avoid saving junk
            if location_text and not looks_plausible_location(location_text):
                location_text = ''

            # Tags/Categories (from buttons under title)
            category_raw = ''
            try:
                tag_el = self.page.query_selector("a[href*='-jobs']")
                if tag_el and tag_el.text_content():
                    category_raw = tag_el.text_content().strip()
            except Exception:
                pass

            # Description block
            description = ''
            for sel in [
                ".job-description",
                "article .content",
                "[class*='job'] [class*='description']",
                "main",
                "article",
            ]:
                try:
                    el = self.page.query_selector(sel)
                    if el and el.inner_text():
                        txt = el.inner_text().strip()
                        if len(txt) > 120:
                            description = self._clean_description(txt)
                            break
                except Exception:
                    continue
            if not description:
                description = 'No description available'

            # Salary text (heuristic)
            body_text = (self.page.text_content('body') or '')
            salary_text = ''
            for p in [
                r'\$[\d,]+\s*-\s*\$[\d,]+\s*(?:per\s+)?(hour|day|week|month|year|annum)',
                r'\$[\d,]+\s*(?:per\s+)?(hour|day|week|month|year|annum)'
            ]:
                m = re.search(p, body_text, flags=re.IGNORECASE)
                if m:
                    salary_text = m.group(0)
                    break

            # Posted info
            posted_ago = ''
            for sel in [".posted", "[class*='published']", "[class*='date']"]:
                try:
                    el = self.page.query_selector(sel)
                    if el and el.text_content():
                        posted_ago = el.text_content().strip()
                        break
                except Exception:
                    continue

            job = {
                'title': title,
                'company_name': company_name,
                'location': location_text,
                'external_url': job_url,
                'description': description,
                'salary_text': salary_text,
                'posted_ago': posted_ago,
                'external_source': 'backpackerjobboard.com.au',
                'category_raw': category_raw
            }
            logger.info(f"Parsed: {title} | {company_name} | {location_text}")
            return job
        except Exception as e:
            logger.warning(f'Failed to parse job {job_url}: {e}')
            return None

    # ---- Persistence ----
    def _parse_salary(self, s: str) -> Tuple[Optional[int], Optional[int], str, str]:
        if not s:
            return None, None, 'AUD', 'yearly'
        try:
            nums = [int(x.replace(',', '')) for x in re.findall(r'\d{2,3}(?:,\d{3})*', s)]
            if not nums:
                return None, None, 'AUD', 'yearly'
            mn, mx = (min(nums), max(nums)) if len(nums) > 1 else (nums[0], nums[0])
            period = 'yearly'
            low = s.lower()
            if 'hour' in low or 'hr' in low:
                period = 'hourly'
            elif 'day' in low:
                period = 'daily'
            elif 'week' in low:
                period = 'weekly'
            elif 'month' in low:
                period = 'monthly'
            return mn, mx, 'AUD', period
        except Exception:
            return None, None, 'AUD', 'yearly'

    def _get_or_create_company(self, name: str) -> Optional[Company]:
        try:
            company, _ = Company.objects.get_or_create(
                name=name,
                defaults={'slug': slugify(name)}
            )
            return company
        except Exception as e:
            logger.error(f'Company creation failed: {e}')
            return None

    def _get_or_create_location(self, loc: str) -> Optional[Location]:
        if not loc:
            return None
        try:
            location, _ = Location.objects.get_or_create(
                name=loc,
                defaults={'city': loc, 'country': 'Australia'}
            )
            return location
        except Exception as e:
            logger.error(f'Location creation failed: {e}')
            return None

    def save_job(self, job: Dict) -> bool:
        try:
            with transaction.atomic():
                existing = JobPosting.objects.filter(external_url=job['external_url']).first()
                if existing:
                    # If previously saved without a location, or with a dubious one, patch it now
                    new_loc_text = (job.get('location') or '').strip()
                    current_loc_text = (existing.location.name if existing.location else '').strip()
                    def looks_plausible(text: str) -> bool:
                        # mirror the heuristic used in extraction
                        if not text:
                            return False
                        low = text.lower()
                        if any(w in low for w in ['visa','available','dates','apply','experience','salary','job','jobs','not specified','function']):
                            return False
                        if re.search(r"[(){}\[\]=<>#/\\;:_@|]", text):
                            return False
                        if re.search(r"\b(form|table|tbody|tr|td|div|span|header|footer|nav|ul|li)\b", low):
                            return False
                        if ' or ' in low or ' and ' in low:
                            return False
                        if text.count(',') > 2:
                            return False
                        if len(text) < 2 or len(text) > 80:
                            return False
                        # With comma, require state in last segment; otherwise require explicit state
                        state_abbrev = r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b"
                        state_names = r"\b(New South Wales|Victoria|Queensland|South Australia|Western Australia|Tasmania|Northern Territory|Australian Capital Territory)\b"
                        if ',' in text:
                            parts = [p.strip() for p in text.split(',') if p.strip()]
                            if not parts:
                                return False
                            tail = parts[-1]
                            return bool(re.search(state_abbrev, tail, re.IGNORECASE) or re.search(state_names, tail, re.IGNORECASE))
                        return bool(re.search(state_abbrev, text, re.IGNORECASE) or re.search(state_names, text, re.IGNORECASE))

                    should_update = False
                    if not existing.location and new_loc_text:
                        should_update = True
                    elif new_loc_text and looks_plausible(new_loc_text) and (not looks_plausible(current_loc_text) or new_loc_text != current_loc_text):
                        should_update = True

                    if should_update:
                        loc_obj = self._get_or_create_location(new_loc_text)
                        if loc_obj:
                            existing.location = loc_obj
                            existing.save(update_fields=['location'])
                            logger.info(f"Updated location for existing job: {existing.title} -> {loc_obj.name}")
                            return True
                    logger.info(f"Duplicate URL, skipping: {job['external_url']}")
                    return False

                company = self._get_or_create_company(job['company_name'])
                if not company:
                    return False
                location = self._get_or_create_location(job.get('location') or '')

                salary_min, salary_max, currency, salary_type = self._parse_salary(job.get('salary_text', ''))
                job_category = JobCategorizationService.categorize_job(job['title'], job.get('description', ''))
                tags = ','.join(JobCategorizationService.get_job_keywords(job['title'], job.get('description', ''))[:10])

                JobPosting.objects.create(
                    title=job['title'],
                    description=job.get('description', ''),
                    company=company,
                    posted_by=self.scraper_user,
                    location=location,
                    job_category=job_category,
                    job_type='casual',
                    work_mode='',
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=currency,
                    salary_type=salary_type,
                    salary_raw_text=job.get('salary_text', ''),
                    external_source='backpackerjobboard.com.au',
                    external_url=job['external_url'],
                    status='active',
                    posted_ago=job.get('posted_ago', ''),
                    tags=tags,
                    additional_info={
                        'scraper': 'backpacker_playwright',
                        'category_raw': job.get('category_raw', ''),
                    }
                )
                self.saved_count += 1
                return True
        except Exception as e:
            logger.error(f'Failed saving job: {e}')
            return False

    # ---- Utilities ----
    def _clean_description(self, text: str) -> str:
        cleaned = re.sub(r"\n{3,}", "\n\n", text).strip()
        # Remove the specific footer/CTA and legal blocks the user asked to ignore
        patterns = [
            r"(?is)View\s+more\s+Jobs\s+in\s+.*?for\s+backpackers\.?\s*",
            r"(?is)Not\s+a\s+backpacker\?.*?apply via the form below\.?\s*",
            r"(?is)By displaying these images, Backpacker Job Board does not claim any ownership.*?However, any removal of content shall not constitute an admission of liability or fault by Backpacker Job Board\.",
            r"(?is)Minimum\s+Wage\s+Guarantee\s+Information\s+for\s+Fruit\s+Picking\s+Jobs:.*?(?:here\.|Report ad here)\s*",
            r"(?is)Inappropriate\?\s*Scam\?\s*Report ad here"
        ]
        for p in patterns:
            cleaned = re.sub(p, "", cleaned, flags=re.MULTILINE)
        # Tidy whitespace again
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned

    # ---- Main runner ----
    def run(self) -> None:
        start = datetime.now()
        try:
            self.setup()
            if not self.goto_home():
                return
            cat_links = self.collect_category_links()
            if not cat_links:
                logger.warning('No category links found; exiting')
                return

            # Process each category
            for cidx, cat in enumerate(cat_links, start=1):
                job_links = self.collect_job_links_from_category(cat)
                if not job_links:
                    continue
                for jidx, url in enumerate(job_links, start=1):
                    if self.job_limit and self.saved_count >= self.job_limit:
                        break
                    detail = self.extract_job_detail(url)
                    if not detail:
                        continue
                    self.save_job(detail)
                    human_delay(0.4, 1.1)
                if self.job_limit and self.saved_count >= self.job_limit:
                    break
        finally:
            self.close()
            elapsed = datetime.now() - start
            logger.info(f'Backpacker scraping finished in {elapsed}. Saved: {self.saved_count}')


def run_backpacker_scraper(job_limit: Optional[int] = None, headless: bool = True) -> None:
    scraper = BackpackerScraper(headless=headless, job_limit=job_limit)
    scraper.run()


if __name__ == '__main__':
    # You can adjust job_limit for quick testing, e.g., 20
    run_backpacker_scraper(job_limit=None, headless=True)


