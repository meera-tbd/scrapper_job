#!/usr/bin/env python3
"""
JobsList Australia scraper using Playwright and the professional DB schema.

- Visits https://jobslist.com.au/
- Clicks "Load more listings" until enough links are collected
- Opens each job page to extract original data (title, company, location, salary,
  posted time, full description) — no static placeholders
- Saves to models: JobPosting, Company, Location

Usage:
    python jobslist_australia_scraper.py [job_limit]
"""

import os
import sys
import re
import time
import random
import logging
from typing import List, Dict, Any, Set, Tuple
from datetime import timedelta

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
# Allow ORM usage from contexts that may be detected as async by Django during Playwright runs
os.environ.setdefault('DJANGO_ALLOW_ASYNC_UNSAFE', 'true')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import django
django.setup()

from django.db import transaction, connections
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.utils.text import slugify
from playwright.sync_api import sync_playwright

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_jobslist.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def human_delay(a: float = 0.8, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))


class JobsListScraper:
    def __init__(self, job_limit: int | None = 50, headless: bool = True) -> None:
        self.start_url = 'https://jobslist.com.au/'
        self.job_limit = job_limit
        self.headless = headless
        self.system_user = self._get_or_create_user()
        # Keep per-URL metadata captured from the listing cards (company/location)
        self._list_meta: Dict[str, Dict[str, str]] = {}

    def _get_or_create_user(self):
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='jobslist_scraper',
            defaults={'email': 'scraper@jobslist.local', 'first_name': 'JobsList', 'last_name': 'Scraper'},
        )
        return user

    # ---------------------- Parsing helpers ----------------------
    @staticmethod
    def _parse_relative_date(text: str | None):
        if not text:
            return None
        low = text.lower()
        now = timezone.now()
        if 'today' in low:
            return now.replace(hour=9, minute=0, second=0, microsecond=0)
        if 'yesterday' in low:
            return (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        m = re.search(r'(\d+)\s*(hour|day|week|month)s?\s*ago', low)
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            if unit == 'hour':
                return now - timedelta(hours=n)
            if unit == 'day':
                return (now - timedelta(days=n)).replace(minute=0, second=0, microsecond=0)
            if unit == 'week':
                return (now - timedelta(weeks=n)).replace(minute=0, second=0, microsecond=0)
            if unit == 'month':
                return (now - timedelta(days=30 * n)).replace(minute=0, second=0, microsecond=0)
        return None

    @staticmethod
    def _parse_location(text: str | None) -> Tuple[str | None, str, str, str]:
        raw = (text or '').strip()
        if not raw:
            return None, '', '', 'Australia'
        raw = re.sub(r'\s+', ' ', raw).strip(', ')
        states = {
            'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland',
            'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
            'ACT': 'Australian Capital Territory', 'NT': 'Northern Territory'
        }
        # Try "City, STATE" or "City STATE" forms
        city = ''
        state = ''
        parts = [p.strip() for p in re.split(r'[|,]', raw) if p.strip()]
        if len(parts) >= 2:
            city = parts[0]
            state_part = parts[1].upper()
            for abbr, full in states.items():
                if abbr in state_part or full.lower() in state_part.lower():
                    state = full
                    break
            if not state:
                # Handle "Brisbane Queensland" style
                for abbr, full in states.items():
                    if full.lower() in parts[1].lower():
                        state = full
                        break
        else:
            tokens = raw.split()
            if tokens:
                # e.g. "Brisbane Queensland"
                if len(tokens) >= 2:
                    maybe_state = tokens[-1]
                    for abbr, full in states.items():
                        if maybe_state.upper() == abbr or full.lower() == maybe_state.lower():
                            state = full
                            city = ' '.join(tokens[:-1])
                            break
                if not city:
                    city = raw
        name = raw if not (city and state) else f"{city}, {state}"
        return name, city, state, 'Australia'

    @staticmethod
    def _parse_salary(text: str | None) -> Tuple[int | None, int | None, str, str, str]:
        if not text:
            return None, None, 'AUD', 'yearly', ''
        low = text.lower()
        period = 'yearly'
        if any(k in low for k in ['per hour', '/hr', 'hourly', 'hour']):
            period = 'hourly'
        elif any(k in low for k in ['per day', '/day', 'daily']):
            period = 'daily'
        elif any(k in low for k in ['per week', '/wk', 'weekly']):
            period = 'weekly'
        elif any(k in low for k in ['per month', '/mo', 'monthly']):
            period = 'monthly'
        nums = [int(n.replace(',', '')) for n in re.findall(r'\d{1,3}(?:,\d{3})*', text)]
        mn = mx = None
        if nums:
            if len(nums) >= 2:
                mn, mx = min(nums), max(nums)
            else:
                mn = mx = nums[0]
        return mn, mx, 'AUD', period, text[:200]

    # ---------------------- DOM extraction ----------------------
    def _extract_job_links(self, page, max_links: int | None) -> List[str]:
        """Collect job detail links from the listing page."""
        links: List[str] = []
        try:
            # Collect links with company/location from each card to avoid mis-parsing on detail pages
            items = page.evaluate(
                r"""
                () => {
                  const toAbs = (u) => u && u.startsWith('/') ? `https://jobslist.com.au${u}` : u;
                  const stateTokens = ['NSW','VIC','QLD','WA','SA','TAS','ACT','NT',' Australia',' AU','New South Wales','Queensland','Victoria','South Australia','Western Australia','Tasmania'];
                  const results = [];
                  const seen = new Set();
                  const anchors = Array.from(document.querySelectorAll('a[href*="/job/"]'));
                  for (const a of anchors) {
                    let href = a.getAttribute('href') || '';
                    const title = (a.textContent || '').trim();
                    if (!href || !title || title.length < 5) continue;
                    href = toAbs(href);
                    if (seen.has(href)) continue;
                    const card = a.closest('article, .job-card, .result, .listing, .job, .card, .job-item, .search-result') || a.parentElement;
                    let company = '';
                    let location = '';
                    if (card) {
                      const textLines = (card.innerText || '').split('\n').map(t => t.trim()).filter(Boolean);
                      // locate title line index in the card
                      const tIdx = textLines.findIndex(ln => ln === title);
                      // company: first meaningful line after title
                      for (let i = Math.max(0, tIdx + 1); i < Math.min(textLines.length, tIdx + 6); i++) {
                        const ln = textLines[i];
                        const low = ln.toLowerCase();
                        if (!ln || ln === title) continue;
                        if (low.includes('apply now') || low.includes('posted')) continue;
                        // skip obvious location lines
                        if (stateTokens.some(s => ln.includes(s))) continue;
                        company = ln; break;
                      }
                      // location: first line after title that contains a state token
                      for (let i = Math.max(0, tIdx + 1); i < Math.min(textLines.length, tIdx + 10); i++) {
                        const ln = textLines[i];
                        if (stateTokens.some(s => ln.includes(s))) { location = ln; break; }
                      }
                      // If company accidentally includes a state token (combined line), cut before token
                      if (company && stateTokens.some(s => company.includes(s))) {
                        let cut = -1;
                        for (const s of stateTokens) { const idx = company.indexOf(s); if (idx !== -1 && (cut === -1 || idx < cut)) cut = idx; }
                        if (cut > 0) company = company.slice(0, cut).replace(/[\-–|,•]+$/,'').trim();
                      }
                      // If still missing, look for common company selectors
                      if (!company) {
                        const c = card.querySelector('.company, .job-company, .employer, .company-name, .card-company');
                        if (c && c.textContent) company = c.textContent.trim();
                      }
                    }
                    results.push({ url: href, company, location });
                    seen.add(href);
                  }
                  return results;
                }
                """
            ) or []

            seen: Set[str] = set()
            for it in items:
                href = it.get('url') or ''
                if not href or href in seen:
                    continue
                links.append(href)
                self._list_meta[href] = {
                    'company': (it.get('company') or '').strip(),
                    'location': (it.get('location') or '').strip(),
                }
                seen.add(href)
                if max_links and len(links) >= max_links:
                    break
        except Exception:
            pass
        return links

    def _extract_detail_from_dom(self, page, job_url: str) -> Dict[str, Any]:
        """Extract title/company/location/salary/posted and description from job page.
        Uses robust heuristics to avoid relying on a single selector.
        """
        result: Dict[str, Any] = {
            'title': '', 'company': '', 'location_text': '', 'salary_text': '',
            'posted_ago': '', 'description': ''
        }
        try:
            # Title
            for sel in ['h1', 'header h1', '.content h1', 'main h1']:
                el = page.query_selector(sel)
                if el:
                    txt = (el.inner_text() or '').strip()
                    if txt:
                        result['title'] = txt
                        break
            # Header text near title
            header_text = ''
            try:
                header_root = page.query_selector('main') or page.query_selector('article') or page
                header_text = (header_root.inner_text() or '')
            except Exception:
                header_text = page.inner_text('body')

            # Try to get the small block right under the title (usually contains company and location)
            meta_block = ''
            try:
                title_el = page.query_selector('h1')
                if title_el:
                    meta_block = title_el.evaluate(
                        """
                        el => {
                          let n = el.parentElement; let txt = '';
                          for (let i=0; i<4 && n; i++) { n = n.nextElementSibling; if (n) txt += '\n' + (n.innerText||''); }
                          return txt;
                        }
                        """
                    ) or ''
            except Exception:
                meta_block = ''
            if meta_block:
                header_text = meta_block + '\n' + header_text

            # Posted
            m_posted = re.search(r'Posted\s+[\w\s]+?ago', header_text, re.IGNORECASE)
            if m_posted:
                result['posted_ago'] = m_posted.group(0).strip()

            # Location: prefer matches that look like City + State (limit city to 1-3 words) and avoid job words
            state_words = '(NSW|VIC|QLD|WA|SA|TAS|ACT|NT|New South Wales|Victoria|Queensland|Western Australia|South Australia|Tasmania|Australian Capital Territory|Northern Territory)'
            location = ''
            city_state_pattern = re.compile(r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,2})\s+' + state_words)
            bad_city_tokens = {'Other', 'Sales', 'Professional', 'Manager', 'Engineer', 'Intern', 'Consultant', 'Coordinator'}
            candidates = city_state_pattern.findall(header_text)
            if candidates:
                for cand in candidates:
                    city = cand[0].strip()
                    state = cand[1].strip()
                    if any(tok in city.split() for tok in bad_city_tokens):
                        continue
                    location = f"{city} {state}"
                    break
            if not location:
                # Fallback: scan lines near the top for a line containing a state token
                lines = [ln.strip() for ln in header_text.split('\n') if ln.strip()]
                for ln in lines[:25]:
                    if any(s in ln for s in ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT', 'New South Wales', 'Queensland', 'Victoria', 'South Australia', 'Western Australia', 'Tasmania', 'Australian Capital Territory', 'Northern Territory']):
                        location = ln
                        break
            if location:
                result['location_text'] = location

            # Salary: look for $ patterns
            m_sal = re.search(r'(?:AU\$|\$)\s?\d[\d,]*(?:\s?[\-–]\s?(?:AU\$|\$)?\s?\d[\d,]*)?(?:[^\n]{0,40}(?:hour|day|week|month|annum|year))?', header_text, re.IGNORECASE)
            if m_sal:
                result['salary_text'] = m_sal.group(0).strip()

            # Company: choose a clean line near the top that is not salary/location/posted
            company = ''
            lines = [ln.strip() for ln in header_text.split('\n') if ln.strip()]
            blacklist_tokens = ['posted', 'other', 'website', 'job', 'bookmark', 'subscribe', 'aud', '/ year', 'salary', 'full time', 'part time', 'remote']
            for ln in lines[:40]:
                low = ln.lower()
                if any(t in low for t in blacklist_tokens):
                    continue
                if result['title'] and ln == result['title']:
                    continue
                if result['location_text'] and result['location_text'] in ln:
                    continue
                if re.search(state_words, ln):
                    continue
                # Prefer lines that look like company names
                if any(s in ln for s in ['Pty', 'Ltd', 'Group', 'Holdings', 'Inc', 'Pty Ltd']):
                    company = ln
                    break
                # Otherwise short proper-case names (2-5 words)
                words = ln.split()
                if 1 <= len(words) <= 5 and all(w[:1].isupper() for w in words if w.isalpha()):
                    company = ln
                    break
            # Prefer values captured from the list card if available
            meta = self._list_meta.get(job_url) or {}
            if meta.get('company'):
                result['company'] = meta['company']
            else:
                result['company'] = company
            if meta.get('location'):
                result['location_text'] = meta['location'] or result['location_text']

            # Description: prefer a specific container; fallback to main text
            description = ''
            for sel in ['#job-description, .job-description, .job-detail, .job-content', 'article', 'main']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        txt = (el.inner_text() or '').strip()
                        if txt and len(txt) > 100:
                            description = txt
                            break
                except Exception:
                    continue
            if not description:
                try:
                    description = (page.inner_text('body') or '').strip()
                except Exception:
                    description = ''
            result['description'] = self._clean_description(description)
        except Exception as e:
            logger.warning(f"Detail extraction warning: {e}")
        return result

    @staticmethod
    def _clean_description(text: str) -> str:
        """Remove footer/CTA/noise lines like Apply buttons and bookmark prompts.
        Keeps original job content intact.
        """
        if not text:
            return ''
        lines = [ln.strip() for ln in text.split('\n')]
        stop_markers = [
            'apply for job', 'apply now', 'apply for this job'
        ]
        remove_markers = [
            'login to bookmark this job', 'show more', 'website'
        ]
        cleaned: list[str] = []
        for ln in lines:
            low = ln.lower()
            if any(m in low for m in stop_markers):
                break
            if not ln:
                continue
            if low.startswith('posted '):
                continue
            if any(m in low for m in remove_markers):
                continue
            # Drop isolated short brand lines (e.g., "adidas", "Randstad")
            if len(ln) <= 25 and len(ln.split()) <= 2 and not any(ch in ln for ch in ['.', ':', '•', '-']):
                continue
            cleaned.append(ln)
        return '\n'.join(cleaned).strip()

    # ---------------------- Persistence ----------------------
    def _save_job(self, job: Dict[str, Any], job_url: str) -> bool:
        try:
            connections.close_all()
            with transaction.atomic():
                title = job.get('title', '').strip()
                company_name = (job.get('company') or 'Unknown Company').strip() or 'Unknown Company'

                # Duplicate checks
                if JobPosting.objects.filter(external_url=job_url).exists():
                    logger.info(f"Duplicate (URL) skipped: {job_url}")
                    return False
                if JobPosting.objects.filter(title=title, company__name=company_name).exists():
                    logger.info(f"Duplicate (Title+Company) skipped: {title} | {company_name}")
                    return False

                # Company
                company_obj, _ = Company.objects.get_or_create(
                    slug=slugify(company_name),
                    defaults={'name': company_name}
                )

                # Location
                location_name, city, state, country = self._parse_location(job.get('location_text'))
                location_obj = None
                if location_name:
                    location_obj, _ = Location.objects.get_or_create(
                        name=location_name,
                        defaults={'city': city, 'state': state, 'country': country}
                    )

                # Salary
                smin, smax, currency, period, raw_salary = self._parse_salary(job.get('salary_text'))

                # Category and tags
                category = JobCategorizationService.categorize_job(title, job.get('description', ''))
                tags = ', '.join(JobCategorizationService.get_job_keywords(title, job.get('description', ''))[:10])

                # Create record (slug auto-created in model.save)
                JobPosting.objects.create(
                    title=title[:200],
                    description=(job.get('description') or '')[:8000],
                    company=company_obj,
                    posted_by=self.system_user,
                    location=location_obj,
                    job_category=category,
                    job_type='full_time',  # JobsList doesn't expose consistently; refine later if needed
                    work_mode='',
                    salary_min=smin,
                    salary_max=smax,
                    salary_currency=currency,
                    salary_type=period,
                    salary_raw_text=raw_salary,
                    external_source='jobslist.com.au',
                    external_url=job_url,
                    status='active',
                    posted_ago=job.get('posted_ago', '')[:50],
                    date_posted=self._parse_relative_date(job.get('posted_ago')),
                    tags=tags,
                    additional_info={'source': 'jobslist'}
                )
                logger.info(f"Saved job: {title} | {company_name} | {location_name or '-'}")
                return True
        except Exception as e:
            logger.error(f"Failed to save job: {e}")
            return False

    # ---------------------- Main scraping flow ----------------------
    def scrape(self) -> int:
        saved = 0
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                timezone_id='Australia/Sydney',
                locale='en-AU',
            )
            page = context.new_page()

            logger.info(f"Opening {self.start_url}")
            page.goto(self.start_url, wait_until='domcontentloaded', timeout=60000)
            human_delay(1.0, 2.0)

            # Try to collect job links, clicking "Load more listings" as needed
            collected: List[str] = []
            last_count = -1
            attempts_without_growth = 0

            while True:
                # Scroll down and attempt to click load more
                try:
                    page.mouse.wheel(0, 1600)
                except Exception:
                    pass
                human_delay(0.4, 0.9)

                # Gather links on the current DOM
                links = self._extract_job_links(page, self.job_limit)
                for url in links:
                    if url not in collected:
                        collected.append(url)
                logger.info(f"Collected links: {len(collected)}")

                if self.job_limit and len(collected) >= self.job_limit:
                    break

                # Try clicking load more
                clicked = False
                for sel in ['text=Load more listings', 'a:has-text("Load more listings")', 'button:has-text("Load more listings")']:
                    try:
                        el = page.query_selector(sel)
                        if el and el.is_enabled():
                            el.scroll_into_view_if_needed()
                            human_delay(0.4, 0.9)
                            el.click()
                            page.wait_for_load_state('domcontentloaded', timeout=20000)
                            human_delay(0.8, 1.6)
                            clicked = True
                            break
                    except Exception:
                        continue

                if not clicked:
                    # No load more visible; stop if we are not growing
                    if len(collected) == last_count:
                        attempts_without_growth += 1
                    else:
                        attempts_without_growth = 0
                    last_count = len(collected)
                    if attempts_without_growth >= 2:
                        break
                # Safety cap on iterations
                if len(collected) > 2000:
                    break

            logger.info(f"Total unique job links collected: {len(collected)}")

            # Process each job link and save
            limit = self.job_limit or len(collected)
            for idx, job_url in enumerate(collected[:limit]):
                try:
                    logger.info(f"[{idx+1}/{limit}] Fetching: {job_url}")
                    page.goto(job_url, wait_until='domcontentloaded', timeout=60000)
                    human_delay(1.0, 2.0)
                    data = self._extract_detail_from_dom(page, job_url)
                    if data.get('title') and data.get('description'):
                        if self._save_job(data, job_url):
                            saved += 1
                    else:
                        logger.info("Skipping: insufficient data parsed from detail page")
                except Exception as e:
                    logger.error(f"Error processing {job_url}: {e}")
                human_delay(0.6, 1.2)

            try:
                browser.close()
            except Exception:
                pass

        logger.info(f"Jobs saved: {saved}")
        return saved


def main():
    job_limit = None
    if len(sys.argv) > 1:
        try:
            job_limit = int(sys.argv[1])
        except Exception:
            job_limit = None
    scraper = JobsListScraper(job_limit=job_limit, headless=True)
    scraper.scrape()


def run(job_limit=300, headless=True):
    """Automation entrypoint for JobsList scraper.

    Mirrors the reference run() signature so schedulers can import and execute
    without CLI args.
    """
    try:
        scraper = JobsListScraper(job_limit=job_limit, headless=headless)
        saved = scraper.scrape()
        return {
            'success': True,
            'jobs_saved': saved,
            'message': f'Saved {saved} JobsList jobs'
        }
    except Exception as e:
        logger.error(f"Scraping failed in run(): {e}")
        return {
            'success': False,
            'error': str(e),
            'message': f'Scraping failed: {e}'
        }


if __name__ == '__main__':
    main()


