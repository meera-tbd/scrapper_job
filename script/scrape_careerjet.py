"""
Careerjet scraper rewritten to Playwright and integrated with
`apps.jobs.models.JobPosting` relations (Company, Location, User).

Focus: extract Job Title and Description from listing/detail pages
and save robustly into the database with `external_source='careerjet.com.au'`.
"""

import os
import sys
import re
import time
import random
import logging
from datetime import datetime, timedelta

# Django setup for this project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
# Allow synchronous ORM access even if an event loop is present (Playwright)
os.environ.setdefault('DJANGO_ALLOW_ASYNC_UNSAFE', 'true')

import django

django.setup()

from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils import timezone

from playwright.sync_api import sync_playwright

from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.models import JobPosting


def _human_wait(min_seconds: float = 0.8, max_seconds: float = 2.2) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


class CareerjetPlaywrightScraper:
    def __init__(self, max_jobs: int = 40, headless: bool = True) -> None:
        self.base_url = 'https://www.careerjet.com.au'
        # Use the site search endpoint for Australia (matches user's requested URL)
        self.start_url = f'{self.base_url}/jobs?s=&l=Australia'
        self.max_jobs = max_jobs
        self.headless = headless
        self.scraper_user = self._get_or_create_scraper_user()
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('careerjet_scraper.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout),
            ],
        )

    # ----- Model helpers -----
    def _get_or_create_scraper_user(self):
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='careerjet_scraper',
            defaults={'email': 'scraper@careerjet.local', 'first_name': 'Careerjet', 'last_name': 'Scraper'},
        )
        return user

    def _get_or_create_company(self, company_name: str | None) -> Company:
        name = (company_name or '').strip() or 'Unknown Company'
        existing = Company.objects.filter(name__iexact=name).first()
        if existing:
            return existing
        return Company.objects.create(name=name, company_size='medium')

    def _get_or_create_location(self, location_text: str | None) -> Location | None:
        text = (location_text or '').strip()
        if not text:
            return None
        existing = Location.objects.filter(name__iexact=text).first()
        if existing:
            return existing
        parts = [p.strip() for p in text.split(',')]
        city = parts[0] if parts else text
        state = parts[1] if len(parts) > 1 else ''
        return Location.objects.create(name=text, city=city, state=state, country='Australia')

    # ----- Parsing helpers -----
    def _parse_relative_date(self, raw: str | None) -> datetime:
        if not raw:
            return timezone.now()
        s = raw.strip().lower()
        now = timezone.now()
        m = re.search(r'(\d+)\s*(hour|day|week|month)', s)
        if m:
            n = int(m.group(1))
            unit = m.group(2)
            if unit == 'hour':
                return now - timedelta(hours=n)
            if unit == 'day':
                return now - timedelta(days=n)
            if unit == 'week':
                return now - timedelta(weeks=n)
            if unit == 'month':
                return now - timedelta(days=n * 30)
        return now

    def _detect_job_type(self, page_text: str) -> str:
        t = (page_text or '').lower()
        if 'part-time' in t or 'part time' in t:
            return 'part_time'
        if 'permanent' in t:
            return 'full_time'
        if 'casual' in t:
            return 'casual'
        if 'contract' in t or 'fixed term' in t or 'temporary' in t:
            if 'temporary' in t:
                return 'temporary'
            return 'contract'
        if 'intern' in t or 'trainee' in t:
            return 'internship'
        return 'full_time'

    def _parse_salary_values(self, salary_text: str) -> tuple[int | None, int | None, str, str]:
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        try:
            nums = re.findall(r'\d+(?:,\d+)?', salary_text)
            values = [int(n.replace(',', '')) for n in nums]
            if not values:
                return None, None, 'AUD', 'yearly'
            if len(values) >= 2:
                mn, mx = min(values), max(values)
            else:
                mn = mx = values[0]
            period = 'yearly'
            low = salary_text.lower()
            if any(x in low for x in ['hour', 'hr']):
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

    def _guess_location(self, page_text: str) -> str:
        if not page_text:
            return ''
        # Scan first lines for a city, STATE pattern
        for line in page_text.split('\n')[:80]:
            line = line.strip()
            m = re.search(r'([A-Za-z .\-/]+),\s*(NSW|VIC|QLD|WA|SA|NT|TAS|ACT)\b', line)
            if m and 4 <= len(m.group(0)) <= 80:
                return m.group(0)
        return ''

    def _extract_from_jsonld(self, page) -> dict:
        """Extract company, location, salary, employmentType from JobPosting JSON-LD if present."""
        result: dict = {}
        try:
            scripts = page.query_selector_all("script[type='application/ld+json']") or []
        except Exception:
            scripts = []
        import json
        for s in scripts:
            try:
                content = s.inner_text() or ''
            except Exception:
                continue
            if not content:
                continue
            try:
                data = json.loads(content)
            except Exception:
                continue
            # Normalize to iterable
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                try:
                    if not isinstance(obj, dict):
                        continue
                    if obj.get('@type') != 'JobPosting':
                        # Sometimes wrapped in graph
                        graph = obj.get('@graph') if isinstance(obj.get('@graph'), list) else []
                        found = False
                        for g in graph:
                            if isinstance(g, dict) and g.get('@type') == 'JobPosting':
                                obj = g
                                found = True
                                break
                        if not found:
                            continue
                    # Company
                    org = obj.get('hiringOrganization') or {}
                    if isinstance(org, dict):
                        name = (org.get('name') or '').strip()
                        if name:
                            result['company'] = name
                    # Employment type
                    emp = obj.get('employmentType')
                    if isinstance(emp, list):
                        emp = ' '.join(emp)
                    if isinstance(emp, str) and emp:
                        result['job_type_hint'] = emp
                    # Location
                    loc = obj.get('jobLocation')
                    if isinstance(loc, list) and loc:
                        loc = loc[0]
                    if isinstance(loc, dict):
                        addr = loc.get('address') or {}
                        if isinstance(addr, dict):
                            locality = (addr.get('addressLocality') or '').strip()
                            region = (addr.get('addressRegion') or '').strip()
                            country = (addr.get('addressCountry') or '').strip()
                            location_parts = [p for p in [locality, region] if p]
                            if location_parts:
                                result['location'] = ', '.join(location_parts)
                            elif country:
                                result['location'] = country
                    # Salary
                    base = obj.get('baseSalary') or {}
                    if isinstance(base, dict):
                        currency = base.get('currency') or base.get('salaryCurrency') or 'AUD'
                        value = base.get('value') or {}
                        unit = (value.get('unitText') or base.get('unitText') or '').lower()
                        unit_map = {
                            'hour': 'per hour', 'HOUR': 'per hour',
                            'day': 'per day', 'week': 'per week', 'month': 'per month',
                            'year': 'per annum', 'yearly': 'per annum', 'annum': 'per annum'
                        }
                        unit_text = unit_map.get(unit, 'per annum') if unit else 'per annum'
                        mn = value.get('minValue'); mx = value.get('maxValue'); one = value.get('value')
                        salary_text = ''
                        if mn and mx:
                            salary_text = f"{currency} {int(mn):,} - {int(mx):,} {unit_text}"
                        elif one:
                            salary_text = f"{currency} {int(one):,} {unit_text}"
                        # Do not trust JSON-LD salary for saving; keep only as hint if needed
                        if salary_text:
                            result['salary_jsonld'] = salary_text
                except Exception:
                    continue
        return result

    def _find_salary_in_text(self, text: str) -> str:
        """Return a trustworthy salary string from text or empty if none.
        Only returns when a currency amount (possibly a range) is present with a time unit.
        """
        if not text:
            return ''
        low = text.lower()
        # Quick reject common non-numeric phrases
        if 'competitive' in low and '$' not in low:
            return ''
        import re as _re
        patterns = [
            r'(?:au\$|\$)\s?\d[\d,]*(?:\.\d+)?\s*-\s*(?:au\$|\$)?\s?\d[\d,]*(?:\.\d+)?\s*(?:per\s*(?:hour|day|week|month|annum|year)|/\s*(?:hr|day|wk|mo|yr))',
            r'(?:au\$|\$)\s?\d[\d,]*(?:\.\d+)?\s*(?:per\s*(?:hour|day|week|month|annum|year)|/\s*(?:hr|day|wk|mo|yr))',
            r'\$\s?\d[\d,]*\s*-\s*\$?\s?\d[\d,]*\s*(?:p\.a\.|pa|per\s*(?:annum|year))',
        ]
        for pat in patterns:
            m = _re.search(pat, text, flags=_re.IGNORECASE)
            if m:
                return m.group(0).strip()
        return ''

    def _go_to_next_page(self, page) -> bool:
        # Try a variety of common next-page controls on Careerjet
        next_selectors = [
            "a[rel='next']",
            "a[aria-label='Next']",
            "button[aria-label='Next']",
            "a:has-text('Next')",
            "a:has-text('Next page')",
            "button:has-text('Next page')",
            "li[class*='next'] a",
            ".pagination a[rel='next']",
            ".pagination a.next",
            "nav[aria-label*='Pagination'] a[rel='next']",
        ]
        for sel in next_selectors:
            try:
                el = page.query_selector(sel)
                if el and not el.get_attribute('disabled'):
                    el.scroll_into_view_if_needed()
                    _human_wait(0.2, 0.6)
                    el.click()
                    page.wait_for_load_state('networkidle', timeout=20000)
                    _human_wait(0.4, 0.9)
                    return True
            except Exception:
                continue
        # Fallback: increment typical ?p= query param if present
        try:
            url = page.url
            import urllib.parse as _u
            parsed = _u.urlparse(url)
            qs = dict(_u.parse_qsl(parsed.query))
            p = int(qs.get('p', '1')) + 1
            qs['p'] = str(p)
            new = parsed._replace(query=_u.urlencode(qs)).geturl()
            if new != url:
                page.goto(new, wait_until='networkidle', timeout=20000)
                _human_wait(0.4, 0.9)
                return True
        except Exception:
            pass
        return False

    # ----- Navigation and extraction -----
    def _collect_listings(self, page) -> list[dict]:
        listings: list[dict] = []
        # Wait a moment for initial content
        try:
            # Nudge lazy content to load
            try:
                page.mouse.wheel(0, 1200)
                _human_wait(0.2, 0.4)
            except Exception:
                pass
            page.wait_for_selector('a', timeout=8000)
        except Exception:
            pass
        # 1) Extract per-card info
        try:
            cards = page.evaluate(
                """
                () => Array.from(document.querySelectorAll('article, .job, .result, .search-result, li'))
                .map(card => {
                  const a = card.querySelector('h2 a, h3 a, .title a, a');
                  let url = a ? (a.href || a.getAttribute('href') || '') : '';
                  const company = (card.querySelector('.company, .employer, [class*="company" i]')?.textContent || '').trim();
                  const location = (card.querySelector('.location, [class*="location" i]')?.textContent || '').trim();
                  const salary = (card.querySelector('.salary, [class*="salary" i]')?.textContent || '').trim();
                  let posted = (card.querySelector('[class*="date" i], .date')?.textContent || '').trim();
                  if (!posted) {
                    const timeNode = card.querySelector('[datetime], time');
                    if (timeNode) posted = (timeNode.textContent || '').trim();
                  }
                  return { url, company, location, salary, posted };
                })
                .filter(o => o.url)
                """
            ) or []
        except Exception:
            cards = []

        # 2) Normalize and filter URLs
        for obj in cards:
            href = obj.get('url') or ''
            if not href:
                continue
            if href.startswith('/'):
                href = f'{self.base_url}{href}'
            elif not href.startswith('http'):
                href = f'{self.base_url}/{href}'
            # Limit to actual Careerjet job ads
            if '/jobad/' in href:
                obj['url'] = href
                listings.append(obj)

        # Deduplicate by URL while preserving order
        seen = set()
        unique: list[dict] = []
        for it in listings:
            u = it['url']
            if u not in seen:
                unique.append(it)
                seen.add(u)
        return unique

    def _parse_job_detail(self, page, url: str, listing_meta: dict | None = None) -> dict | None:
        try:
            self.logger.info(f'Opening job detail: {url}')
            page.goto(url, wait_until='networkidle', timeout=35000)
        except Exception:
            self.logger.warning('Failed to open job detail page')
            return None

        _human_wait(1.0, 2.0)

        # Title
        title = ''
        for sel in ['h1', '.job-title', 'header h1', '[class*="job-title"]']:
            try:
                el = page.query_selector(sel)
                if el:
                    t = (el.inner_text() or '').strip()
                    if len(t) >= 5:
                        title = t
                        break
            except Exception:
                continue
        if not title:
            try:
                title = (page.title() or '').strip()
            except Exception:
                title = ''
        if not title or len(title) < 5:
            return None

        # Description
        description = ''
        for sel in [
            '.job-description', '.description', '.content', '.job-content', '.job-detail',
            '.jobad-description', '#jobad-description', 'main', 'article', '[class*="description"]',
        ]:
            try:
                el = page.query_selector(sel)
                if el:
                    text = (el.inner_text() or '').strip()
                    if len(text) > 200:
                        description = text
                        break
            except Exception:
                continue
        if not description:
            description = (page.inner_text('body') or '').strip()

        # Prefer structured data if present
        jsonld = self._extract_from_jsonld(page)

        # Company
        company_text = jsonld.get('company', '')
        if not company_text:
            for sel in ['.company', '[class*="company"]', '.employer', '[class*="employer"]']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        company_text = (el.inner_text() or '').strip()
                        if company_text:
                            break
                except Exception:
                    continue
        if not company_text:
            try:
                header_block = (page.inner_text('main') or page.inner_text('article') or page.inner_text('body') or '')
            except Exception:
                header_block = ''
            lines = [ln.strip() for ln in (header_block.split('\n') if header_block else []) if ln.strip()]
            if lines and title:
                for ln in lines[:10]:
                    if ln == title:
                        continue
                    lower = ln.lower()
                    if any(k in lower for k in ['full-time', 'part-time', 'permanent', 'contract', 'temporary', 'apply now', 'location']):
                        continue
                    if 3 <= len(ln) <= 60:
                        company_text = ln
                        break
        if not company_text and listing_meta:
            company_text = listing_meta.get('company') or ''
        company_text = company_text or 'Unknown Company'

        # Location
        location_text = jsonld.get('location', '')
        if not location_text:
            for sel in ['.job-location', '.location', '[class*="location"]']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        text = (el.inner_text() or '').strip()
                        if text and text.lower() != 'location':
                            location_text = text
                            break
                except Exception:
                    continue
        if not location_text:
            try:
                page_text = page.inner_text('body')
            except Exception:
                page_text = ''
            location_text = self._guess_location(page_text)

        # Posted date/ago
        posted_ago = ''
        for sel in ['.posted-date', '[class*="posted" i]', '[class*="date" i]']:
            try:
                el = page.query_selector(sel)
                if el:
                    txt = (el.inner_text() or '').strip()
                    if txt:
                        posted_ago = txt
                        break
            except Exception:
                continue

        # Salary
        # Intentionally ignore JSON-LD salary for saving; rely on visible text only
        salary_text = ''
        body_text = ''
        try:
            body_text = page.inner_text('body')
        except Exception:
            body_text = ''
        if not salary_text:
            # Use strict detector to avoid false positives like "competitive rates"
            salary_text = self._find_salary_in_text(body_text)
        if not salary_text and listing_meta:
            salary_text = self._find_salary_in_text(listing_meta.get('salary') or '')

        # Job type
        job_type_hint = (jsonld.get('job_type_hint') or '')
        job_type = self._detect_job_type(' '.join([body_text, job_type_hint]))
        if not job_type and listing_meta:
            job_type = self._detect_job_type(' '.join([listing_meta.get('posted', ''), listing_meta.get('salary', '')]))

        return {
            'title': title,
            'description': description,
            'company': company_text,
            'location': location_text,
            'job_url': url,
            'posted_ago': posted_ago,
            'salary_text': salary_text,
            'job_type': job_type,
        }

    def _save_job(self, data: dict) -> bool:
        try:
            with transaction.atomic():
                if JobPosting.objects.filter(external_url=data['job_url']).exists():
                    return False
                company = self._get_or_create_company(data.get('company'))
                location = self._get_or_create_location(data.get('location'))
                # Parse salary only if we have a trustworthy salary text
                raw_salary = data.get('salary_text', '').strip()
                smin, smax, currency, period = (None, None, 'AUD', 'yearly')
                if raw_salary:
                    smin, smax, currency, period = self._parse_salary_values(raw_salary)
                JobPosting.objects.create(
                    title=data['title'],
                    description=data['description'],
                    company=company,
                    location=location,
                    posted_by=self.scraper_user,
                    job_category='other',
                    job_type=data.get('job_type', 'full_time'),
                    salary_min=smin,
                    salary_max=smax,
                    salary_currency=currency,
                    salary_type=period,
                    salary_raw_text=raw_salary,
                    external_source='careerjet.com.au',
                    external_url=data['job_url'],
                    posted_ago=data.get('posted_ago', ''),
                    date_posted=self._parse_relative_date(data.get('posted_ago', '')),
                    status='active',
                    additional_info={'scraped_from': 'careerjet.com.au'},
                )
            return True
        except Exception:
            self.logger.exception('Failed to save job')
            return False

    def scrape(self) -> list[dict]:
        saved_jobs: list[dict] = []
        with sync_playwright() as p:
            self.logger.info('Launching browser')
            browser = p.chromium.launch(headless=self.headless, args=['--no-sandbox'])
            context = browser.new_context(
                viewport={'width': 1366, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                timezone_id='Australia/Sydney',
                locale='en-AU',
            )
            page = context.new_page()

            # Open listing page
            self.logger.info(f'Navigating to {self.start_url}')
            page.goto(self.start_url, wait_until='networkidle', timeout=45000)
            _human_wait(1.2, 2.0)

            # Collect URLs across paginated result pages
            listings: list[dict] = []
            pages_seen = 0
            while len(listings) < self.max_jobs and pages_seen < 60:
                page_listings = self._collect_listings(page)
                # Append unique by URL
                seen_urls = set(it['url'] for it in listings)
                for it in page_listings:
                    if it['url'] not in seen_urls:
                        listings.append(it)
                        seen_urls.add(it['url'])
                        if len(listings) >= self.max_jobs:
                            break
                pages_seen += 1
                if len(listings) >= self.max_jobs:
                    break
                if not self._go_to_next_page(page):
                    break
            urls = [it['url'] for it in listings]
            if not urls:
                # Dump a debug HTML to ease troubleshooting
                try:
                    html = page.content()
                    with open('careerjet_debug_page.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                except Exception:
                    pass
            urls = urls[: self.max_jobs]

            # Visit each detail page
            jobs_saved = 0
            for idx, u in enumerate(urls):
                if jobs_saved >= self.max_jobs:
                    break
                meta = listings[idx] if idx < len(listings) else None
                data = self._parse_job_detail(page, u, meta)
                if not data:
                    continue
                if self._save_job(data):
                    jobs_saved += 1
                    saved_jobs.append(data)
                    self.logger.info(f"Saved {jobs_saved}/{self.max_jobs}: {data['title']} - {data.get('company','')} ")
                _human_wait(0.7, 1.5)

            try:
                browser.close()
            except Exception:
                pass
        self.logger.info(f'Scraping finished. Saved {len(saved_jobs)} jobs.')
        return saved_jobs


def main():
    max_jobs = 30
    try:
        if len(sys.argv) > 1:
            max_jobs = int(sys.argv[1])
    except Exception:
        pass
    scraper = CareerjetPlaywrightScraper(max_jobs=max_jobs, headless=True)
    scraper.scrape()


if __name__ == '__main__':
    main()


def run(max_jobs=None, headless=True):
    """Automation entrypoint for Careerjet scraper."""
    try:
        scraper = CareerjetPlaywrightScraper(max_jobs=max_jobs, headless=headless)
        saved = scraper.scrape()
        return {
            'success': True,
            'jobs_saved': len(saved) if isinstance(saved, list) else None,
            'message': 'Careerjet scraping completed'
        }
    except Exception as e:
        try:
            logging.getLogger(__name__).error(f"Scraping failed in run(): {e}")
        except Exception:
            pass
        return {
            'success': False,
            'error': str(e)
        }


