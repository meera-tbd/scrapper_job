#!/usr/bin/env python
"""
eFinancialCareers Australia Job Scraper (Playwright)

Scrapes the AU listing page, opens each job detail page, and stores original
and normalized data into the Django models `Company`, `Location`, and
`JobPosting` following the existing backend flow.

Data captured per job (no static fallbacks):
- title
- company name (parsed from job header)
- description (full text cleaned)
- location (normalized to `Location`)
- salary (min/max/currency/type + raw text)
- job_type (mapped from terms like Permanent/Contract/etc.)
- work_mode (Remote/Hybrid/In-Office if present)
- posted_ago and date_posted
- external_url and external_id

Usage:
  python script/scrape_efinancialcareers.py [max_jobs] [search_url]

If search_url is omitted, defaults to AU-wide listing with salary shown, e.g.:
  https://www.efinancialcareers.com.au/jobs?countryCode=AU&radius=40&radiusUnit=km&pageSize=15&currencyCode=AUD&language=en&includeUnspecifiedSalary=true
"""

import os
import sys
import re
import time
import random
import logging
from urllib.parse import urlparse, urljoin

# Django setup (same convention as other scrapers)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'

try:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    PROJECT_ROOT = os.getcwd()

sys.path.append(PROJECT_ROOT)

import django  # noqa: E402
django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from django.db import transaction, connections  # noqa: E402
from django.utils import timezone  # noqa: E402
from django.utils.text import slugify  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

from apps.companies.models import Company  # noqa: E402
from apps.core.models import Location  # noqa: E402
from apps.jobs.models import JobPosting  # noqa: E402
from apps.jobs.services import JobCategorizationService  # noqa: E402


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_efinancialcareers.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class EFinancialCareersScraper:
    """Playwright-based scraper for eFinancialCareers AU."""

    DEFAULT_SEARCH_URL = (
        'https://www.efinancialcareers.com.au/jobs?countryCode=AU&radius=40&radiusUnit=km'
        '&pageSize=15&currencyCode=AUD&language=en&includeUnspecifiedSalary=true'
    )

    def __init__(self, max_jobs: int | None = None, headless: bool = True, search_url: str | None = None):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = 'https://www.efinancialcareers.com.au'
        self.search_url = search_url or self.DEFAULT_SEARCH_URL
        self.scraper_user: User | None = None
        self.scraped_count = 0

    # --- Generic helpers -------------------------------------------------
    def human_like_delay(self, min_s=0.8, max_s=2.0):
        time.sleep(random.uniform(min_s, max_s))

    def setup_user(self):
        self.scraper_user, _ = User.objects.get_or_create(
            username='efinancialcareers_scraper',
            defaults={
                'email': 'scraper@efinancialcareers.local',
                'first_name': 'eFinancialCareers',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_company(self, name: str | None) -> Company:
        clean = (name or '').strip() or 'Unknown Company'
        slug = slugify(clean)
        company, _ = Company.objects.get_or_create(
            slug=slug,
            defaults={
                'name': clean,
                'description': f'{clean} - Jobs via eFinancialCareers',
                'website': '',
                'company_size': 'medium'
            }
        )
        return company

    def get_or_create_location(self, location_text: str | None) -> Location | None:
        if not location_text:
            return None
        text = re.sub(r'\s+', ' ', location_text.strip())
        # Expand common AU abbreviations
        abbrev = {
            'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland', 'SA': 'South Australia',
            'WA': 'Western Australia', 'TAS': 'Tasmania', 'NT': 'Northern Territory',
            'ACT': 'Australian Capital Territory',
        }
        for k, v in abbrev.items():
            text = re.sub(rf'\b{k}\b', v, text, flags=re.IGNORECASE)

        city = ''
        state = ''
        if ',' in text:
            parts = [p.strip() for p in text.split(',')]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        elif ' - ' in text:
            parts = [p.strip() for p in text.split(' - ', 1)]
            if len(parts) == 2:
                state, city = parts[0], parts[1]
        else:
            # If only a state or city provided
            state = text

        name = f"{city}, {state}" if city and state else (state or city)
        # Enforce DB max length for Location.name (100)
        safe_name = (name or '')[:100]
        location, _ = Location.objects.get_or_create(
            name=safe_name,
            defaults={'city': city[:100], 'state': state[:100], 'country': 'Australia'}
        )
        return location

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
        # If only a non-numeric indicator like 'Competitive', keep raw and skip numeric parsing
        if not re.search(r'[0-9]', text):
            return result
        if re.search(r'hour', text, re.IGNORECASE):
            result['salary_type'] = 'hourly'
        elif re.search(r'week', text, re.IGNORECASE):
            result['salary_type'] = 'weekly'
        elif re.search(r'month', text, re.IGNORECASE):
            result['salary_type'] = 'monthly'
        elif re.search(r'year|annum|pa|p\.a\.', text, re.IGNORECASE):
            result['salary_type'] = 'yearly'
        # Support k-notation too
        k_match = re.findall(r'(\d{2,3})\s*k', text, flags=re.IGNORECASE)
        if k_match:
            values = []
            for n in k_match:
                try:
                    values.append(float(n) * 1000)
                except Exception:
                    continue
            if values:
                result['salary_min'] = min(values)
                result['salary_max'] = max(values)
                return result

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

    def map_job_type(self, text: str | None) -> str:
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
        if 'permanent' in t or 'full-time' in t or 'full time' in t:
            return 'full_time'
        return 'full_time'

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
        lines = [ln.strip() for ln in text.split('\n')]
        cleaned = []
        drop_exact = {'Apply now', 'Apply Now', 'Save'}
        for ln in lines:
            if not ln or ln in drop_exact:
                continue
            if ln.lower().startswith(('apply now', 'save job')) and len(ln) <= 40:
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    # --- Page extraction -------------------------------------------------
    def extract_job_links_from_search(self, page) -> list[str]:
        """Collect unique job detail links from the search page via scrolling."""
        links = set()
        try:
            page.goto(self.search_url, wait_until='domcontentloaded', timeout=45000)
            self.human_like_delay(1.0, 2.0)

            last_height = 0
            for _ in range(18):  # several scrolls to load many cards
                anchors = page.query_selector_all('a[href]')
                for a in anchors:
                    href = a.get_attribute('href') or ''
                    if not href or href.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    low = href.lower()
                    # eFinancialCareers job detail sample: /jobs-Australia-...id23096595
                    if '/jobs-' in low and '.id' in low:
                        full = href if low.startswith('http') else urljoin(self.base_url, href)
                        links.add(full)
                try:
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                except Exception:
                    break
                self.human_like_delay(0.6, 1.2)
                try:
                    new_height = page.evaluate('() => document.body.scrollHeight')
                    if new_height == last_height:
                        break
                    last_height = new_height
                except Exception:
                    break
        except Exception as e:
            logger.warning(f"Search extraction warning: {e}")
        return list(sorted(links))

    def _header_text(self, page) -> str:
        """Return a compact header/meta text region for parsing location/salary/etc."""
        try:
            # Try common containers near title
            candidates = [
                'header',
                'section[role="banner"]',
                'main',
                'article',
                '.job-details, .job-header, .header, .content'
            ]
            for sel in candidates:
                el = page.query_selector(sel)
                if el:
                    txt = (el.inner_text() or '').strip()
                    if txt and len(txt) > 80:
                        return re.sub(r'\s+', ' ', txt)
        except Exception:
            pass
        try:
            body = page.inner_text('body')
            return re.sub(r'\s+', ' ', body.strip())
        except Exception:
            return ''

    def _first_location_guess(self, text: str) -> str:
        # Use bullet-separated tokens to avoid greedy matches across title/company
        tokens = [t.strip() for t in re.split(r'\s*•\s*', text) if t.strip()]
        # Match tokens that look like a location, e.g., "Adelaide, Australia"
        for tok in tokens:
            if re.search(r'^[A-Za-z .&\-/]+,\s*(Australia|New Zealand)\b', tok):
                return tok
        # Fallback: look for City, State style
        states = [
            'New South Wales', 'Victoria', 'Queensland', 'South Australia', 'Western Australia',
            'Tasmania', 'Northern Territory', 'Australian Capital Territory'
        ]
        for tok in tokens:
            for state in states:
                if re.search(rf'^[A-Za-z .&\-/]+,\s*{re.escape(state)}\b', tok):
                    return tok
        return ''

    def _meta_from_header(self, header_text: str, title: str) -> tuple[str, str, str, str]:
        """Extract (company, location, salary_text, job_type_text) from header tokens.

        We split on bullet separators to keep tokens localized and avoid title bleed.
        """
        tokens = [t.strip() for t in re.split(r'\s*•\s*', header_text) if t.strip()]

        # Location (prefer token match)
        location_text = ''
        loc_index = -1
        # Try to find a token that looks like a location
        loc_token_regex = re.compile(r'^[A-Za-z .&\-/]+,\s*(Australia|New Zealand|New\s+Zealand)$', re.IGNORECASE)
        for i, tok in enumerate(tokens):
            if loc_token_regex.search(tok):
                location_text = tok
                loc_index = i
                break
        if not location_text:
            # Fallback to free-text search
            location_text = self._first_location_guess(header_text)
            # try to align to closest token containing it
            if location_text:
                for i, tok in enumerate(tokens):
                    if location_text.lower() in tok.lower():
                        loc_index = i
                        location_text = tok
                        break

        # Company candidate: previous token before location that isn't title/buttons
        company_name = ''
        if loc_index > 0:
            cand = tokens[loc_index - 1]
            # Remove title fragment if present
            cand = cand.replace(title, '').strip()
            if not re.search(r'^(Apply now|Save|Posted|Jobs on|Search Jobs|POSTED BY|Recruiter)$', cand, re.IGNORECASE):
                company_name = cand
        # Fallback: choose the first plausible token not matching location/salary/time/action
        if not company_name:
            for tok in tokens[:5]:  # early tokens generally contain company
                if tok == title:
                    continue
                if re.search(r'(Apply now|Save|POSTED BY|Recruiter|Posted|Australia|New Zealand)', tok, re.IGNORECASE):
                    continue
                if re.search(r'(\$|\b\d{2,3}k\b|\baud\b|competitive)', tok, re.IGNORECASE):
                    continue
                if ',' in tok and re.search(r'\b(Australia|New Zealand)\b', tok, re.IGNORECASE):
                    continue
                company_name = tok
                break
        # Final cleanup: collapse extra separators from titles like "Company | Branch"
        if company_name and '|' in company_name:
            parts = [p.strip() for p in company_name.split('|') if p.strip()]
            # If first part looks like a role start, prefer the last part; else keep first
            if len(parts) >= 2:
                # Heuristic: company names rarely start with verbs like 'Customer', 'Manager'
                if re.match(r'^(customer|manager|senior|junior|lead|graphic|software)\b', parts[0], re.IGNORECASE):
                    company_name = parts[-1]
                else:
                    company_name = parts[0]

        # Salary token: look for a compact fragment, not the whole header
        salary_text = ''
        for tok in tokens:
            # Prefer pure keywords
            if re.search(r'\bcompetitive\b', tok, re.IGNORECASE):
                salary_text = 'Competitive'
                break
            # Dollar amounts or k-ranges
            m = re.search(r'(\$\s*\d[\d,]*(?:\s*[-–]\s*\$?\s*\d[\d,]*)?\s*(?:per\s*(?:year|month|week|day|hour))?)', tok, re.IGNORECASE)
            if m:
                salary_text = re.sub(r'\s+', ' ', m.group(1)).strip()
                break
            # AUD amounts
            m = re.search(r'(AUD\s*\$?\s*\d[\d,]*(?:\s*[-–]\s*\$?\s*\d[\d,]*)?)', tok, re.IGNORECASE)
            if m:
                salary_text = re.sub(r'\s+', ' ', m.group(1)).strip()
                break
            # k notation
            m = re.search(r'(\d{2,3}\s*k(?:\s*[-–]\s*\d{2,3}\s*k)?)', tok, re.IGNORECASE)
            if m:
                salary_text = m.group(1).replace(' ', '')
                break

        # Job type token: Permanent/Contract/Temporary/Part-time/Full-time/Internship
        job_type_text = ''
        for tok in tokens:
            m = re.search(r'\b(Permanent|Contract|Temporary|Casual|Part[- ]?time|Full[- ]?time|Internship)\b', tok, re.IGNORECASE)
            if m:
                job_type_text = m.group(1)
                break

        return company_name, location_text, salary_text, job_type_text

    def extract_job_from_detail(self, page, job_url: str) -> dict | None:
        try:
            try:
                page.goto(job_url, wait_until='domcontentloaded', timeout=45000)
            except Exception:
                page.goto(job_url, wait_until='load', timeout=65000)
            self.human_like_delay(0.8, 1.6)

            try:
                page.wait_for_selector('h1', timeout=12000)
            except Exception:
                pass

            # Title
            title_raw = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title_raw = (h1.inner_text() or '').strip()
            except Exception:
                pass
            title = re.sub(r'\s+', ' ', (title_raw or '')).strip()[:200]

            # Header/meta text for salary/work-mode/posted/location/company
            header_text = self._header_text(page)

            # Work mode
            work_mode = ''
            if re.search(r'\bremote\b', header_text, re.IGNORECASE):
                work_mode = 'Remote'
            elif re.search(r'\bhybrid\b', header_text, re.IGNORECASE):
                work_mode = 'Hybrid'
            elif re.search(r'in[- ]?office', header_text, re.IGNORECASE):
                work_mode = 'In-Office'

            # Posted ago
            posted_ago = ''
            m = re.search(r'Posted\s+([^•|\\n]+?)\b(?:•|$)', header_text, re.IGNORECASE)
            if m:
                posted_ago = m.group(1).strip()

            # Company, location, salary, job type from tokens
            company_name, location_text, salary_text, job_type_text = self._meta_from_header(header_text, title)
            salary_parsed = self.parse_salary(salary_text)
            company = self.get_or_create_company(company_name)
            location_obj = self.get_or_create_location(location_text)

            # Description: choose the largest meaningful content region
            description = ''
            for sel in ['.description', '.job-description', 'article', 'main', 'section', '[class*="description"]', '.content']:
                try:
                    el = page.query_selector(sel)
                    if not el:
                        continue
                    txt = (el.inner_text() or '').strip()
                    if txt and len(txt) > len(description):
                        description = txt
                except Exception:
                    continue
            if not description:
                try:
                    description = page.inner_text('body')
                except Exception:
                    description = ''
            description = self.clean_description(description)

            # Job type: prefer token found; fallback to mapping from description
            job_type = self.map_job_type(job_type_text or '')

            # External id from URL: .id123456
            external_id = ''
            try:
                m = re.search(r'\.id([0-9]{5,})', urlparse(job_url).path)
                if m:
                    external_id = m.group(1)
            except Exception:
                pass

            if not title or not description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            # Category: classify using TITLE ONLY to avoid recruiter/HR words in body
            job_category = JobCategorizationService.categorize_job(title, '')

            return {
                'title': title,
                'description': description[:8000],
                'company': company,
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f'efc_{external_id}' if external_id else f'efc_{hash(job_url)}',
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': work_mode,
                'posted_ago': posted_ago[:50],
                'additional_info': {'header_text': header_text}
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    def canonicalize_url(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme or 'https'}://{parsed.netloc}{parsed.path}"[:200]
        except Exception:
            return url[:200]

    def save_job(self, data: dict) -> JobPosting | None:
        try:
            with transaction.atomic():
                external_url = self.canonicalize_url(data['external_url'])
                existing = JobPosting.objects.filter(external_url=external_url).first()
                if existing:
                    logger.info(f"Already exists, skipping: {existing.title}")
                    return existing
                job = JobPosting.objects.create(
                    title=data['title'],
                    description=data['description'],
                    company=data['company'],
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
                    external_source='efinancialcareers.com.au',
                    external_url=external_url,
                    external_id=data['external_id'][:100],
                    status='active',
                    posted_ago=data['posted_ago'],
                    date_posted=data['date_posted'],
                    tags='',
                    additional_info=data.get('additional_info', {})
                )
                logger.info(f"Saved job: {job.title} at {job.company.name}")
                return job
        except Exception as e:
            logger.error(f"DB save error: {e}")
            return None

    # --- Orchestration ---------------------------------------------------
    def scrape(self) -> int:
        logger.info('Starting eFinancialCareers scraping...')
        self.setup_user()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
                )
            )
            page = context.new_page()
            try:
                links = self.extract_job_links_from_search(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning('No job links found on search page.')
                for i, job_url in enumerate(links):
                    if self.max_jobs and self.scraped_count >= self.max_jobs:
                        break
                    detail_page = context.new_page()
                    try:
                        data = self.extract_job_from_detail(detail_page, job_url)
                    finally:
                        try:
                            detail_page.close()
                        except Exception:
                            pass
                    if data:
                        saved = self.save_job(data)
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
    search_url = None
    if len(sys.argv) >= 2:
        try:
            max_jobs = int(sys.argv[1]) if sys.argv[1] else None
        except ValueError:
            # If first arg isn't an int, treat it as URL and shift
            search_url = sys.argv[1]
            if len(sys.argv) >= 3:
                try:
                    max_jobs = int(sys.argv[2])
                except ValueError:
                    max_jobs = None
    if search_url is None and len(sys.argv) >= 3:
        search_url = sys.argv[2]

    scraper = EFinancialCareersScraper(max_jobs=max_jobs, headless=True, search_url=search_url)
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


