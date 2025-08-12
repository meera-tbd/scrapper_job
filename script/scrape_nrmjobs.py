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
import logging
from urllib.parse import urljoin, urlparse

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
    def __init__(self, max_jobs: int | None = None, headless: bool = True):
        self.base_url = "https://nrmjobs.com.au"
        self.search_url = f"{self.base_url}/jobs/search-jobs"
        self.max_jobs = max_jobs
        self.headless = headless
        self.scraper_user: User | None = None
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

    def get_or_create_company(self, name: str | None) -> Company:
        cname = (name or "NRM Advertiser").strip() or "NRM Advertiser"
        company, _ = Company.objects.get_or_create(
            name=cname,
            defaults={
                'description': 'Advertiser published via NRMjobs',
                'website': self.base_url,
                'company_size': 'medium',
                'logo': ''
            }
        )
        return company

    def get_or_create_location(self, text: str | None) -> Location | None:
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

    def extract_job_from_detail(self, page, job_url: str) -> dict | None:
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

            # Prefer targeted NRM body extraction; fallback to container scraping
            description = self.extract_description_from_nrm_body(body_text)
            if len(description) < 150:
                for sel in ['.job-description', '.job-details', 'article', 'main', '[class*="content"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            txt = (el.inner_text() or '').strip()
                            if txt and len(txt) > 150:
                                description = self.clean_description(txt)
                                break
                    except Exception:
                        continue

            salary_text = salary_labeled
            if not salary_text:
                try:
                    m = re.search(r"\$[0-9,].{0,60}(?:per\s+(?:annum|year|hour)|p\.?a\.|\+\s*super)", body_text, re.IGNORECASE)
                    if m:
                        salary_text = m.group(0)
                except Exception:
                    pass

            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(body_text)
            location_obj = self.get_or_create_location(location_text)

            job_category = JobCategorizationService.categorize_job(title, description)

            external_id = ''
            m = re.search(r"/jobs/(?:20\d{2})/([0-9]{5,})", urlparse(job_url).path)
            if m:
                external_id = m.group(1)

            if not title or not description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            return {
                'title': title[:200],
                'description': description[:8000],
                'company_name': advertiser,
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f"nrm_{external_id}" if external_id else f"nrm_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
            }
        except Exception as e:
            logger.error(f"Detail extraction error: {e}")
            return None

    def save_job(self, data: dict) -> JobPosting | None:
        try:
            with transaction.atomic():
                safe = self.sanitize_for_model(data)
                existing = JobPosting.objects.filter(external_url=safe['external_url']).first()
                if existing:
                    logger.info(f"Already exists, skipping: {existing.title}")
                    return existing
                company = self.get_or_create_company(safe.get('company_name'))
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
