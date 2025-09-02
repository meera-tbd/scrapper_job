#!/usr/bin/env python
"""
Healthcare Australia (healthcareaustralia.com.au) Job Scraper using Playwright.

Collects all job detail links from the main "Find a job" listing, opens each
detail page, extracts original data (title, description, location, job type,
salary, category and posted date) and saves to the Django `JobPosting` model.

Usage:
  python script/scrape_healthjobs.py [max_jobs]
"""

import os
import sys
import re
import time
import random
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse

# Django setup (align with other Playwright scrapers)
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
        logging.FileHandler('scraper_healthjobs.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class HealthcareAustraliaScraper:
    """Playwright-based scraper for Healthcare Australia job postings."""

    def __init__(self, max_jobs: int | None = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = "https://healthcareaustralia.com.au"
        self.listing_url = f"{self.base_url}/find-a-job/"
        self.company: Company | None = None
        self.scraper_user: User | None = None
        self.scraped_count = 0

    def human_like_delay(self, min_s: float = 0.8, max_s: float = 2.0):
        time.sleep(random.uniform(min_s, max_s))

    def setup_database_objects(self):
        self.company, _ = Company.objects.get_or_create(
            name="Healthcare Australia",
            defaults={
                'description': "Healthcare Australia (HCA) is a leading healthcare recruitment provider in Australia.",
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': ''
            }
        )
        self.scraper_user, _ = User.objects.get_or_create(
            username='healthcareaustralia_scraper',
            defaults={
                'email': 'scraper@hca.local',
                'first_name': 'HCA',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: str | None) -> Location | None:
        if not location_text:
            return None
        text = location_text.strip()
        text = re.sub(r'\s+', ' ', text)
        # Expand common AU state abbreviations
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

        city = ''
        state = ''
        if ' - ' in text:
            # Patterns like "VIC - Rural & Remote" or "NSW - Sydney"
            parts = [p.strip() for p in text.split(' - ', 1)]
            if len(parts) == 2:
                left, right = parts[0], parts[1]
                if left and right:
                    state = left
                    city = right
        elif ',' in text:
            parts = [p.strip() for p in text.split(',')]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        else:
            state = text

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
        if 'contract' in t or 'locum' in t or 'fixed term' in t:
            return 'contract'
        if 'temp' in t or 'temporary' in t:
            return 'temporary'
        if 'intern' in t:
            return 'internship'
        if 'free' in t:
            return 'freelance'
        if 'permanent' in t or 'full' in t:
            return 'full_time'
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
        values: list[float] = []
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

    def parse_posted_date(self, text: str | None):
        if not text:
            return None
        t = text.strip()
        fmts = [
            "%d %B %Y",
            "%d %b %Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%Y-%m-%d",
            "%d/%m/%Y",
        ]
        for fmt in fmts:
            try:
                return datetime.strptime(t, fmt)
            except Exception:
                continue
        return None

    def extract_field_from_sidebar(self, page, label: str) -> str:
        # Look inside likely containers first
        selectors = [
            'aside', '.sidebar', '.job-summary', '.summary', '[class*="sidebar"]', '[class*="summary"]'
        ]
        for sel in selectors:
            try:
                containers = page.query_selector_all(sel)
            except Exception:
                containers = []
            for c in containers:
                try:
                    text = (c.inner_text() or '').strip()
                except Exception:
                    text = ''
                if not text:
                    continue
                m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", text, re.IGNORECASE)
                if m:
                    return re.sub(r'\s+', ' ', m.group(1).strip())
        # Fallback to whole body
        try:
            body = page.inner_text('body')
            m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", body, re.IGNORECASE)
            if m:
                return re.sub(r'\s+', ' ', m.group(1).strip())
        except Exception:
            pass
        return ''

    def clean_description(self, text: str, page_title: str | None = None) -> str:
        if not text:
            return ''
        # Remove common inline navigation tokens first
        text = re.sub(r'\bView job detail\b', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\bView job details\b', '', text, flags=re.IGNORECASE)
        # Normalize whitespace
        text = re.sub(r'\s+\n', '\n', text)
        text = re.sub(r'\n\s+', '\n', text)

        lines = [ln.strip() for ln in text.split('\n')]
        drop_exact = {
            'Apply now', 'Apply Now', 'Apply', 'Save', 'Share', 'Register for jobs', 'Register For Jobs',
            'Home', 'Find a job'
        }
        drop_prefix = (
            'apply now', 'save job', 'share job', 'register for jobs',
        )
        cleaned_lines = []
        for ln in lines:
            if not ln:
                continue
            # Drop exact small UI/breadcrumb lines
            if ln in drop_exact:
                continue
            # Drop short CTA lines and social labels
            if any(ln.lower().startswith(p) for p in drop_prefix) and len(ln) <= 40:
                continue
            # Drop breadcrumb fragments that sometimes appear inline
            if re.fullmatch(r'Home\s*[›>\-/]??\s*Find a job', ln, flags=re.IGNORECASE):
                continue
            if re.fullmatch(r'Find a job', ln, flags=re.IGNORECASE):
                continue
            # If a line is only the job path like: Home  Find a job  <Title>
            if re.match(r'^Home\s+Find a job\b', ln, flags=re.IGNORECASE):
                continue
            # Drop a line that exactly equals the page title (duplicate header)
            if page_title:
                norm_ln = re.sub(r'\s+', ' ', ln).strip().lower()
                norm_title = re.sub(r'\s+', ' ', page_title).strip().lower()
                if norm_ln == norm_title:
                    continue
            cleaned_lines.append(ln)

        # Compress consecutive duplicate lines
        dedup_lines = []
        for ln in cleaned_lines:
            if not dedup_lines or dedup_lines[-1].strip().lower() != ln.strip().lower():
                dedup_lines.append(ln)

        cleaned = '\n'.join(dedup_lines)
        # Remove any residual repeated breadcrumb tokens within paragraphs
        cleaned = re.sub(r'(?:^|\n)Home\s*(?:\n|\s+)*Find a job\s*(?:\n|\s+)*', '\n', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\bView job detail[s]?\b', '', cleaned, flags=re.IGNORECASE)

        # Hard-cut everything after CTA/related-jobs sections
        cut_markers = [
            'sounds like the role for you',
            'explore another role',
            'other roles you may be interested in',
            'other roles you may be interested',
            'similar jobs',
            'related jobs',
            'more jobs',
        ]
        low = cleaned.lower()
        cut_idx = None
        for m in cut_markers:
            i = low.find(m)
            if i != -1:
                cut_idx = i if cut_idx is None else min(cut_idx, i)
        if cut_idx is not None and cut_idx > 0:
            cleaned = cleaned[:cut_idx].rstrip()

        # Normalize whitespace and compress long blank runs
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
        return cleaned.strip()

    def canonicalize_url(self, job_url: str) -> str:
        try:
            parsed = urlparse(job_url)
            path = parsed.path.rstrip('/')
            return f"{parsed.scheme}://{parsed.netloc}{path}/"
        except Exception:
            return job_url[:200]

    def extract_job_links_from_listing(self, page) -> list[str]:
        links: set[str] = set()
        try:
            page.goto(self.listing_url, wait_until="domcontentloaded", timeout=35000)
            self.human_like_delay(1.2, 2.0)

            # Scroll to load many cards
            same_count_rounds = 0
            for _ in range(40):
                try:
                    anchors = page.query_selector_all('a:has-text("View job detail"), a[href*="/find-a-job/"]')
                except Exception:
                    anchors = []
                before = len(links)
                for a in anchors:
                    try:
                        href = a.get_attribute('href') or ''
                    except Exception:
                        href = ''
                    if not href:
                        continue
                    low = href.lower()
                    if low.startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    # Accept only detail pages under /find-a-job/<slug>/
                    if '/find-a-job/' in low and low.count('/') >= 3:
                        full = href if low.startswith('http') else urljoin(self.base_url, href)
                        links.add(self.canonicalize_url(full))
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    break
                self.human_like_delay(0.7, 1.2)
                after = len(links)
                if after == before:
                    same_count_rounds += 1
                else:
                    same_count_rounds = 0
                if same_count_rounds >= 3:
                    break

            # Try next-page navigation if present
            try:
                while True:
                    if self.max_jobs and len(links) >= self.max_jobs:
                        break
                    next_el = page.query_selector('a[rel="next"], a:has-text("Next"), button:has-text("Next")')
                    if not next_el:
                        break
                    try:
                        next_el.click()
                    except Exception:
                        break
                    self.human_like_delay(1.0, 1.8)
                    anchors = page.query_selector_all('a:has-text("View job detail"), a[href*="/find-a-job/"]')
                    for a in anchors:
                        href = a.get_attribute('href') or ''
                        low = href.lower()
                        if not href or low.startswith(('mailto:', 'tel:', 'javascript:')):
                            continue
                        if '/find-a-job/' in low and low.count('/') >= 3:
                            full = href if low.startswith('http') else urljoin(self.base_url, href)
                            links.add(self.canonicalize_url(full))
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"Listing extraction warning: {e}")
        return list(sorted(links))

    def extract_job_from_detail(self, page, job_url: str) -> dict | None:
        try:
            try:
                page.goto(job_url, wait_until="domcontentloaded", timeout=40000)
            except Exception:
                page.goto(job_url, wait_until="load", timeout=60000)
            self.human_like_delay(0.9, 1.8)

            try:
                page.wait_for_selector('h1', timeout=12000)
            except Exception:
                pass

            # Title
            title = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title = (h1.inner_text() or '').strip()
            except Exception:
                pass
            if not title:
                slug = urlparse(job_url).path.rstrip('/').split('/')[-1]
                words = [w for w in re.split(r'[-_]', slug) if w]
                title = ' '.join(w.capitalize() for w in words)[:200]

            # Description: prefer a semantic container
            description = ''
            for sel in ['main', 'article', '.content', '.container', '[class*="description"]']:
                try:
                    el = page.query_selector(sel)
                    if el:
                        txt = (el.inner_text() or '').strip()
                        if txt and len(txt) > 150:
                            description = self.clean_description(txt, title)
                            break
                except Exception:
                    continue
            if not description:
                try:
                    body_text = page.inner_text('body')
                except Exception:
                    body_text = ''
                chunk = body_text.strip()
                if chunk and len(chunk) > 150:
                    description = self.clean_description(chunk, title)

            # Sidebar fields
            location_text = self.extract_field_from_sidebar(page, 'Location')
            work_type_text = self.extract_field_from_sidebar(page, 'Work Type') or self.extract_field_from_sidebar(page, 'Work type')
            salary_text = self.extract_field_from_sidebar(page, 'Salary') or self.extract_field_from_sidebar(page, 'Rate')
            listed_text = self.extract_field_from_sidebar(page, 'Listed')

            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(work_type_text or description)
            location_obj = self.get_or_create_location(location_text)
            date_posted = self.parse_posted_date(listed_text) or timezone.now()

            # Category: HCA pages usually don't show a distinct category; derive from content
            job_category = JobCategorizationService.categorize_job(title, description)

            # External id
            external_id = f"hca_{re.sub(r'[^a-z0-9]+', '_', urlparse(job_url).path.strip('/').split('/')[-1].lower())}"

            if not title or not description:
                logger.info(f"Skipping (insufficient content): {job_url}")
                return None

            return {
                'title': title.strip(),
                'description': description.strip()[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': date_posted,
                'external_url': self.canonicalize_url(job_url),
                'external_id': external_id,
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': '',
                'work_type_raw': work_type_text or '',
                'listed_raw': listed_text or '',
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    def sanitize_for_model(self, data: dict) -> dict:
        safe = dict(data)
        # Canonicalize external_url and limit sizes
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
                    external_source='healthcareaustralia.com.au',
                    external_url=safe['external_url'],
                    external_id=safe['external_id'],
                    status='active',
                    posted_ago=safe['posted_ago'],
                    date_posted=safe['date_posted'],
                    tags='',
                    additional_info={
                        'scraped_from': 'healthcare_australia',
                        'scraper_version': '1.0',
                        'work_type_raw': safe.get('work_type_raw', ''),
                        'listed_raw': safe.get('listed_raw', ''),
                    }
                )
                logger.info(f"Saved job: {job.title}")
                return job
        except Exception as e:
            logger.error(f"DB save error: {e}")
        return None

    def scrape(self) -> int:
        logger.info("Starting Healthcare Australia scraping...")
        self.setup_database_objects()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            try:
                links = self.extract_job_links_from_listing(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning("No job links found on HCA listing page.")
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
                    self.human_like_delay(0.6, 1.3)
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

    scraper = HealthcareAustraliaScraper(max_jobs=max_jobs, headless=True)
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


def run(max_jobs=None):
    """Automation entrypoint for Healthcare Australia scraper."""
    try:
        scraper = HealthcareAustraliaScraper(max_jobs=max_jobs, headless=True)
        count = scraper.scrape()
        return {
            'success': True,
            'jobs_scraped': count,
            'message': f'HCA scraping completed, saved {count} jobs'
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



