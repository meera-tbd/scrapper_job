#!/usr/bin/env python3
"""
Australia Post job scraper converted to Playwright
- Only this script changed, all model relations honored.
"""

import os
import sys
import time
import re
import random
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import django
from django.db import transaction
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor

# Django setup for this project
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_auspost.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


def human_delay(min_seconds: float = 1.2, max_seconds: float = 3.5) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


class AusPostPlaywrightScraper:
    def __init__(self, job_limit: Optional[int] = 30, headless: bool = True) -> None:
        self.base_domain = 'https://jobs.auspost.com.au'
        self.search_url = f'{self.base_domain}/en_GB/careers/SearchJobs'
        self.job_limit = job_limit
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
        self.bot_user = self.get_or_create_bot_user()

    def get_or_create_bot_user(self):
        User = get_user_model()
        user, _ = User.objects.get_or_create(
            username='auspost_scraper_bot',
            defaults={
                'email': 'auspost.bot@jobscraper.local',
                'first_name': 'AusPost',
                'last_name': 'Scraper'
            }
        )
        return user

    def setup_browser(self) -> None:
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        self.context = self.browser.new_context(
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            locale='en-AU',
            timezone_id='Australia/Sydney'
        )
        self.context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
            window.chrome = { runtime: {} };
            """
        )
        self.page = self.context.new_page()

    def close_browser(self) -> None:
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if hasattr(self, '_pw') and self._pw:
                self._pw.stop()
        except Exception:
            pass

    def navigate_to_search(self) -> bool:
        try:
            logger.info(f'Navigating to {self.search_url}')
            self.page.goto(self.search_url, wait_until='networkidle', timeout=90000)
            # Dismiss cookie banners if present
            try:
                cookie_btn = self.page.query_selector("button:has-text('Accept') , button:has-text('Agree'), .cookie-accept, [id*='cookie']")
                if cookie_btn:
                    cookie_btn.click(timeout=2000)
                    human_delay(0.5, 1.0)
            except Exception:
                pass
            # Wait for any of the typical result anchors to appear
            try:
                self.page.wait_for_selector(
                    "a[href*='JobDetail'], a:has-text('View more'), a:has-text('View More')",
                    timeout=20000
                )
            except Exception:
                # Try a short scroll to trigger lazy loading
                try:
                    self.page.mouse.wheel(0, 800)
                    human_delay(1, 2)
                    self.page.wait_for_selector(
                        "a[href*='JobDetail'], a:has-text('View more'), a:has-text('View More')",
                        timeout=10000
                    )
                except Exception:
                    pass
            human_delay(3, 5)
            return True
        except Exception as e:
            logger.error(f'Navigation failed: {e}')
            return False

    def collect_job_links(self) -> List[str]:
        # Prefer explicit "View more" links per card
        view_links: List[str] = []
        try:
            for a in self.page.query_selector_all("a:has-text('View more'), a:has-text('View More')"):
                href = a.get_attribute('href') or ''
                if not href:
                        continue
                if not href.startswith('http'):
                    href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                if '/JobDetail/' in href:
                    view_links.append(href)
        except Exception:
            pass
        if view_links:
            logger.info(f'Collected {len(view_links)} View more links')
            return list(dict.fromkeys(view_links))

        # Poll for up to 40s while scrolling to ensure dynamic content is loaded
        end_time = time.time() + 40
        job_links: List[str] = []
        while time.time() < end_time and not job_links:
            try:
                # Evaluate all anchors and collect absolute hrefs containing JobDetail
                anchors = self.page.eval_on_selector_all(
                    'a',
                    "els => els.map(e => e.href).filter(h => h && h.includes('JobDetail'))"
                )
                job_links = anchors or []
            except Exception:
                job_links = []
            if job_links:
                break
            # Try to scroll to trigger lazy loading
            try:
                self.page.mouse.wheel(0, 1000)
            except Exception:
                pass
            human_delay(0.5, 1.0)

            # Also search inside iframes (some Avature instances use embedded frames)
            try:
                for frame in self.page.frames:
                    if frame == self.page.main_frame:
                                    continue
                    try:
                        frame_links = frame.evaluate(
                            "Array.from(document.querySelectorAll('a')).map(a=>a.href).filter(h=>h && h.includes('JobDetail'))"
                        )
                        if frame_links:
                            job_links.extend(frame_links)
                    except Exception:
                            continue
            except Exception:
                pass

        # As a fallback, also inspect any "View more" anchors for hrefs
        if not job_links:
            try:
                view_more_links = self.page.query_selector_all("a:has-text('View more'), a:has-text('View More')")
                for a in view_more_links:
                    href = a.get_attribute('href') or ''
                    if href:
                        if not href.startswith('http'):
                            href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                        if '/JobDetail/' in href:
                            job_links.append(href)
            except Exception:
                pass

        # Final fallback: click on first few job title containers to force anchor rendering
        if not job_links:
            try:
                possible_titles = self.page.query_selector_all("h2 a, h3 a, a:has([href*='JobDetail'])")
                count = 0
                for el in possible_titles:
                    try:
                        el.scroll_into_view_if_needed()
                        human_delay(0.2, 0.5)
                        href = el.get_attribute('href')
                        if href:
                            if not href.startswith('http'):
                                href = f"{self.base_domain}{href if href.startswith('/') else '/' + href}"
                            if '/JobDetail/' in href:
                                job_links.append(href)
                                count += 1
                        if count >= 5:
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        # Uniquify while preserving order
        seen = set()
        unique_links = []
        for u in job_links:
            if u not in seen:
                unique_links.append(u)
                seen.add(u)
        logger.info(f'Collected {len(unique_links)} potential job links')
        return unique_links

    def go_to_next_page(self) -> bool:
        # Try multiple common next-page patterns
        next_selectors = [
            "a[aria-label='Next']",
            "button[aria-label='Next']",
            "a[rel='next']",
            "a:has-text('Next')",
            "a:has-text('>')",
            "li[class*='next'] a",
            ".pager .next a",
            "a[href*='SearchJobs'][href*='page=']",
            "a[href*='SearchJobs'][href*='from=']",
            "a[href*='SearchJobs'][href*='start']"
        ]
        for sel in next_selectors:
            try:
                el = self.page.query_selector(sel)
                if el and not el.get_attribute('disabled'):
                    el.scroll_into_view_if_needed()
                    human_delay(0.3, 0.8)
                    el.click()
                    self.page.wait_for_load_state('networkidle', timeout=60000)
                    human_delay(1.0, 2.0)
                    return True
            except Exception:
                continue
        return False

    def collect_links_across_pages(self, max_pages: int = 50) -> List[str]:
        all_links: List[str] = []
        pages = 0
        while pages < max_pages:
            links = self.collect_job_links()
            if links:
                all_links.extend(links)
            pages += 1
            # Stop if we already have a lot and no next page
            if not self.go_to_next_page():
                break
        # Deduplicate while keeping order
        deduped = []
        seen = set()
        for u in all_links:
            if u not in seen:
                deduped.append(u)
                seen.add(u)
        logger.info(f'Total links collected across pages: {len(deduped)}')
        return deduped

    def extract_job_from_detail(self, url: str) -> Optional[dict]:
        try:
            self.page.goto(url, wait_until='domcontentloaded', timeout=60000)
            # Ensure the job content has rendered
            try:
                self.page.wait_for_selector("h1, .job-title, text=General information", timeout=30000)
            except Exception:
                human_delay(1.5, 2.5)
            human_delay(1, 2)

            # Title
            def normalize_title(raw: str) -> str:
                t = (raw or '').strip()
                # Reject generic or home-page like titles
                blocked = ['australia post home page', 'home page', 'australia post']
                if not t:
                    return ''
                if any(b in t.lower() for b in blocked):
                    return ''
                return t

            title = ''
            # 1) Strongest: H1 text
            try:
                h1 = self.page.locator('h1').first
                if h1 and h1.count() > 0:
                    t = (h1.text_content() or '').strip()
                    title = normalize_title(t)
            except Exception:
                pass
            # 2) Other common title containers
            if not title:
                title_selectors = [
                    '.job-title', '.position-title', '.role-title', "[class*='title']",
                    "xpath=//*[normalize-space(text())='Name']/following::*[1]"
                ]
                for sel in title_selectors:
                    try:
                        el = self.page.query_selector(sel)
                        if el and el.text_content():
                            t = el.text_content().strip()
                            t = normalize_title(t)
                            if t:
                                title = t
                                break
                    except Exception:
                        continue
            # 3) Meta tags
            if not title:
                try:
                    meta = self.page.query_selector("meta[property='og:title'], meta[name='title']")
                    if meta:
                        t = (meta.get_attribute('content') or '').strip()
                        t = normalize_title(t)
                        if t:
                            title = t
                except Exception:
                    pass
            # 4) Derive from URL path
            if not title:
                try:
                    slug_part = url.rstrip('/').split('/')[-2]  # second last is usually the title slug
                    pretty = ' '.join(w.capitalize() for w in slug_part.replace('-', ' ').split())
                    title = normalize_title(pretty)
                except Exception:
                    pass
            # 5) Document title as a very last resort
            if not title:
                try:
                    doc_title = self.page.title() or ''
                    title = normalize_title(doc_title.split('-')[0] if '-' in doc_title else doc_title)
                except Exception:
                    pass
            if not title:
                title = 'Australia Post Position'

            # Company
            company_name = 'Australia Post'

            # Location
            # Location: try reading the General information table next to labels
            location_text = ''
            try:
                # Find label node then read following sibling text
                loc_node = self.page.locator("xpath=//*[normalize-space(text())='Site / Location']/following::*[1]")
                if loc_node and loc_node.count() > 0:
                    txt = loc_node.first.text_content().strip()
                    if txt:
                        location_text = txt
            except Exception:
                pass
            if not location_text:
                location_selectors = [
                    '.location', '.job-location', "[class*='location']", '.address', "[class*='address']"
                ]
                for sel in location_selectors:
                    el = self.page.query_selector(sel)
                    if el and el.text_content():
                        txt = el.text_content().strip()
                        txt = re.sub(r'\s*,?\s*(Australia|NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\s*$', '', txt, flags=re.IGNORECASE)
                        if txt and txt.lower() != 'location':
                            location_text = txt
                            break
        
            # Work Type
            job_type_text = ''
            try:
                wt_node = self.page.locator("xpath=//*[normalize-space(text())='Work Type']/following::*[1]")
                if wt_node and wt_node.count() > 0:
                    job_type_text = (wt_node.first.text_content() or '').strip()
            except Exception:
                pass

            # General information fields to capture into additional_info
            def read_info(label: str) -> str:
                try:
                    node = self.page.locator(f"xpath=//*[normalize-space(text())='{label}']/following::*[1]")
                    if node and node.count() > 0:
                        return (node.first.text_content() or '').strip()
                except Exception:
                    return ''
                return ''

            general_info = {
                'name': read_info('Name'),
                'site_location': read_info('Site / Location') or location_text,
                'ref_number': read_info('Ref #'),
                'entity': read_info('Entity'),
                'opening_date': read_info('Opening Date'),
                'suburb': read_info('Suburb'),
                'state': read_info('State'),
                'work_type_text': job_type_text,
                'length_of_assignment': read_info('Length of Assignment')
            }

            # Description: prioritize the "Description & Requirements" section
            description = ''
            # 1) Try to capture the container that has the heading
            try:
                container = self.page.locator(
                    "xpath=//*[self::h2 or self::h3][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'description')]/following::*[self::div or self::section][1]"
                )
                if container and container.count() > 0:
                    # Use inner_text to preserve visible structure and newlines
                    txt = container.first.inner_text()
                    if txt and len(txt.strip()) > 80:
                        description = txt.strip()
            except Exception:
                pass
            # 2) Try common description classes/selectors
            if not description:
                description_selectors = [
                    '.job-description', '.job-details', '.position-description', '.role-description',
                    "[class*='description']", '.job-content', '.content', "[class*='content']",
                    '.job-detail', "[class*='detail']", '.job-summary', '.summary'
                ]
                for sel in description_selectors:
                    el = self.page.query_selector(sel)
                    if el and el.text_content():
                        txt = (el.inner_text() or '').strip()
                        if len(txt) > 100:
                            description = self.clean_description_text(txt)
                            break
            # 3) Fallback to main content area
            if not description:
                body_text = (self.page.inner_text('main') or self.page.inner_text('article') or self.page.inner_text('body') or '').strip()
                if body_text and len(body_text) > 300:
                    # Keep full description (no truncation)
                    description = self.clean_description_text(body_text)

            # Salary (text extraction)
            full_text = (self.page.text_content('body') or '')
            salary_text = ''
            patterns = [
            r'AU\$[\d,]+\s*-\s*AU\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'\$[\d,]+\s*-\s*\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'AU\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'\$[\d,]+\s*per\s+(?:hour|year|annum)',
            r'[\d,]+k\s*-\s*[\d,]+k\s*per\s+annum',
            r'[\d,]+k\s*per\s+annum',
                r'\$[\d,]+\s*-\s*\$[\d,]+' ,
                r'\$[\d,]+\+'
            ]
            for ptn in patterns:
                m = re.search(ptn, full_text, flags=re.IGNORECASE)
                if m:
                    salary_text = m.group(0)
                    break
            if not salary_text:
                # Heuristic: capture phrasing like "competitive rates/pay"
                if re.search(r'competitive\s+(rates|pay|salary)', full_text, re.IGNORECASE):
                    salary_text = 'Competitive rates'

            # Posted date (relative)
            date_selectors = [
                '.posted-date', "[class*='date']", "[class*='posted']", '.date', "[class*='created']"
            ]
            posted_ago = ''
            for sel in date_selectors:
                el = self.page.query_selector(sel)
                if el and el.text_content():
                    posted_ago = el.text_content().strip()
                    break
        
            job_payload = {
                'title': title,
                'company_name': company_name,
                'location': location_text,
                'external_url': url,
                'description': description or 'No detailed description available',
                'salary_text': salary_text,
                'posted_ago': posted_ago,
                'external_source': 'jobs.auspost.com.au',
                'job_type': self.normalize_job_type(job_type_text or full_text),
                'general_info': general_info
            }
            # Debug log to confirm extraction
            logger.info(f"Extracted: {title} | {location_text} | desc_len={len(job_payload['description'])}")
            return job_payload
        except Exception as e:
            logger.warning(f'Failed to extract from {url}: {e}')
            return None

    def parse_salary(self, salary_text: str) -> Tuple[Optional[int], Optional[int], str, str]:
        if not salary_text:
            return None, None, 'AUD', 'yearly'
        try:
            numbers = re.findall(r'\d+(?:,\d+)?', salary_text)
            values = [int(n.replace(',', '')) for n in numbers]
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
            elif 'month' in low:
                period = 'monthly'
            elif 'week' in low:
                period = 'weekly'
            elif 'day' in low:
                period = 'daily'
            return mn, mx, 'AUD', period
        except Exception:
            return None, None, 'AUD', 'yearly'

    def normalize_job_type(self, text: str) -> str:
        if not text:
            return 'full_time'
        t = text.lower()
        if 'part' in t:
            return 'part_time'
        if 'casual' in t:
            return 'casual'
        if 'contract' in t or 'fixed term' in t or 'fixed-term' in t or 'temporary' in t or 'fixed' in t:
            if 'temporary' in t:
                return 'temporary'
            return 'contract'
        if 'intern' in t or 'trainee' in t:
            return 'internship'
        return 'full_time'

    def clean_description_text(self, text: str) -> str:
        if not text:
            return text
        cleaned = text
        # Remove the promotional line the user asked to exclude
        promo = "See and hear what it's like to be part of our teams in digital tech:"
        lower_cleaned = cleaned.lower()
        promo_l = promo.lower()
        if promo_l in lower_cleaned:
            idx = lower_cleaned.find(promo_l)
            cleaned = cleaned[:idx].rstrip()
        # Remove common accessibility helper line if present
        cleaned = re.sub(r"(?im)^\s*Press\s+space\s+or\s+enter\s+keys\s+to\s+toggle\s+section\s+visibility\s*$", "", cleaned)
        # Collapse excessive blank lines
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned

    def get_or_create_company(self, company_name: str) -> Optional[Company]:
        try:
            company, _ = Company.objects.get_or_create(
                name=company_name,
                defaults={'slug': slugify(company_name), 'company_size': 'large'}
            )
            return company
        except Exception as e:
            logger.error(f'Company create failed: {e}')
            return None

    def get_or_create_location(self, location_name: str) -> Optional[Location]:
        if not location_name:
            return None
        try:
            location, _ = Location.objects.get_or_create(
                name=location_name,
                defaults={'city': location_name, 'country': 'Australia'}
            )
            return location
        except Exception as e:
            logger.error(f'Location create failed: {e}')
        return None

    def _save_job_sync(self, job: dict) -> bool:
        with transaction.atomic():
            # Primary duplicate check by URL only
            if job.get('external_url') and JobPosting.objects.filter(external_url=job['external_url']).exists():
                logger.info(f"Duplicate by URL, skipping: {job['external_url']}")
                return False

            company = self.get_or_create_company(job['company_name'])
            if not company:
                logger.error(f"Failed to get/create company: {job['company_name']}")
                return False
            location = self.get_or_create_location(job.get('location', ''))

            salary_min, salary_max, currency, salary_type = self.parse_salary(job.get('salary_text', ''))
            job_category = JobCategorizationService.categorize_job(job['title'], job.get('description', ''))
            tags = ','.join(JobCategorizationService.get_job_keywords(job['title'], job.get('description', ''))[:10])

            JobPosting.objects.create(
                title=job['title'],
                description=job.get('description', ''),
                company=company,
                location=location,
                posted_by=self.bot_user,
                job_category=job_category,
                job_type=job.get('job_type', 'full_time'),
                salary_min=salary_min,
                salary_max=salary_max,
                salary_currency=currency,
                salary_type=salary_type,
                salary_raw_text=job.get('salary_text', ''),
                external_source=job['external_source'],
                external_url=job.get('external_url', ''),
                status='active',
                posted_ago=job.get('posted_ago', ''),
                tags=tags,
                additional_info={
                    'scraper': 'auspost_playwright',
                    'country': 'Australia',
                    'general_information': job.get('general_info', {})
                }
            )
            return True

    def save_job(self, job: dict) -> bool:
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._save_job_sync, job)
                return future.result(timeout=30)
        except Exception as e:
            logger.error(f'Failed to save job: {e}')
            return False

    def run(self) -> None:
        start = datetime.now()
        try:
            self.setup_browser()
            if not self.navigate_to_search():
                return
            human_delay(2, 4)

            links = self.collect_links_across_pages(max_pages=60)
            if not links:
                logger.warning('No job links found on search page')
                return

            saved = 0
            for idx, url in enumerate(links):
                if self.job_limit and saved >= self.job_limit:
                                    break
                job = self.extract_job_from_detail(url)
                if not job:
                    logger.info(f"Skipped (no data extracted): {url}")
                    continue
                if self.save_job(job):
                    saved += 1
                    logger.info(f"Saved {saved}: {job['title']} at {job['company_name']}")
                human_delay(0.5, 1.2)
        finally:
            self.close_browser()
            duration = datetime.now() - start
            logger.info(f'AusPost scraping complete in {duration}. Jobs saved: {saved if "saved" in locals() else 0}')


def fetch_auspost_all_jobs() -> None:
    scraper = AusPostPlaywrightScraper(job_limit=30)
    scraper.run()


def parse_date(date_str: Optional[str]):
    if not date_str:
        return None
    s = date_str.strip().lower()
    try:
        if 'ago' in s:
            if 'today' in s:
                return datetime.now().date()
            if 'yesterday' in s:
                return (datetime.now() - timedelta(days=1)).date()
            m = re.search(r'(\d+)\s*(day|week|month)', s)
            if m:
                n = int(m.group(1))
                unit = m.group(2)
                if unit == 'day':
                    return (datetime.now() - timedelta(days=n)).date()
                if unit == 'week':
                    return (datetime.now() - timedelta(weeks=n)).date()
                if unit == 'month':
                    return (datetime.now() - timedelta(days=30*n)).date()
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue
    except Exception:
        return None
    return None


if __name__ == '__main__':
    fetch_auspost_all_jobs()

