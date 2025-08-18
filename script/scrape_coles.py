#!/usr/bin/env python
"""
Coles Careers Job Scraper (Playwright)

Scrapes the public search results at `https://colescareers.com.au/au/en/search-results`,
collects each job detail URL dynamically (no hardcoded selectors), opens every
job detail page, extracts title, description, location, job type, salary (when
available), and saves records into the Django `JobPosting` model to match the
backend schema.

Usage:
  python script/scrape_coles.py [max_jobs]
"""

import os
import sys
import re
import time
import random
import logging
from urllib.parse import urljoin, urlparse

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'

try:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    PROJECT_ROOT = os.getcwd()
sys.path.append(PROJECT_ROOT)

import django
django.setup()

from django.db import transaction, connections
from django.db.models import Q
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
        logging.FileHandler('scraper_coles.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

User = get_user_model()


class ColesScraper:
    def __init__(self, max_jobs: int | None = None, headless: bool = True):
        self.max_jobs = max_jobs
        self.headless = headless
        self.base_url = 'https://colescareers.com.au'
        self.search_url = 'https://colescareers.com.au/au/en/search-results'
        self.company: Company | None = None
        self.scraper_user: User | None = None
        self.scraped_count = 0

    # ---------- Utilities ----------
    def human_like_delay(self, min_s=0.6, max_s=1.4):
        time.sleep(random.uniform(min_s, max_s))

    def ensure_full_content_loaded(self, page, steps: int = 14):
        """Scrolls the page incrementally to trigger any lazy-loaded sections."""
        last_height = 0
        try:
            for _ in range(max(6, steps)):
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    break
                self.human_like_delay(0.25, 0.55)
                try:
                    new_height = page.evaluate("() => document.body.scrollHeight")
                except Exception:
                    break
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception:
            pass

    def setup_database_objects(self):
        self.company, _ = Company.objects.get_or_create(
            name='Coles',
            defaults={
                'description': 'Coles Careers',
                'website': self.base_url,
                'company_size': 'enterprise',
                'logo': ''
            }
        )
        self.scraper_user, _ = User.objects.get_or_create(
            username='coles_scraper',
            defaults={
                'email': 'scraper@coles.local',
                'first_name': 'Coles',
                'last_name': 'Scraper',
                'is_active': True,
            }
        )

    def get_or_create_location(self, location_text: str | None) -> Location | None:
        if not location_text:
            return None
        raw = (location_text or '').strip()
        if not raw:
            return None

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
        text = raw
        for k, v in abbrev.items():
            text = re.sub(rf'\b{k}\b', v, text, flags=re.IGNORECASE)
        text = re.sub(r'\bCBD\b', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s+', ' ', text).strip(' ,\t\n')

        city = ''
        state = ''
        if ',' in text:
            parts = [p.strip() for p in text.split(',') if p.strip()]
            if len(parts) >= 2:
                city, state = parts[0], ', '.join(parts[1:])
        else:
            state = text

        city = city.title()
        state = state.title()
        name = f"{city}, {state}" if city and state else (state or city)

        existing = (
            Location.objects.filter(name__iexact=name).first()
            or (Location.objects.filter(city__iexact=city, state__iexact=state).first() if city and state else None)
            or (Location.objects.filter(Q(name__istartswith=f"{city}, ") & Q(name__icontains=state)).first() if city and state else None)
        )
        if existing:
            return existing

        return Location.objects.create(
            name=name,
            city=city or '',
            state=state or '',
            country='Australia',
        )

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
        if 'permanent' in t:
            return 'permanent'
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

    def ensure_category_choice(self, display_text: str | None) -> str:
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
            m = re.search(r"(.+?/[0-9]{4,}.+)$", path)
            if m:
                path = m.group(1)
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

    # ---------- Extraction ----------
    def _build_search_url(self, offset: int) -> str:
        """Return search URL with correct 'from' offset param (increments of 10)."""
        try:
            parsed = urlparse(self.search_url)
            query = dict(re.findall(r'([^&=?]+)=([^&]*)', parsed.query))
            query['from'] = str(max(0, int(offset)))
            if 's' not in query:
                query['s'] = '1'
            qs = '&'.join([f"{k}={v}" for k, v in query.items()])
            path = parsed.path or '/au/en/search-results'
            return f"{parsed.scheme}://{parsed.netloc}{path}?{qs}"
        except Exception:
            return f"{self.search_url}?from={offset}&s=1"

    def extract_job_links_from_search(self, page) -> list[str]:
        """Collect job links across paginated search results until max_jobs reached."""
        links: set[str] = set()
        try:
            offset = 0
            pages_scanned = 0
            max_pages = 100
            while pages_scanned < max_pages:
                url = self._build_search_url(offset)
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=45000)
                except Exception:
                    page.goto(url, wait_until='load', timeout=65000)
                self.human_like_delay(0.7, 1.2)

                anchors = page.query_selector_all('a[href]')
                new_links_on_page = 0
                for a in anchors:
                    try:
                        href = a.get_attribute('href') or ''
                    except Exception:
                        continue
                    if not href or href.lower().startswith(('mailto:', 'tel:', 'javascript:')):
                        continue
                    abs_url = href if href.startswith('http') else urljoin(self.base_url, href)
                    if re.search(r'/au/en/job/\d+/', abs_url) or re.search(r'/en/job/\d+/', abs_url):
                        normalized = abs_url.split('?')[0]
                        if normalized not in links:
                            links.add(normalized)
                            new_links_on_page += 1
                pages_scanned += 1

                # Stop if we've collected enough for the current run
                if self.max_jobs and len(links) >= self.max_jobs:
                    break

                # Determine if there is a "Next" page
                has_next = False
                try:
                    # Check for explicit next control or compute from total jobs text
                    next_el = page.query_selector('a:has-text("Next")') or page.query_selector('[aria-label*="Next"]')
                    if next_el:
                        has_next = True
                except Exception:
                    has_next = False

                if not has_next or new_links_on_page == 0:
                    break

                offset += 10
        except Exception as e:
            logger.warning(f"Search extraction warning: {e}")
        return list(sorted(links))

    def _read_block_text(self, page, selector: str) -> str:
        try:
            el = page.query_selector(selector)
            if el:
                return (el.inner_text() or '').strip()
        except Exception:
            return ''
        return ''

    def _extract_header_meta_tokens(self, page) -> list[str]:
        """Return tokens from the hero header meta bar (dot-separated or bullets)."""
        for sel in ['header', '.job-description__header', '.hero', 'main header']:
            text = self._read_block_text(page, sel)
            if text:
                # Split by bullets, middle dot, or pipes
                tokens = re.split(r"\s*[•\u2022\|]\s*", text)
                tokens = [re.sub(r'\s+', ' ', t).strip(" -\n\t") for t in tokens if t and len(t) < 80]
                if tokens and len(tokens) >= 2:
                    return tokens
        return []

    def extract_field_by_label(self, page, label: str) -> str:
        try:
            candidates = page.query_selector_all('aside, .summary, .job-summary, [class*="summary"], [class*="details"], [class*="meta"]')
            for c in candidates:
                text = (c.inner_text() or '').strip()
                if not text:
                    continue
                m = re.search(rf"{re.escape(label)}\s*\n\s*(.+?)\s*(?:\n|$)", text, re.IGNORECASE)
                if m:
                    return re.sub(r'\s+', ' ', m.group(1).strip())
                m2 = re.search(rf"{re.escape(label)}\s*[:\-]?\s*(.+?)\s*(?:\n|$)", text, re.IGNORECASE)
                if m2:
                    return re.sub(r'\s+', ' ', m2.group(1).strip())
        except Exception:
            pass
        return ''

    def extract_location(self, page, job_url: str) -> str:
        # 1) Header tokens
        tokens = self._extract_header_meta_tokens(page)
        au_states = [
            'New South Wales', 'Victoria', 'Queensland', 'South Australia',
            'Western Australia', 'Tasmania', 'Northern Territory', 'Australian Capital Territory'
        ]
        for token in tokens:
            # Look for "City, State" or state alone
            m = re.search(r'([A-Za-z\- ]+,\s*(?:' + '|'.join([re.escape(s) for s in au_states]) + '))', token)
            if m:
                return m.group(1).strip()
            for st in au_states:
                if re.search(rf"\b{re.escape(st)}\b", token, re.IGNORECASE):
                    return st

        # 2) Explicit label
        loc = self.extract_field_by_label(page, 'location')
        if loc:
            return loc

        # 3) Body scan
        try:
            body = page.inner_text('body')
        except Exception:
            body = ''
        if body:
            for st in au_states:
                m = re.search(rf'([A-Za-z\- ]+,\s*{re.escape(st)})', body, re.IGNORECASE)
                if m:
                    return m.group(1).strip()
            for st in au_states:
                if re.search(rf"\b{re.escape(st)}\b", body, re.IGNORECASE):
                    return st

        # 4) Fallback: parse from URL segments if they include a city/state
        try:
            path = urlparse(job_url).path
            parts = [p for p in path.split('/') if p]
            # Often: /au/en/job/<id>/<slug>
            if parts:
                slug = parts[-1].replace('-', ' ').title()
                # Try extract "City, State" from slug words if present
                m = re.search(r'([A-Za-z\- ]+,\s*[A-Za-z\- ]+)$', slug)
                if m:
                    return m.group(1)
        except Exception:
            pass
        return ''

    def clean_description(self, text: str) -> str:
        if not text:
            return ''
        # Hard cutoff: remove everything after testimonial/gallery sections
        lc = text.lower()
        cut_markers = [
            'hear from some of the team',
        ]
        for mark in cut_markers:
            idx = lc.find(mark)
            if idx != -1:
                text = text[:idx]
                lc = text.lower()
                break

        lines = [ln.rstrip() for ln in text.split('\n')]
        cleaned = []
        drop_exact = {'Apply', 'Apply now', 'Save', 'Share', '-', '–', '—'}
        skip_block = False
        skip_remaining_block_lines = 0
        for ln in lines:
            if not ln.strip() or ln.strip() in drop_exact:
                continue
            if skip_block:
                # Keep skipping until a blank separator or a reasonable number of lines
                heading_break = re.search(r'(about the role|about you|what\'s in it|about the recruitment process)', ln.strip(), re.IGNORECASE)
                if not ln.strip() or skip_remaining_block_lines <= 0 or heading_break:
                    skip_block = False
                else:
                    skip_remaining_block_lines -= 1
                    continue
            if ln.strip().lower().startswith(('apply', 'save', 'share')) and len(ln.strip()) <= 40:
                continue
            # Generic removal of video/transcript controls without relying on site-specific wording
            low = ln.strip().lower()
            if ('audio' in low and 'description' in low and len(low) <= 80) or ('transcript' in low and 'video' in low and len(low) <= 120):
                continue
            # Drop global navigation/footer/cta/cookie/chat patterns (domain-agnostic)
            drop_patterns = [
                r'\bcookie(s)?\b',
                r'\bprivacy\b',
                r'\b(back to search results)\b',
                r'\b(get notified|job alerts|sign up|activate|subscribe)\b',
                r'\b(life at|rewards and benefits|diversity and inclusion)\b',
                r'\bfollow us\b',
                r'\bcopyright\b',
                r'\bfaq\b',
                r'\bsearch and apply\b',
                r'\bexplore location\b',
                r'\b(chat|chatbot)\b',
                r'\b(disable|enable) audio description\b',
                r'\b(personal information request)\b',
                r'\bskip to main content\b',
                r'\bwork with us\b',
                r'\bcareer paths\b',
                r'\bmeet the team\b',
                r'\bmy first job\b',
                r'\bcommunity and sustainability\b',
                r'\ba day in the life\b',
                r'\blearn what being in the store leadership team is like\b',
                r'\benter email address\b',
                r'^today\b',
                r'^bot message\b',
                r"let's get started",
                r"hi there, i'm here to help",
                r'\bask a question\b',
                r'\bguided job search\b',
                r'\bupload resume\b',
                r'\bset job alerts\b',
                r'\bexplore jobs\b',
                r'\bhear from some of the team\b',
                r'\btoorak rd\b',
            ]
            if any(re.search(pat, low) for pat in drop_patterns):
                # If we hit the Life at Coles block header, skip a short block after it
                if 'life at coles' in low or 'rewards and benefits' in low or 'diversity and inclusion' in low or 'hear from some of the team' in low:
                    skip_block = True
                    skip_remaining_block_lines = 12
                continue
            # Drop short, menu-like title-cased items (heuristic)
            words = [w for w in re.split(r'\s+', ln.strip()) if w]
            if 1 <= len(words) <= 4 and len(ln.strip()) <= 32:
                # Title-cased or all-caps words without punctuation are likely menu items
                if all((w.isupper() or (w[:1].isupper() and w[1:].islower())) and w.isalpha() for w in words):
                    continue
            # Drop short testimonial quotes like “I really thrive...” that are standalone
            if re.match(r'^["\u201C].{0,300}["\u201D]$\s*', ln.strip()):
                continue
            cleaned.append(ln)
        text = '\n'.join(cleaned)
        # Remove any leading stray dash from the very start
        text = text.lstrip()
        text = re.sub(r'^(?:[-\u2013\u2014])\s*', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def expand_description_if_collapsed(self, page):
        candidates = [
            'button:has-text("show more")',
            'button:has-text("read more")',
            'button:has-text("see more")',
            'a:has-text("show more")',
            'a:has-text("read more")',
            '[aria-expanded="false"][aria-controls]',
        ]
        for sel in candidates:
            try:
                el = page.query_selector(sel)
                if el:
                    el.click(timeout=1000)
                    self.human_like_delay(0.3, 0.8)
            except Exception:
                continue

    def extract_dynamic_description(self, page) -> str:
        """Dynamically extract the largest meaningful text block after the H1,
        excluding headers/nav/footers, video/iframe containers, and highly
        interactive sections. Avoids any site-specific static words.
        """
        try:
            js = """
            () => {
              const root = document.querySelector('main, article, [role="main"]') || document.body;
              const h1 = document.querySelector('h1');
              const startTop = h1 ? (h1.getBoundingClientRect().top + window.scrollY) : 0;
              const isVisible = (el) => {
                const style = window.getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden' || parseFloat(style.opacity || '1') === 0) return false;
                const rect = el.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
              };
              const nodes = Array.from(root.querySelectorAll('section, article, div'));
              const candidates = [];
              for (const el of nodes) {
                if (!isVisible(el)) continue;
                const top = el.getBoundingClientRect().top + window.scrollY;
                if (top < startTop - 10) continue;
                if (el.closest('header, nav, footer, form, aside')) continue;
                if (el.querySelector('video, iframe, figure video')) continue;
                const text = (el.innerText || '').trim();
                const textLen = text.replace(/\s+/g, ' ').length;
                if (textLen < 200) continue;
                const linkCount = el.querySelectorAll('a').length;
                const buttonCount = el.querySelectorAll('button,[role="button"]').length;
                if (linkCount > 120 || buttonCount > 40) continue;
                candidates.push({ el, score: textLen, text });
              }
              candidates.sort((a,b) => b.score - a.score);
              if (candidates.length === 0) return '';
              const best = candidates[0].el;
              let text = (best.innerText || '').trim();
              // Append a few following siblings that look like content blocks
              let next = best.nextElementSibling;
              let added = 0;
              while (next && added < 5) {
                if (!isVisible(next)) break;
                if (next.matches('header, nav, footer, form, aside')) break;
                if (next.querySelector('video, iframe')) break;
                const t = (next.innerText || '').trim();
                const len = t.replace(/\s+/g, ' ').length;
                const linkCount = next.querySelectorAll('a').length;
                const buttonCount = next.querySelectorAll('button,[role="button"]').length;
                if (len < 80) break;
                if (buttonCount > 20 || linkCount > 80) break;
                text += '\n\n' + t;
                next = next.nextElementSibling;
                added++;
              }
              return text.replace(/\n{3,}/g, '\n\n').trim();
            }
            """
            raw = page.evaluate(js)
        except Exception:
            raw = ''
        return self.clean_description(raw)

    def extract_category(self, page, title: str, description: str) -> tuple[str, str]:
        # Try breadcrumbs or visible labels
        category_raw = ''
        try:
            for sel in ['nav.breadcrumb', 'nav[aria-label*="breadcrumb"]', 'ul.breadcrumb', '[class*="breadcrumb"]']:
                for el in page.query_selector_all(f'{sel} a, {sel} li'):
                    txt = (el.inner_text() or '').strip()
                    if txt:
                        t = txt.lower()
                        if t in {'jobs', 'home'}:
                            continue
                        if re.search(r'\b(australia|nsw|vic|qld|wa|sa|tas|nt|act|sydney|melbourne|brisbane|perth)\b', t):
                            continue
                        category_raw = txt
                        break
                if category_raw:
                    break
        except Exception:
            pass

        if category_raw:
            key = slugify(category_raw).replace('-', '_')
            if any(c[0] == key for c in JobPosting.JOB_CATEGORY_CHOICES):
                return key, category_raw
            return self.ensure_category_choice(category_raw), category_raw

        # Fallback to categorization service
        return JobCategorizationService.categorize_job(title, description), ''

    def extract_description_after_video(self, page) -> str:
        """Extract description blocks that appear after a visible video container.
        Does not depend on any fixed words; uses DOM structure and tag heuristics.
        """
        try:
            js = """
            () => {
              const root = document.querySelector('main, article, [role="main"]') || document.body;
              let video = root.querySelector('video, [class*="video"] video, iframe[src*="youtube" i], [class*="video"] iframe');
              if (!video) return '';
              // Build list of ancestors up to 10 levels to test for rich-text siblings
              const ancestors = [];
              let cur = video;
              for (let i = 0; i < 10 && cur && cur.parentElement; i++) {
                cur = cur.parentElement;
                if (!cur) break;
                if (cur.matches('section, article, div')) ancestors.push(cur);
              }
              const isVisible = (el) => {
                const st = window.getComputedStyle(el);
                if (st.display === 'none' || st.visibility === 'hidden' || parseFloat(st.opacity || '1') === 0) return false;
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
              };
              const collectFromSibling = (node) => {
                const result = [];
                let sib = node.nextElementSibling;
                let steps = 0;
                while (sib && steps < 14) {
                  if (!isVisible(sib)) { sib = sib.nextElementSibling; steps++; continue; }
                  if (sib.matches('header, nav, footer, form, aside')) { sib = sib.nextElementSibling; steps++; continue; }
                  if (sib.querySelector('video, iframe')) { sib = sib.nextElementSibling; steps++; continue; }
                  const txt = (sib.innerText || '').trim();
                  const len = txt.replace(/\s+/g, ' ').length;
                  const btns = sib.querySelectorAll('button,[role="button"]').length;
                  if (len < 100 || btns > 20) { sib = sib.nextElementSibling; steps++; continue; }
                  result.push(sib);
                  if (result.length >= 6) break;
                  sib = sib.nextElementSibling;
                  steps++;
                }
                return result;
              };
              for (const anc of ancestors) {
                const sibs = collectFromSibling(anc);
                if (sibs.length) {
                  const parts = [];
                  for (const s of sibs) {
                    const nodes = s.querySelectorAll('p,li,h2,h3');
                    if (nodes.length === 0) {
                      const t = (s.innerText || '').trim();
                      if (t.length > 120) parts.push(t);
                      continue;
                    }
                    for (const n of nodes) {
                      const txt = (n.innerText || '').trim();
                      if (txt && txt.length >= 2) parts.push(txt);
                    }
                  }
                  const text = parts.join('\n').replace(/\n{3,}/g, '\n\n').trim();
                  if (text.length > 200) return text;
                }
              }
              return '';
            }
            """
            raw = page.evaluate(js)
        except Exception:
            raw = ''
        return self.clean_description(raw)

    def extract_job_from_detail(self, page, job_url: str) -> dict | None:
        try:
            try:
                page.goto(job_url, wait_until='networkidle', timeout=60000)
            except Exception:
                page.goto(job_url, wait_until='load', timeout=65000)
            self.human_like_delay(0.9, 1.6)
            # Ensure all lazy content is rendered
            self.ensure_full_content_loaded(page)

            try:
                page.wait_for_selector('h1, h2', timeout=20000)
            except Exception:
                pass

            title = ''
            try:
                h1 = page.query_selector('h1')
                if h1:
                    title = (h1.inner_text() or '').strip()
                if not title:
                    h2 = page.query_selector('h2')
                    if h2:
                        title = (h2.inner_text() or '').strip()
                if not title:
                    # Fallback to document title
                    try:
                        title = (page.title() or '').strip()
                    except Exception:
                        title = ''
            except Exception:
                pass

            # Description: prioritize content after video, fallback to dynamic block
            description = ''
            self.expand_description_if_collapsed(page)
            description = self.extract_description_after_video(page)
            if not description or len(description) < 120:
                description = self.extract_dynamic_description(page)
            if not description or len(description) < 120:
                # Generic fallback: paragraphs from main/article
                try:
                    txt = ''
                    for sel in ['main', 'article']:
                        el = page.query_selector(sel)
                        if el:
                            txt = (el.inner_text() or '').strip()
                            if txt and len(txt) > 120:
                                break
                    if not txt:
                        txt = (page.inner_text('body') or '').strip()
                except Exception:
                    txt = ''
                if txt:
                    description = self.clean_description(txt)
            # Append compliance footer (accessibility/disability support), job id, employment type if present anywhere on page
            try:
                body_text = page.inner_text('body') or ''
            except Exception:
                body_text = ''
            if body_text:
                extras = []
                # Accessibility/disability support sentence
                m = re.search(r"We\s*’?'?re\s+happy\s+to\s+adjust[\s\S]{0,200}?careers\s+site\s+or\s+email\s+[^\s]+@coles\.com\.au", body_text, re.IGNORECASE)
                if m:
                    extras.append(m.group(0).strip())
                # Job ID
                m = re.search(r"\bJob\s*ID\s*:\s*([A-Za-z0-9\-]+)", body_text, re.IGNORECASE)
                if m:
                    extras.append(f"Job ID: {m.group(1)}")
                # Employment Type
                m = re.search(r"\bEmployment\s+Type\s*:\s*([A-Za-z ]+)", body_text, re.IGNORECASE)
                if m:
                    extras.append(f"Employment Type: {m.group(1).strip()}")
                if extras:
                    description = (description + "\n\n" + "\n".join(extras)).strip()

            # Key meta fields
            location_text = self.extract_location(page, job_url)

            # Job type commonly shown in header tokens or summary
            job_type_text = ''
            tokens = self._extract_header_meta_tokens(page)
            for tok in tokens:
                if re.search(r'full\s*time|part\s*time|casual|contract|temporary|permanent', tok, re.IGNORECASE):
                    job_type_text = tok
                    break
            if not job_type_text:
                job_type_text = self.extract_field_by_label(page, 'employment type') or self.extract_field_by_label(page, 'job type')

            salary_text = self.extract_field_by_label(page, 'salary')
            if not salary_text:
                try:
                    body = page.inner_text('body')
                except Exception:
                    body = ''
                if body:
                    m = re.search(r'(AU\$\s?[0-9,]+(?:\s?-\s?AU\$\s?[0-9,]+)?[^\n]{0,40}(?:hour|annum|year|month|week))', body, re.IGNORECASE)
                    if m:
                        salary_text = m.group(1)
            # Validate salary text: discard generic phrases like 'salary sacrifice' or lines without numbers/currency
            if salary_text:
                if re.search(r'salary\s*sacrifice', salary_text, re.IGNORECASE):
                    salary_text = ''
                elif not re.search(r'(\$|\bAUD\b|\d)', salary_text, re.IGNORECASE):
                    salary_text = ''

            salary_parsed = self.parse_salary(salary_text)
            job_type = self.normalize_job_type(job_type_text or description)
            location_obj = self.get_or_create_location(location_text)

            job_category, category_raw = self.extract_category(page, title, description)

            # External ID: often numeric in the URL path
            external_id = ''
            m = re.search(r'/job/(\d+)', urlparse(job_url).path)
            if m:
                external_id = m.group(1)

            if not title or not description:
                # Debug dump to help tune selectors if something goes wrong again
                try:
                    body_len = len(page.inner_text('body') or '')
                except Exception:
                    body_len = -1
                logger.info(f"Skipping (insufficient content): {job_url} | title_ok={bool(title)} desc_len={len(description or '')} body_len={body_len}")
                return None

            return {
                'title': title[:200],
                'description': description.strip()[:8000],
                'location': location_obj,
                'job_type': job_type,
                'job_category': job_category,
                'date_posted': timezone.now(),
                'external_url': job_url,
                'external_id': f"coles_{external_id}" if external_id else f"coles_{hash(job_url)}",
                'salary_min': salary_parsed['salary_min'],
                'salary_max': salary_parsed['salary_max'],
                'salary_currency': salary_parsed['salary_currency'],
                'salary_type': salary_parsed['salary_type'],
                'salary_raw_text': salary_parsed['salary_raw_text'],
                'work_mode': 'On-site',
                'posted_ago': '',
                'category_raw': category_raw,
            }
        except Exception as e:
            logger.error(f"Error extracting detail from {job_url}: {e}")
            return None

    # ---------- Persistence ----------
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
                    external_source='colescareers.com.au',
                    external_url=safe['external_url'],
                    external_id=safe['external_id'],
                    status='active',
                    posted_ago=safe['posted_ago'],
                    date_posted=safe['date_posted'],
                    tags='',
                    additional_info={'scraped_from': 'coles', 'scraper_version': '1.0'}
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

    # ---------- Orchestration ----------
    def scrape(self) -> int:
        logger.info('Starting Coles scraping...')
        self.setup_database_objects()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            try:
                links = self.extract_job_links_from_search(page)
                logger.info(f"Found {len(links)} job detail links")
                if not links:
                    logger.warning('No job links found on Coles search page.')
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
                    self.human_like_delay(0.5, 1.0)
            finally:
                browser.close()

        connections.close_all()
        logger.info(f'Completed. Jobs processed: {self.scraped_count}')
        return self.scraped_count


def main():
    max_jobs = None
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except ValueError:
            logger.error('Invalid max_jobs argument. Provide an integer.')
            sys.exit(1)

    scraper = ColesScraper(max_jobs=max_jobs, headless=True)
    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f'Fatal error: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()


