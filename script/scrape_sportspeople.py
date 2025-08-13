import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import django
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Django setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "australia_job_scraper.settings_dev")
django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from apps.jobs.models import JobPosting  # noqa: E402
from apps.companies.models import Company  # noqa: E402
from apps.core.models import Location  # noqa: E402
from apps.jobs.services import JobCategorizationService  # noqa: E402

LIST_CARD_SELECTOR = ".job-teaser"
TITLE_LINK_SELECTOR = ".job-teaser__title a, a.job-teaser__title, .job-teaser a[href*='/jobs/']"
EMPLOYER_SELECTOR = ".job-teaser__employer-name, .job-teaser__employer a, .job-teaser__employer"
LOCATION_FEATURE_SELECTOR = ".features li.loc, [class*='location']"
SALARY_FEATURE_SELECTOR = ".features li.sal, [class*='salary']"
CLOSING_DATE_SELECTOR = ".job-teaser__closing__date, .job-teaser__closing, .closing-date"


@dataclass
class ScrapedJob:
    title: str
    company_name: str
    location_text: str
    job_url: str
    description: str
    job_type: str
    salary_text: str
    work_mode: str
    external_id: str


def human_sleep(seconds: float = 0.8) -> None:
    time.sleep(seconds)


def parse_salary_text(salary_text: str) -> Tuple[Optional[float], Optional[float], str, str]:
    if not salary_text:
        return None, None, "AUD", "yearly"
    text = salary_text.replace(",", "").replace("$", "").strip()
    numbers = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", text)]
    period = "yearly"
    if re.search(r"per\s*hour|\bph\b|hourly", text, re.I):
        period = "hourly"
    elif re.search(r"per\s*day|daily", text, re.I):
        period = "daily"
    elif re.search(r"per\s*week|weekly", text, re.I):
        period = "weekly"
    elif re.search(r"per\s*month|monthly", text, re.I):
        period = "monthly"
    if not numbers:
        return None, None, "AUD", period
    if len(numbers) == 1:
        return numbers[0], numbers[0], "AUD", period
    return min(numbers), max(numbers), "AUD", period


def map_job_type(text: str) -> str:
    if not text:
        return "full_time"
    t = text.lower()
    if "part" in t:
        return "part_time"
    if "casual" in t:
        return "casual"
    if "temp" in t or "temporary" in t:
        return "temporary"
    if "contract" in t:
        return "contract"
    if "intern" in t:
        return "internship"
    if "freelance" in t:
        return "freelance"
    return "full_time"


def detect_work_mode(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    if "remote" in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    return "On-site"


def ensure_user():
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if user:
        return user
    return User.objects.create(username="scraper")


def get_or_create_company(name: str) -> Company:
    if not name:
        name = "Unknown Employer"
    safe_name = name.strip()[:200]
    company, _ = Company.objects.get_or_create(name=safe_name)
    return company


def get_or_create_location(text: str) -> Optional[Location]:
    if not text:
        return None
    normalized = re.sub(r"\s+", " ", text).strip()
    city = state = ""
    parts = [p.strip() for p in re.split(r"[,\u25B8\u203A\u2023\u25B6\u2794\u00BB]", normalized) if p.strip()]
    if parts:
        city = parts[0]
    if len(parts) > 1:
        state = parts[1]
    # Obey model max lengths
    safe_name = normalized[:100]
    location, _ = Location.objects.get_or_create(name=safe_name, defaults={"city": city[:100], "state": state[:100]})
    return location


def extract_detail_fields(html: str):
    soup = BeautifulSoup(html, "html.parser")

    def extract_clean_description() -> str:
        # First remove unwanted sections entirely
        for unwanted in soup.select('form, .application-form, nav, header, footer, .sidebar, .related-searches, .learning-recommendations'):
            unwanted.decompose()
        
        # Find description heading and extract content after it
        desc_heading = None
        for h in soup.select("h1,h2,h3,h4,h5,h6"):
            if re.search(r"^\s*description\s*$", h.get_text(strip=True), re.I):
                desc_heading = h
                break
        
        description_parts = []
        
        if desc_heading:
            # Extract everything between Description heading and next major section
            current = desc_heading.next_sibling
            while current:
                if hasattr(current, 'name'):
                    # Stop at next major heading or application sections
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        text = current.get_text(strip=True).lower()
                        if any(stop_word in text for stop_word in [
                            'application', 'apply', 'contact', 'further information',
                            'view related', 'improve your chances', 'recaptcha'
                        ]):
                            break
                    
                    # Extract meaningful content
                    if current.name in ['p', 'div', 'ul', 'ol', 'li']:
                        text = current.get_text('\n', strip=True)
                        if text and len(text) > 10:
                            description_parts.append(text)
                elif isinstance(current, str):
                    text = current.strip()
                    if text and len(text) > 10:
                        description_parts.append(text)
                
                current = current.next_sibling
        
        # If no description heading found, try to extract from main content area
        if not description_parts:
            for container in soup.select('article, main, .content, .job-content, .page-content'):
                text = container.get_text('\n', strip=True)
                if len(text) > 200:
                    description_parts.append(text)
                    break
        
        # Join and clean the description
        if description_parts:
            full_desc = '\n\n'.join(description_parts)
            return clean_job_description(full_desc)
        return ""

    def clean_job_description(text: str) -> str:
        if not text:
            return ""
        
        # Comprehensive list of UI/navigation text to remove
        unwanted_patterns = [
            # Navigation and UI
            r'^(home|jobs|back to job list|edit job \d+|save|share|featured|hot)$',
            # Application sections
            r'apply for this role|application form|first name\*|last name\*|email\*|phone\*|message\*',
            r'attach resum[eé]\*|upload other file|click to select|maximum of \d+ files',
            # Legal and privacy
            r'recaptcha|privacy policy|terms of service|google|protected by',
            # Promotional sections  
            r'view related searches|improve your chances|view all learning|more jobs by this employer',
            r'global fitness institute|seda group|hoops capital sport',
            # Job metadata that's not description
            r'applications\s*\d+|closing\s*\d+|edit job|save$|share$',
            # Location breadcrumbs when standalone
            r'^[a-z\s]+▸[a-z\s]+▸[a-z\s]+$',
            # Contact info patterns (keep when part of larger text)
            r'^for further information please contact:?$',
            r'^\(\d{2}\)\s*\d{4}\s*\d{4}$',  # Standalone phone numbers
        ]
        
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if not line or len(line) < 3:
                continue
                
            line_lower = line.lower()
            
            # Skip lines matching unwanted patterns
            skip_line = False
            for pattern in unwanted_patterns:
                if re.search(pattern, line_lower, re.I):
                    skip_line = True
                    break
            
            if skip_line:
                continue
                
            # Skip lines that are just metadata
            if re.match(r'^(full time|part time|casual|contract|up to \$[\d,]+ pa|\d+ \w+ \d{4})$', line_lower):
                continue
                
            # Keep substantial content lines
            cleaned_lines.append(line)
        
        # Join lines and clean up spacing
        result = '\n'.join(cleaned_lines)
        
        # Remove excessive whitespace and empty lines
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        result = re.sub(r'^\s+|\s+$', '', result)
        
        return result

    # Extract the clean description
    desc = extract_clean_description()

    # Job type
    jt_text = ""
    jt_node = soup.find(string=re.compile(r"Full Time|Part Time|Casual|Contract|Temp|Temporary|Intern", re.I))
    if jt_node:
        jt_text = str(jt_node)
    
    # Salary
    sal_text = ""
    sal_node = soup.find(string=re.compile(r"\$\s*\d", re.I))
    if sal_node:
        sal_text = str(sal_node)
    
    return desc, jt_text, sal_text


def scrape_sportspeople(max_jobs: int = 60) -> None:
    external_source = "sportspeople.com.au"
    base_url = "https://www.sportspeople.com.au/jobs"
    user = ensure_user()

    scraped_jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(20000)
        page.goto(base_url, timeout=120_000, wait_until="domcontentloaded")
        page.wait_for_selector(LIST_CARD_SELECTOR, timeout=20000)

        processed = 0
        seen_hrefs = set()
        page_index = 1

        while processed < max_jobs:
            cards = page.locator(LIST_CARD_SELECTOR)
            count = cards.count()
            if page_index == 1:
                print(f"Found {count} job cards on first page")

            to_take = min(count, max_jobs - processed)
            for i in range(to_take):
                card = cards.nth(i)
                title_link = card.locator(TITLE_LINK_SELECTOR).first
                try:
                    title = (title_link.text_content(timeout=5000) or "").strip()
                except Exception:
                    continue

                href = title_link.get_attribute("href") or ""
                if href.startswith("/"):
                    href = "https://www.sportspeople.com.au" + href
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)

                employer = card.locator(EMPLOYER_SELECTOR).first.text_content(timeout=2000) if card.locator(
                    EMPLOYER_SELECTOR).count() else ""
                employer = (employer or "").strip()

                loc_text = card.locator(LOCATION_FEATURE_SELECTOR).first.text_content(timeout=2000) if card.locator(
                    LOCATION_FEATURE_SELECTOR).count() else ""
                loc_text = (loc_text or "").strip()

                salary_text = card.locator(SALARY_FEATURE_SELECTOR).first.text_content(timeout=2000) if card.locator(
                    SALARY_FEATURE_SELECTOR).count() else ""
                salary_text = (salary_text or "").strip()

                # Fetch job detail page via HTTP request (faster than UI navigation)
                description = ""
                jt_text = ""
                salary_detail_text = ""
                try:
                    resp = context.request.get(href, timeout=45000)
                    if resp.ok:
                        html = resp.text()
                        description, jt_text, salary_detail_text = extract_detail_fields(html)
                except Exception:
                    pass
                job_type = map_job_type(jt_text)
                salary_combined = salary_detail_text or salary_text
                work_mode = detect_work_mode(loc_text)

                # External id from job URL like /jobs/85552-learning-designer-...
                m = re.search(r"/jobs/(\d+)-", href)
                external_id = m.group(1) if m else ""

                scraped_jobs.append({
                    "title": title[:200],
                    "employer": employer,
                    "loc_text": loc_text[:100],
                    "href": href,
                    "description": description,
                    "job_type": job_type,
                    "salary_text": salary_combined,
                    "work_mode": work_mode,
                    "external_id": external_id,
                })

                processed += 1
                if processed >= max_jobs:
                    break

            if processed >= max_jobs:
                break

            # Pagination: try explicit Next link first
            next_href = None
            try:
                next_link = page.locator("a:has-text('Next')").first
                if next_link.count():
                    cand = (next_link.get_attribute("href") or "").strip()
                    # Ignore anchors or javascript links
                    if cand and cand not in ("#", "javascript:void(0)") and not cand.lower().startswith("javascript"):
                        next_href = cand
            except Exception:
                next_href = None

            if next_href:
                if next_href.startswith("/"):
                    next_href = "https://www.sportspeople.com.au" + next_href
                page.goto(next_href, wait_until="domcontentloaded")
                page.wait_for_selector(LIST_CARD_SELECTOR, timeout=20000)
                page_index += 1
                continue

            # Fallback: increment offset parameter (case-insensitive)
            current_url = page.url
            # Sportspeople uses '?offset=N' where N is page index (1-based) or starting index.
            # We'll treat it as a page index and increment by 1.
            m = re.search(r"(?i)([?&])offset=([0-9]+)", current_url)
            if m:
                current_offset = int(m.group(2))
                next_offset = current_offset + 1
                next_url = re.sub(r"(?i)offset=([0-9]+)", f"offset={next_offset}", current_url)
            else:
                sep = '&' if '?' in current_url else '?'
                next_url = f"{current_url}{sep}offset=1"

            page.goto(next_url, wait_until="domcontentloaded")
            try:
                page.wait_for_selector(LIST_CARD_SELECTOR, timeout=8000)
                page_index += 1
            except Exception:
                break

        browser.close()
        print(f"Scraped {processed} jobs from SportsPeople. Saving…")

    # Now safely interact with Django ORM outside Playwright's event loop
    saved = 0
    skipped = 0
    for item in scraped_jobs:
        href = item["href"]
        if JobPosting.objects.filter(external_url=href).exists():
            skipped += 1
            continue

        company = get_or_create_company(item["employer"])
        location = get_or_create_location(item["loc_text"])
        sal_min, sal_max, currency, salary_type = parse_salary_text(item["salary_text"])
        category = JobCategorizationService.categorize_job(item["title"], item["description"])
        tags = ", ".join(JobCategorizationService.get_job_keywords(item["title"], item["description"]))

        JobPosting.objects.create(
            title=(item["title"] or "")[:200],
            description=item["description"] or "",
            company=company,
            posted_by=user,
            location=location,
            job_category=category,
            job_type=item["job_type"],
            work_mode=item["work_mode"],
            salary_min=sal_min,
            salary_max=sal_max,
            salary_currency=currency,
            salary_type=salary_type,
            salary_raw_text=(item["salary_text"] or "")[:200],
            external_source=external_source,
            external_url=href,
            external_id=(item["external_id"] or "")[:100],
            posted_ago="",
            date_posted=None,
            tags=tags,
            additional_info={
                "source_page": "jobs list",
            },
        )
        saved += 1

    print(f"Saved {saved} new jobs (skipped existing: {skipped}) out of {processed} scraped")


if __name__ == "__main__":
    # Optional CLI arg to limit number of jobs
    max_jobs = 60
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except Exception:
            pass
    scrape_sportspeople(max_jobs=max_jobs)
