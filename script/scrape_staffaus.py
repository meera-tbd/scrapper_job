import os
import sys
import re
from datetime import datetime
from decimal import Decimal
import django
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import logging

# ----------------------------------------------------------------------------
# Django setup (run from project root)
# ----------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# ----------------------------------------------------------------------------
# Settings selection and logging
# ----------------------------------------------------------------------------
# Prefer existing env; otherwise try settings_dev, then fallback to settings
os.environ.setdefault("DJANGO_ALLOW_ASYNC_UNSAFE", "true")
if not os.environ.get("DJANGO_SETTINGS_MODULE"):
    dev_settings_path = os.path.join(PROJECT_ROOT, "australia_job_scraper", "settings_dev.py")
    chosen = (
        "australia_job_scraper.settings_dev" if os.path.exists(dev_settings_path) else "australia_job_scraper.settings"
    )
    os.environ["DJANGO_SETTINGS_MODULE"] = chosen

# Configure logging to file
LOG_PATH = os.path.join(PROJECT_ROOT, "scraper_staffaus.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("staffaus")
django.setup()

from django.contrib.auth import get_user_model
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location
from apps.jobs.services import JobCategorizationService


LISTING_URL = "https://staffaus.com.au/job-seekers/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def get_or_create_scraper_user():
    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="scraper_bot",
        defaults={"email": "scraper.bot@example.com"},
    )
    return user


def get_company() -> Company:
    company, _ = Company.objects.get_or_create(
        name="Staff Australia",
        defaults={
            "website": "https://staffaus.com.au",
            "description": "Staff Australia recruitment",
        },
    )
    return company


def parse_salary(salary_text: str):
    if not salary_text:
        return None, None, "AUD", "yearly", salary_text

    text = salary_text.replace(",", "").replace("AU$", "$")
    currency = "AUD" if "$" in text or "AU$" in salary_text else "AUD"
    salary_type = "yearly"

    if re.search(r"per\s*hour", text, re.I):
        salary_type = "hourly"
    elif re.search(r"per\s*(annum|year)", text, re.I):
        salary_type = "yearly"
    elif re.search(r"per\s*month", text, re.I):
        salary_type = "monthly"
    elif re.search(r"per\s*week", text, re.I):
        salary_type = "weekly"
    elif re.search(r"per\s*day", text, re.I):
        salary_type = "daily"

    range_match = re.search(r"\$\s*(\d+(?:\.\d+)?)\s*[-–]\s*\$?\s*(\d+(?:\.\d+)?)", text)
    single_match = re.search(r"\$\s*(\d+(?:\.\d+)?)", text)

    salary_min = salary_max = None
    if range_match:
        salary_min = Decimal(range_match.group(1))
        salary_max = Decimal(range_match.group(2))
    elif single_match:
        salary_min = Decimal(single_match.group(1))
        salary_max = salary_min

    return salary_min, salary_max, currency, salary_type, salary_text.strip()


def parse_location_name(text: str):
    if not text:
        return None, None, None
    states = [
        "New South Wales",
        "Victoria",
        "Queensland",
        "Western Australia",
        "South Australia",
        "Tasmania",
        "Australian Capital Territory",
        "Northern Territory",
        "NSW",
        "VIC",
        "QLD",
        "WA",
        "SA",
        "TAS",
        "ACT",
        "NT",
    ]
    for state in states:
        if state.lower() in text.lower():
            parts = [p.strip() for p in re.split(r",|\u2013|\-|/", text) if p.strip()]
            if len(parts) >= 2:
                city = parts[0].title()
                st = parts[1].title()
                return f"{city}, {st}", city, st
            return text.title(), "", state.title()
    return text.title(), "", ""


def upsert_job(data: dict):
    company = get_company()
    user = get_or_create_scraper_user()

    # Location
    location_name, city, state = parse_location_name(data.get("location") or "")
    location_obj = None
    if location_name:
        location_obj, _ = Location.objects.get_or_create(
            name=location_name,
            defaults={"city": city or "", "state": state or "Australia"},
        )

    # Salary (robust): ensure salary_raw_text always stored even when numeric parse fails
    smin, smax, currency, salary_type, raw_salary = parse_salary(data.get("salary") or "")

    # Categorization and tags
    category = JobCategorizationService.categorize_job(data.get("title", ""), data.get("description", ""))
    keywords = JobCategorizationService.get_job_keywords(data.get("title", ""), data.get("description", ""))

    obj, created = JobPosting.objects.get_or_create(
        external_url=data["external_url"],
        defaults={
            "title": data.get("title", ""),
            "description": data.get("description", ""),
            "company": company,
            "posted_by": user,
            "location": location_obj,
            "job_category": category,
            "job_type": data.get("job_type", "full_time"),
            "salary_min": smin,
            "salary_max": smax,
            "salary_currency": currency,
            "salary_type": salary_type,
            "salary_raw_text": raw_salary or "",
            "external_source": "staffaus.com.au",
            "external_id": data.get("external_id", ""),
            "date_posted": data.get("date_posted"),
            "work_mode": data.get("work_mode", ""),
            "tags": ", ".join(keywords) if keywords else "",
        },
    )

    if not created:
        # Update existing
        obj.title = data.get("title", obj.title)
        obj.description = data.get("description", obj.description)
        obj.location = location_obj or obj.location
        obj.job_category = category
        obj.job_type = data.get("job_type", obj.job_type)
        obj.salary_min = smin
        obj.salary_max = smax
        obj.salary_currency = currency
        obj.salary_type = salary_type
        if data.get("salary"):
            obj.salary_raw_text = raw_salary or data.get("salary")
        obj.external_id = data.get("external_id", obj.external_id)
        obj.date_posted = data.get("date_posted", obj.date_posted)
        obj.work_mode = data.get("work_mode", obj.work_mode)
        obj.tags = ", ".join(keywords) if keywords else obj.tags
        obj.save()

    return obj, created


def text_or_none(value):
    return value.strip() if isinstance(value, str) else None


def extract_text_preserving_structure(container) -> str:
    """
    Traverse a rich-text container and return text with paragraph breaks and bullet points preserved.
    """
    lines = []
    for el in container.descendants:
        if getattr(el, "name", None) in {"ul", "ol"}:
            continue
        if getattr(el, "name", None) == "li":
            text = el.get_text(" ", strip=True)
            if text:
                lines.append(f"- {text}")
        elif getattr(el, "name", None) in {"p", "h2", "h3", "h4"}:
            text = el.get_text(" ", strip=True)
            if text:
                lines.append(text)
    # Coalesce multiple blank lines and trim
    text = "\n\n".join([ln for ln in lines if ln.strip()])
    return text.strip()


def clean_description_text(text: str) -> str:
    """Remove boilerplate/acknowledgement/footer from description."""
    if not text:
        return ""
    # Drop acknowledgement section if present
    ack_patterns = [
        r"Staff Australia Pty Ltd\.[\s\S]*?Traditional owners[\s\S]*?emerging\.",
        r"Acknowledgement[\s\S]*?Traditional owners[\s\S]*?emerging\.",
    ]
    for pat in ack_patterns:
        text = re.sub(pat, "", text, flags=re.IGNORECASE)

    # Trim anything after obvious site/footer headings
    stop_markers = [
        "Similar Jobs",
        "EMPLOYERS",
        "JOB SEEKERS",
        "EMPLOYEES",
        "CALL US ON",
        "JOIN STAFF AUSTRALIA",
        "Privacy Policy",
        "Terms of Use",
        "Diversity and Inclusion",
    ]
    for marker in stop_markers:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]
    # Normalize whitespace
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def extract_description_from_heading(soup: BeautifulSoup) -> str:
    """Target the block under the 'Job Description' heading and capture paragraphs and lists until next section."""
    heading = None
    for tag in soup.find_all(["h1", "h2", "h3", "strong"]):
        if tag.get_text(strip=True).lower().startswith("job description"):
            heading = tag
            break
    if not heading:
        return ""

    # Start after the heading; don't include the heading text itself
    lines = []
    # Optionally include the role tag under the heading (often right below the heading)
    nxt = heading.find_next()
    if nxt and nxt.name in {"p", "div", "strong"} and len(nxt.get_text(strip=True)) < 80:
        lines.append(nxt.get_text(" ", strip=True))

    stop_tags = {"h1", "h2"}
    stop_texts = {"similar jobs", "details"}

    for sib in heading.find_all_next():
        if sib is heading:
            continue
        txt = sib.get_text(" ", strip=True)
        if not txt:
                continue
        # stop at next major section or sidebar label
        if (sib.name in stop_tags and sib is not heading) or any(t in txt.lower() for t in stop_texts):
            break
        if sib.name == "li":
            lines.append(f"- {txt}")
        elif sib.name in {"p"}:
            lines.append(txt)
        # cap runaway capture
        if len("\n\n".join(lines)) > 5000:
            break

    text = "\n\n".join([ln for ln in lines if ln.strip()])
    return text.strip()


def dedupe_description(text: str) -> str:
    """Remove duplicate lines and collapse repeated headings while keeping order."""
    if not text:
        return ""
    out_lines = []
    seen = set()
    last_blank = False
    for raw in text.splitlines():
        line = raw.strip()
        norm = re.sub(r"\s+", " ", line).casefold()
        # Treat bullet and non-bullet equivalently for dedupe
        norm = norm.lstrip("- ")
        if not line:
            if not last_blank and out_lines:
                out_lines.append("")
                last_blank = True
            continue
        if norm in seen:
            continue
        seen.add(norm)
        out_lines.append(line)
        last_blank = False
    # Remove trailing blank
    while out_lines and out_lines[-1] == "":
        out_lines.pop()
    return "\n\n".join(out_lines)


def normalize_spacing(text: str) -> str:
    """Collapse excessive blank lines/spaces and strip trailing spaces per line."""
    if not text:
        return ""
    # Trim spaces per line
    lines = [ln.rstrip() for ln in text.splitlines()]
    # Collapse multiple blank lines to a single blank line
    out = []
    blank = False
    for ln in lines:
        if ln.strip() == "":
            if not blank:
                out.append("")
            blank = True
        else:
            out.append(ln)
            blank = False
    return "\n".join(out).strip()


def extract_job_from_detail(html: str, url: str):
    soup = BeautifulSoup(html, "html.parser")

    # Title
    title = None
    for sel in ["h1", ".elementor-heading-title", ".entry-title"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = el.get_text(strip=True)
            break
        
    # Location (banner line near top)
    location = None
    state_regex = re.compile(
        r"(New South Wales|Victoria|Queensland|Western Australia|South Australia|Tasmania|Australian Capital Territory|Northern Territory|NSW|VIC|QLD|WA|SA|TAS|ACT|NT)",
        re.I,
    )
    for t in soup.find_all(string=state_regex):
        candidate = t.strip()
        if 3 <= len(candidate) <= 60 and "," in candidate:
            location = candidate
            break
        
    # Details box (salary, job type, work mode)
    salary_text = None
    job_type = None
    work_mode = None
    details_anchor = soup.find(string=re.compile(r"^\s*Details\s*$", re.I))
    details_container = details_anchor.find_parent() if details_anchor else None
    if details_container:
        # Walk forward collecting the right-column lines until next major section
        pieces = []
        for sib in details_container.find_all_next():
            if sib is details_container:
                continue
            t = sib.get_text(" ", strip=True)
            if not t:
                continue
            if sib.name in {"h1", "h2"} or t.lower().startswith("similar jobs") or t.lower().startswith("job description"):
                    break
            pieces.append(t)
            if len(" ".join(pieces)) > 600:
                break
        text_blob = " \n".join(pieces)

        # Salary (raw): try line-by-line first for robustness
        salary_text = None
        for line in [ln.strip() for ln in text_blob.splitlines() if ln.strip()]:
            if re.search(r"AU?\$|per\s*hour|per\s*annum|per\s*year|negotiable|allowance", line, re.I):
                salary_text = line
                break
        if not salary_text:
            # Fallback: single regex over the blob
            m = re.search(r"AU?\$\s*[\d,]+(?:\.\d+)?\s*(?:[-–to]{1,3}\s*AU?\$\s*[\d,]+(?:\.\d+)?)?[^\n]*", text_blob, re.I)
            if m:
                salary_text = m.group(0).strip()
        # Job type
        if re.search(r"Full\s*Time", text_blob, re.I):
            job_type = "full_time"
        elif re.search(r"Contract|Temporary", text_blob, re.I):
            job_type = "contract"
        elif re.search(r"Casual|Vacation", text_blob, re.I):
            job_type = "casual"
        if re.search(r"Not a Remote Position", text_blob, re.I):
            work_mode = "On-site"
        elif re.search(r"Remote", text_blob, re.I):
            work_mode = "Remote"

    # Description: prefer the block directly under the "Job Description" heading
    description = extract_description_from_heading(soup)
    
    # If that fails, prefer content inside the main post content widget
    content_selectors = [
        ".elementor-widget-theme-post-content",
        ".entry-content",
        ".elementor-widget-text-editor",
        "article .elementor-widget-text-editor",
    ]
    if not description:
        containers = []
        for sel in content_selectors:
            containers.extend(soup.select(sel))

        prioritized = [c for c in containers if "job description" in c.get_text(" ", strip=True).lower()]
        if prioritized:
            containers = prioritized + [c for c in containers if c not in prioritized]

        for c in containers:
            txt = extract_text_preserving_structure(c)
            if len(txt) > len(description):
                description = txt

    # Fallback: stitch together paragraphs and bullet items
    if not description:
        temp_lines = []
        for li in soup.find_all("li"):
            t = li.get_text(" ", strip=True)
            if t:
                temp_lines.append(f"- {t}")
        for p in soup.find_all("p"):
            t = p.get_text(" ", strip=True)
            if t and len(t) > 20:
                temp_lines.append(t)
        if temp_lines:
            description = "\n\n".join(temp_lines)

    description = clean_description_text(description)
    description = dedupe_description(description)
    description = normalize_spacing(description)

    return {
        "title": text_or_none(title) or "",
        "location": text_or_none(location) or "",
        "salary": text_or_none(salary_text) or "",
        "description": text_or_none(description) or "",
        "job_type": job_type or "full_time",
        "work_mode": work_mode or "",
        "external_url": url,
    }


def collect_listing_links(page):
    stable_rounds = 0
    last_count = 0
    link_selector = "a:has-text('See Full Job Description')"
    while stable_rounds < 3:
        page.mouse.wheel(0, 16000)
        page.wait_for_timeout(1200)
        count = page.locator(link_selector).count()
        if count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = count

    links = []
    for handle in page.locator(link_selector).element_handles():
        href = handle.get_attribute("href")
        if href and href.startswith("http"):
            links.append(href)
    return list(dict.fromkeys(links))


def fetch_staff_australia_jobs(max_jobs: int = 100):
    logger.info("Starting Staff Australia scrape | settings=%s | log=%s", os.environ.get("DJANGO_SETTINGS_MODULE"), LOG_PATH)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1440, "height": 900})
        page = context.new_page()
        page.goto(LISTING_URL, wait_until="networkidle")

        job_links = collect_listing_links(page)
        logger.info("Collected %d job links from listing page", len(job_links))

        processed = 0
        created_count = 0
        updated_count = 0

        for url in job_links[:max_jobs]:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(800)
            html = page.content()
            data = extract_job_from_detail(html, url)
            data["date_posted"] = None

            obj, created = upsert_job(data)
            created_count += int(created)
            updated_count += int(not created)
            processed += 1
            logger.info("Saved: %s (%s)", obj.title, "created" if created else "updated")

        browser.close()
        logger.info("Done. Processed=%s, created=%s, updated=%s", processed, created_count, updated_count)


if __name__ == "__main__":
    max_jobs = 100
    if len(sys.argv) > 1:
        try:
            max_jobs = int(sys.argv[1])
        except Exception:
            pass
    fetch_staff_australia_jobs(max_jobs=max_jobs)


def run(max_jobs=None):
    """Automation entrypoint for Staff Australia scraper."""
    try:
        fetch_staff_australia_jobs(max_jobs=max_jobs)
        return {
            'success': True,
            'message': f'Staff Australia scraping completed (limit {max_jobs})'
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }



