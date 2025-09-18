import os
import re
import sys
import random
import logging
from typing import Tuple, Optional, List, Dict

import django
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Django setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "australia_job_scraper.settings_dev")
django.setup()

from django.contrib.auth import get_user_model
from apps.jobs.models import JobPosting
from apps.jobs.services import JobCategorizationService
from apps.companies.models import Company
from apps.core.models import Location

BASE_URL = "https://www.teachingjobs.com.au/school-jobs"
SOURCE = "teachingjobs.com.au"

# Logging setup
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scraper_teachingjobs.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def get_or_create_scraper_user():
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first()
    if user:
        return user
    user = User.objects.filter(username="scraper").first()
    if user:
        return user
    user = User(username="scraper", email="scraper@example.com", is_active=True)
    user.set_unusable_password()
    user.save()
    return user


def get_or_create_company(name: str) -> Company:
    company_name = name.strip() if name else "Educational Institution"
    company, _ = Company.objects.get_or_create(name=company_name)
    return company


def parse_location_text(text: str) -> Tuple[str, str, str]:
    if not text:
        return ("", "", "Australia")
    cleaned = re.sub(r"\s+", " ", text).replace("Australia", "").strip().strip(",")
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    city = parts[-2] if len(parts) >= 2 else (parts[0] if parts else "")
    state = parts[-1] if parts else ""
    name = ", ".join(parts) if parts else ""
    return (name, city, state)


def get_or_create_location(text: str) -> Optional[Location]:
    name, city, state = parse_location_text(text)
    if not name:
        return None
    location, _ = Location.objects.get_or_create(
        name=name,
        defaults={"city": city, "state": state, "country": "Australia"},
    )
    return location


def map_job_type(raw: str) -> str:
    if not raw:
        return "full_time"
    raw_l = raw.lower()
    if "part" in raw_l:
        return "part_time"
    if any(k in raw_l for k in ["casual", "relief"]):
        return "casual"
    if any(k in raw_l for k in ["contract", "fixed term", "temp"]):
        return "contract"
    if "intern" in raw_l:
        return "internship"
    return "full_time"


def parse_salary(raw_text: str):
    if not raw_text:
        return (None, None, "AUD", "yearly", "")
    text = raw_text.replace("AU$", "$")
    m = re.search(r"\$([\d,]+)\s*[-–]\s*\$([\d,]+)", text)
    salary_min = salary_max = None
    if m:
        salary_min = float(m.group(1).replace(",", ""))
        salary_max = float(m.group(2).replace(",", ""))
    else:
        m2 = re.search(r"\$([\d,]+)", text)
        if m2:
            salary_min = float(m2.group(1).replace(",", ""))
    period = "hourly" if re.search(r"per\s*hour|/\s*hour", text, re.I) else "yearly"
    return (salary_min, salary_max, "AUD", period, raw_text.strip())


def _clean_description(text: str) -> str:
    """Remove CTA/advert boilerplate from description."""
    skip_patterns = [
        r"\bapply now\b",
        r"\bto apply\b",
        r"\bapply here\b",
        r"\bapply online\b",
        r"\bapply today\b",
        r"\bclick apply\b",
        r"\bfind your best opportunity\b",
        r"teachingjobs\.com\.au",
        r"visit our website",
        r"contact us",
        r"tell them",
        r"how to apply",
        r"\bapply via\b",
    ]
    # Drop lines containing URLs or any CTA phrase
    cleaned_lines: List[str] = []
    for line in (text or "").split("\n"):
        l = line.strip()
        if not l:
            continue
        lower = l.lower()
        if re.search(r"https?://\S+", lower):
            continue
        if any(re.search(p, lower) for p in skip_patterns):
            continue
        cleaned_lines.append(l)
    return "\n".join(cleaned_lines)


def _find_salary_raw(text: str) -> str:
    patterns = [
        r"\$[\d,]+\s*[–-]\s*\$[\d,]+",              # range
        r"\$[\d,]+\s*per\s*annum",
        r"\$[\d,]+\s*per\s*year",
        r"\$[\d,]+\s*(?:p\.?a\.?)",
        r"\$[\d,]+\s*(?:per\s*hour|/\s*hour|hourly)", # hourly
        r"AU\$[\d,]+\s*(?:per\s*hour|/\s*hour|hourly)",
    ]
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            return m.group(0)
    return ""


def _extract_html_description(soup: BeautifulSoup) -> str:
    """Extract and clean job description as HTML format."""
    # Try different selectors for job description
    description_selectors = [
        ".job-description",
        ".description", 
        ".job-content",
        ".job-details",
        "main .content",
        "article",
        ".prose",
        "[role='main']"
    ]
    
    description_element = None
    for selector in description_selectors:
        element = soup.select_one(selector)
        if element and element.get_text(strip=True):
            description_element = element
            break
    
    if not description_element:
        # Fallback to body content
        description_element = soup.body or soup
    
    # Clean the HTML
    html_content = str(description_element)
    
    # Remove unwanted elements
    temp_soup = BeautifulSoup(html_content, "html.parser")
    
    # Remove script, style, nav, footer, header elements
    for unwanted in temp_soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
        unwanted.decompose()
    
    # Remove elements with unwanted classes/ids
    unwanted_patterns = ['nav', 'footer', 'header', 'sidebar', 'advertisement', 'ad-', 'cookie']
    for pattern in unwanted_patterns:
        for element in temp_soup.find_all(attrs={'class': re.compile(pattern, re.I)}):
            element.decompose()
        for element in temp_soup.find_all(attrs={'id': re.compile(pattern, re.I)}):
            element.decompose()
    
    # Get inner HTML content
    if temp_soup.body:
        html_content = ''.join(str(child) for child in temp_soup.body.children)
    else:
        html_content = str(temp_soup)
    
    # Clean up HTML
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)
    html_content = re.sub(r'<!--.*?-->', '', html_content, flags=re.DOTALL)
    html_content = re.sub(r'\s+', ' ', html_content)
    html_content = html_content.strip()
    
    # If no proper HTML structure, wrap text in paragraphs
    if not re.search(r'<(p|div|h[1-6]|ul|ol|li)', html_content):
        text_content = BeautifulSoup(html_content, "html.parser").get_text()
        if text_content.strip():
            paragraphs = [p.strip() for p in text_content.split('\n') if p.strip()]
            html_content = ''.join(f'<p>{p}</p>' for p in paragraphs)
    
    return html_content or "<p>Job description not available</p>"


def _extract_skills_from_description(description_text: str, title: str = "") -> tuple[str, str]:
    """Extract skills and preferred skills from job description text."""
    if not description_text:
        return "", ""
    
    # Convert HTML to text if needed
    if '<' in description_text and '>' in description_text:
        soup = BeautifulSoup(description_text, 'html.parser')
        text_content = soup.get_text()
    else:
        text_content = description_text
    
    # Combine title and description for analysis
    combined_text = f"{title} {text_content}".lower()
    
    # Education and teaching specific skills
    education_skills = [
        # Teaching specializations
        'primary teaching', 'secondary teaching', 'early childhood', 'special education', 'esol', 'esl',
        'literacy', 'numeracy', 'mathematics', 'science', 'english', 'history', 'geography', 'art',
        'music', 'physical education', 'pe', 'drama', 'languages', 'stem', 'steam',
        
        # Teaching qualifications and certifications
        'bachelor of education', 'bed', 'teaching degree', 'dit', 'graduate diploma teaching',
        'teaching registration', 'vit registration', 'nesa accreditation', 'working with children',
        'wwcc', 'blue card', 'yellow card', 'first aid', 'cpr', 'anaphylaxis', 'asthma',
        
        # Teaching methods and approaches
        'differentiated instruction', 'inclusive education', 'play based learning', 'inquiry learning',
        'project based learning', 'collaborative learning', 'assessment', 'rubrics', 'scaffolding',
        'classroom management', 'behaviour management', 'positive behaviour support', 'restorative practices',
        
        # Technology and digital skills
        'google classroom', 'microsoft teams', 'seesaw', 'schoology', 'canvas', 'moodle', 'blackboard',
        'interactive whiteboard', 'smartboard', 'ipad', 'chromebook', 'educational technology',
        'digital literacy', 'coding', 'robotics', 'scratch programming',
        
        # Curriculum frameworks
        'australian curriculum', 'victorian curriculum', 'nsw syllabus', 'queensland curriculum',
        'early years learning framework', 'eylf', 'national quality standard', 'nqs',
        'ib curriculum', 'cambridge curriculum', 'montessori', 'waldorf', 'steiner',
        
        # Professional skills
        'lesson planning', 'curriculum development', 'student assessment', 'parent communication',
        'report writing', 'data analysis', 'professional learning', 'mentoring', 'supervision'
    ]
    
    # General professional skills
    general_skills = [
        'communication', 'leadership', 'teamwork', 'collaboration', 'problem solving',
        'critical thinking', 'creativity', 'organization', 'time management', 'adaptability',
        'empathy', 'patience', 'cultural awareness', 'diversity', 'inclusion', 'equity'
    ]
    
    # Combine all skills
    all_skills = education_skills + general_skills
    
    found_skills = []
    preferred_skills = []
    
    for skill in all_skills:
        if skill in combined_text:
            found_skills.append(skill.title())
    
    # Look for preferred/desired skills section
    preferred_patterns = [
        r'preferred[^:]*:([^.]*)',
        r'desired[^:]*:([^.]*)', 
        r'advantageous[^:]*:([^.]*)',
        r'would be an advantage[^:]*:([^.]*)',
        r'highly regarded[^:]*:([^.]*)'
    ]
    
    for pattern in preferred_patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        for match in matches:
            for skill in all_skills:
                if skill in match.lower():
                    preferred_skills.append(skill.title())
    
    # If no specific preferred skills found, use last 50% of found skills as preferred
    if not preferred_skills and found_skills:
        mid_point = len(found_skills) // 2
        preferred_skills = found_skills[mid_point:]
        found_skills = found_skills[:mid_point] if mid_point > 0 else found_skills
    
    # Ensure we have some skills
    if not found_skills and not preferred_skills:
        # Extract from title
        title_lower = title.lower()
        if 'teacher' in title_lower:
            found_skills.append('Teaching')
        if 'psychology' in title_lower:
            found_skills.append('Psychology')
        if 'primary' in title_lower:
            found_skills.append('Primary Education')
        if 'secondary' in title_lower:
            found_skills.append('Secondary Education')
    
    # Remove duplicates while preserving order
    found_skills = list(dict.fromkeys(found_skills))
    preferred_skills = list(dict.fromkeys(preferred_skills))
    
    # Limit to reasonable lengths and convert to CSV
    skills_csv = ', '.join(found_skills[:10])[:200]  # Max 200 chars
    preferred_csv = ', '.join(preferred_skills[:10])[:200]  # Max 200 chars
    
    return skills_csv, preferred_csv


def _extract_closing_date(text: str, kv: dict) -> str:
    """Extract job closing date from text and key-value pairs."""
    # First check key-value pairs
    for key in kv:
        key_lower = key.lower()
        if any(term in key_lower for term in ['closing', 'close', 'deadline', 'due', 'expires', 'until']):
            date_value = kv[key].strip()
            if date_value:
                return date_value
    
    # Enhanced pattern matching for dates - ordered by specificity
    date_patterns = [
        # Format: "Closes: Oct 26, 2025" or "Closes: Sep 21, 2025"
        r'(?:closes?|closing|deadline|due|expires?|until|by)\s*:?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})',
        
        # Format: "Closes: 26 October 2025" or "Closes: 21 September 2025"
        r'(?:closes?|closing|deadline|due|expires?|until|by)\s*:?\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9},?\s+\d{4})',
        
        # Format: "Closes: 26/10/2025" or "Closes: 21/09/2025"
        r'(?:closes?|closing|deadline|due|expires?|until|by)\s*:?\s*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
        
        # Format: "Applications close Oct 26, 2025"
        r'applications?\s+(?:close|due|must be received)\s+(?:by|on)?\s*:?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})',
        
        # Format: "Applications close 26 October 2025"
        r'applications?\s+(?:close|due|must be received)\s+(?:by|on)?\s*:?\s*(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9},?\s+\d{4})',
        
        # Format: "Applications close 26/10/2025"
        r'applications?\s+(?:close|due|must be received)\s+(?:by|on)?\s*:?\s*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
        
        # Generic month-day-year patterns
        r'(?:closes?|ends?|expires?|deadline)\s*:?\s*([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})',
        r'(?:closes?|ends?|expires?|deadline)\s*:?\s*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})',
        
        # Standalone date patterns (fallback)
        r'\b([A-Za-z]{3,9}\s+\d{1,2},?\s+2025)\b',  # Focus on 2025 dates
        r'\b(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]2025)\b',  # Focus on 2025 dates
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            extracted_date = match.group(1).strip()
            # Basic validation - ensure we have a reasonable date
            if len(extracted_date) > 5 and ('2024' in extracted_date or '2025' in extracted_date or '2026' in extracted_date):
                return extracted_date
    
    return ""


def _extract_closing_date_from_html(soup: BeautifulSoup) -> str:
    """Extract closing date from HTML structure specifically."""
    # Look for specific HTML elements that might contain closing dates
    date_selectors = [
        '[class*="close"]',
        '[class*="deadline"]', 
        '[class*="date"]',
        '.date-posted',
        '.closing-date',
        '.deadline',
        '.expires',
        'time',
        '[datetime]'
    ]
    
    for selector in date_selectors:
        elements = soup.select(selector)
        for element in elements:
            text = element.get_text(strip=True)
            if text and len(text) > 5:
                # Check if this looks like a closing date
                if any(keyword in text.lower() for keyword in ['close', 'deadline', 'due', 'expire', 'until']):
                    # Extract date from this text
                    date_match = re.search(r'([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', text)
                    if not date_match:
                        date_match = re.search(r'(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})', text)
                    if date_match:
                        return date_match.group(1).strip()
    
    # Look for datetime attributes
    datetime_elements = soup.find_all(attrs={'datetime': True})
    for element in datetime_elements:
        datetime_val = element.get('datetime', '')
        if datetime_val and ('2024' in datetime_val or '2025' in datetime_val or '2026' in datetime_val):
            # Format datetime to readable format
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(datetime_val.replace('Z', '+00:00'))
                return dt.strftime('%b %d, %Y')
            except:
                return datetime_val
    
    return ""


def extract_detail_fields(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    candidates = [
        soup.select_one(".job-description"),
        soup.select_one(".description"),
        soup.select_one("main"),
        soup.select_one("article"),
        soup.select_one(".prose"),
        soup.select_one("[role='main']"),
    ]
    container = next((c for c in candidates if c and c.get_text(strip=True)), soup.body)
    full_text = container.get_text("\n", strip=True)

    # Build raw key/value pairs from unfiltered text
    raw_kv: Dict[str, str] = {}
    for line in [l.strip() for l in full_text.split("\n") if ":" in l]:
        try:
            key, val = line.split(":", 1)
            key = re.sub(r"\s+", " ", key).strip()
            val = re.sub(r"\s+", " ", val).strip()
            if len(key) <= 60 and len(val) >= 1:
                raw_kv[key] = val
        except ValueError:
            continue

    # Normalize keys (handle cases like "Salary ﻿Location:")
    kv: Dict[str, str] = {}
    for k, v in raw_kv.items():
        lk = k.lower()
        if "location" in lk:
            kv["Location"] = v
        elif "job type" in lk or "employment type" in lk or "position type" in lk:
            kv["Job Type"] = v
        elif "salary" in lk or "remuneration" in lk or re.search(r"\bpay\b", lk):
            kv["Salary"] = v
        elif "date advertised" in lk or "advertised" in lk or "date posted" in lk:
            kv["Date Advertised"] = v
        else:
            kv[k] = v

    title = None
    h = soup.select_one("h1") or soup.select_one("h2")
    if h:
        title = h.get_text(strip=True)

    company = None
    header_block = soup.select_one("h1").find_parent() if soup.select_one("h1") else None
    if header_block:
        txt = header_block.get_text(" • ", strip=True)
        m = re.search(r"\b([A-Z][A-Za-z&'\- ]{2,})\b.*?\b(College|School|Schools|University|Institute)\b", txt)
        if m:
            company = m.group(0)
    if not company:
        cand = soup.select_one(".company, .employer, .text-gray-600, strong")
        if cand:
            company = cand.get_text(strip=True)

    # Prefer explicit KV location; otherwise fallback by scanning for state abbreviations
    location_text = kv.get("Location", "")
    if not location_text:
        loc_cand = soup.find(string=re.compile(r"\b(NSW|VIC|QLD|WA|SA|TAS|NT|ACT)\b"))
        if loc_cand:
            location_text = str(loc_cand)

    job_type_raw = kv.get("Job Type", "")
    salary_raw = kv.get("Salary", "") or _find_salary_raw(full_text)
    date_advertised = kv.get("Date Advertised", "")

    # Extract HTML description
    html_description = _extract_html_description(soup)
    
    # Clean description: remove ads/CTAs (for text analysis)
    description_text = _clean_description(full_text)
    
    # Extract skills from description
    skills, preferred_skills = _extract_skills_from_description(description_text, title or "")
    
    # Extract closing date - try both text and HTML
    closing_date = _extract_closing_date(full_text, kv)
    if not closing_date:
        # Try extracting from HTML structure as well
        closing_date = _extract_closing_date_from_html(soup)
    
    if closing_date:
        logger.info(f"✅ Found closing date: '{closing_date}' for job: {title[:50] if title else 'Unknown'}")
    else:
        logger.warning(f"❌ No closing date found for job: {title[:50] if title else 'Unknown'}")
        # Log a sample of the text to help debug
        sample_text = full_text[:500] if full_text else "No text available"
        logger.debug(f"Sample text for debugging: {sample_text}")

    return {
        "title": title,
        "company": company,
        "location_text": location_text,
        "job_type_raw": job_type_raw,
        "salary_raw": salary_raw,
        "date_advertised": date_advertised,
        "description": html_description,  # Now HTML format
        "description_text": description_text,  # Keep text version for analysis
        "skills": skills,
        "preferred_skills": preferred_skills,
        "closing_date": closing_date,
        "kv": kv,
    }


def _collect_listing_links(page, max_pages: int = 9999, pause_ms: int = 500) -> list:
    links_all = []
    seen = set()

    def _current_job_links() -> list:
        # Prefer explicit "View Job" anchors to avoid header/footer links
        hrefs = page.eval_on_selector_all(
            "a:has-text('View Job')",
            "els => Array.from(new Set(els.map(e => e.href))).filter(x => x && x.includes('/school-jobs/'))",
        ) or []
        if not hrefs:
            hrefs = page.eval_on_selector_all(
                "a[href*='/school-jobs/']",
                "els => Array.from(new Set(els.map(e => e.href))).filter(x => x && x.includes('/school-jobs/'))",
            ) or []
        # Keep only detail pages with numeric id if present
        hrefs = [u for u in hrefs if re.search(r"/school-jobs/.+?/\\d+", u)] or hrefs
        return hrefs

    def _links_signature() -> str:
        try:
            links = _current_job_links()
            return "|".join(sorted(links))
        except Exception:
            return ""

    def collect_from_current_page():
        nonlocal links_all
        page.wait_for_selector("a:has-text('View Job')", timeout=15000)
        links = _current_job_links()
        new = [u for u in links if u not in seen]
        for u in new:
            seen.add(u)
        links_all.extend(new)
        logger.info("Collected %d links on this page (%d total)", len(new), len(links_all))

    def click_next_if_possible() -> bool:
        # Try a range of selectors; prefer the last visible control on the pager
        next_selectors = [
            "nav[aria-label*='Pagination'] a[rel='next']",
            "nav[aria-label*='Pagination'] button[aria-label='Next']",
            "nav[aria-label*='Pagination'] a",
            "nav[aria-label*='Pagination'] button",
            ".pagination a",
            ".pagination button",
            "a:has-text('Next')",
            "button:has-text('Next')",
            "a:has-text('›'), button:has-text('›'), a:has-text('»'), button:has-text('»'), a:has-text('►'), button:has-text('►'), a:has-text('>'), button:has-text('>')",
        ]
        for sel in next_selectors:
            try:
                loc = page.locator(sel)
                count = loc.count()
                if not count:
                    continue
                candidate = loc.nth(count - 1)
                if not candidate.is_visible():
                    continue
                aria_disabled = candidate.get_attribute('aria-disabled')
                if aria_disabled and aria_disabled.lower() == 'true':
                    continue
                old_set = set(_current_job_links())
                prev_url = page.url
                candidate.scroll_into_view_if_needed()
                candidate.click(force=True)
                page.wait_for_timeout(pause_ms)
                # Wait for DOM update without navigation
                for _ in range(20):
                    page.wait_for_timeout(300)
                    new_set = set(_current_job_links())
                    if (new_set and new_set != old_set) or page.url != prev_url:
                        return True
                # If not changed, try another selector
            except Exception:
                continue
        # JS fallback: click any button/anchor with arrow glyphs at bottom of page
        try:
            page.evaluate("""
                const glyphs = /[›»▶►▸>]/;
                const buttons = Array.from(document.querySelectorAll('button,a'));
                const candidates = buttons.filter(b => glyphs.test((b.textContent||'').trim()));
                const el = candidates[candidates.length - 1];
                if (el) { el.click(); return true;} else { return false; }
            """)
            page.wait_for_timeout(pause_ms)
            page.wait_for_load_state('networkidle')
            if set(_current_job_links()):
                return True
        except Exception:
            pass
        return False

    def click_next_by_footer_geometry() -> bool:
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            page.wait_for_timeout(200)
            # Pick a clickable small control near the bottom-right of the viewport
            script = """
            const bottom = window.scrollY + window.innerHeight;
            const nodes = Array.from(document.querySelectorAll('a,button'))
              .map(el => {
                const r = el.getBoundingClientRect();
                const text = (el.textContent||'').trim();
                return {el, y: r.top, x: r.left, w: r.width, h: r.height, text};
              })
              .filter(n => n.w>10 && n.h>10 && n.y>window.innerHeight-250);
            // Prefer arrow-looking text or very short labels
            const arrowRe = /[›»▶►▸>]/;
            nodes.sort((a,b)=> (arrowRe.test(b.text)-arrowRe.test(a.text)) || (a.text.length - b.text.length) || (b.x - a.x));
            const cand = nodes[0];
            if (cand && cand.el) { cand.el.click(); return true; }
            return false;
            """
            prev = set(_current_job_links())
            page.evaluate(script)
            page.wait_for_timeout(pause_ms)
            # Wait for DOM update
            for _ in range(20):
                page.wait_for_timeout(300)
                new = set(_current_job_links())
                if new != prev:
                    return True
            return False
        except Exception:
            return False

    def click_next_by_number_sibling() -> bool:
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            page.wait_for_timeout(200)
            script = r"""
            function clickable(el){
              if(!el) return null;
              if(el.tagName==='A' || el.tagName==='BUTTON') return el;
              return el.closest('a,button');
            }
            const nodes = Array.from(document.querySelectorAll('body *'));
            const candidates = nodes.filter(n=>{
              const t=(n.textContent||'').trim();
              if(!/^\d{1,3}$/.test(t)) return false;
              const r=n.getBoundingClientRect();
              return r.width>10 && r.height>10 && r.top>window.innerHeight-280; // near bottom
            });
            // prefer the last numeric page badge
            const badge = candidates[candidates.length-1];
            if(!badge) return false;
            let next = badge.nextElementSibling;
            if(!next){
              const parent = badge.parentElement;
              if(parent){ next = parent.children[parent.children.length-1]; }
            }
            const clickEl = clickable(next);
            if(clickEl){ clickEl.click(); return true; }
            return false;
            """
            before = set(_current_job_links())
            page.evaluate(script)
            page.wait_for_timeout(pause_ms)
            for _ in range(20):
                page.wait_for_timeout(300)
                after = set(_current_job_links())
                if after != before:
                    return True
            return False
        except Exception:
            return False

    # Navigate to base URL with retry logic and increased timeout
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to navigate to {BASE_URL} (attempt {attempt + 1}/{max_retries})")
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)  # Increased timeout to 60s
            page.wait_for_timeout(2000)  # Additional wait for page to stabilize
            logger.info("Successfully navigated to base URL")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to navigate to {BASE_URL} after {max_retries} attempts: {e}")
                raise
            logger.warning(f"Navigation attempt {attempt + 1} failed: {e}. Retrying...")
            page.wait_for_timeout(3000)  # Wait before retry
    
    collect_from_current_page()

    # Try to detect URL pattern for direct page navigation (preferred)
    pagination_hrefs = page.eval_on_selector_all(
        "a",
        "els => Array.from(new Set(els.map(e => e.getAttribute('href')))).filter(Boolean)",
    )
    next_url_maker = None
    if pagination_hrefs:
        # Pattern 1: ?page=2 or &page=2
        m = next((re.search(r"([?&])page=(\d+)", h) for h in pagination_hrefs if h and 'page=' in h), None)
        if m and m.group(0):
            sep = m.group(1)
            def _make(n:int, base=BASE_URL, sep=sep):
                joiner = '&' if ('?' in base and sep == '&') else '?'
                if 'page=' in base:
                    # Replace existing page param
                    return re.sub(r"([?&])page=\d+", f"{sep}page={n}", base)
                return f"{base}{joiner}page={n}"
            next_url_maker = _make
        else:
            # Pattern 2: /page/2 or /p/2
            m2 = next((re.search(r"/(?:page|p)/(\d+)", h) for h in pagination_hrefs if h and re.search(r"/(?:page|p)/\d+", h)), None)
            if m2 and m2.group(0):
                def _make(n:int, base=BASE_URL):
                    return base.rstrip('/') + f"/page/{n}"
                next_url_maker = _make

    page_index = 1
    while page_index < max_pages:
        page_index += 1
        moved = False
        if next_url_maker:
            # Try direct URL pattern
            try:
                target = next_url_maker(page_index)
                page.goto(target, wait_until='networkidle')
                moved = True
            except Exception:
                moved = False
        if not moved:
            # Fallback: click next
            moved = click_next_if_possible()
            if not moved:
                moved = click_next_by_footer_geometry()
            if not moved:
                moved = click_next_by_number_sibling()
            if not moved:
                break
        logger.info("Moved to listing page %d", page_index)
        collect_from_current_page()
    return links_all


def scrape_teaching_jobs(max_jobs: int = 100, max_pages: int = 9999):
    """Scrape list and detail pages with Playwright, then write to DB after the browser closes.

    This avoids Django's SynchronousOnlyOperation by ensuring ORM calls occur outside
    of the Playwright event loop.
    """
    logger.info("Starting scrape for %s (max_jobs=%s, max_pages=%s)", SOURCE, max_jobs, max_pages)
    scraped_items: List[Dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        page = context.new_page()
        
        # Set additional navigation timeout for all page operations
        page.set_default_navigation_timeout(60000)  # 60 seconds
        page.set_default_timeout(30000)  # 30 seconds for other operations
        links = _collect_listing_links(page, max_pages=max_pages)
        if max_jobs:
            random.shuffle(links)
            links = links[:max_jobs]
        logger.info("Collected %d job links to process", len(links))

        seen_urls = set()
        for i, url in enumerate(links):
            if url in seen_urls:
                continue
            seen_urls.add(url)
            detail = context.new_page()
            try:
                # Navigate to detail page with retry logic
                max_detail_retries = 2
                detail_success = False
                for detail_attempt in range(max_detail_retries):
                    try:
                        detail.goto(url, wait_until="domcontentloaded", timeout=45000)
                        detail.wait_for_timeout(1000)  # Wait for page to stabilize
                        detail_success = True
                        break
                    except Exception as nav_e:
                        if detail_attempt == max_detail_retries - 1:
                            logger.warning("Failed to navigate to detail page %s after %d attempts: %s", url, max_detail_retries, nav_e)
                            raise nav_e
                        logger.warning("Detail navigation attempt %d failed for %s: %s. Retrying...", detail_attempt + 1, url, nav_e)
                        detail.wait_for_timeout(2000)
                
                if detail_success:
                    html = detail.content()
                    fields = extract_detail_fields(html)
                    scraped_items.append({"url": url, **fields})
                    logger.info("Scraped detail: %s (%d/%d)", url, i+1, len(links))
            except Exception as e:
                logger.exception("Failed to scrape %s: %s", url, e)
            finally:
                detail.close()
                
            # Add a small delay between requests to be respectful
            if i < len(links) - 1:  # Don't wait after the last item
                page.wait_for_timeout(random.randint(1000, 3000))

        browser.close()

    # ORM work outside Playwright context
    user = get_or_create_scraper_user()
    urls = [item["url"] for item in scraped_items]
    existing = set(JobPosting.objects.filter(external_url__in=urls).values_list("external_url", flat=True))
    to_insert = [item for item in scraped_items if item["url"] not in existing]

    inserted = 0
    for item in to_insert:
        try:
            title = item["title"] or "Teaching Position"
            company = get_or_create_company(item["company"] or "Educational Institution")
            location = get_or_create_location(item["location_text"]) if item["location_text"] else None
            job_type = map_job_type(item["job_type_raw"]) 
            s_min, s_max, s_cur, s_type, s_raw = parse_salary(item["salary_raw"]) 
            category = JobCategorizationService.categorize_job(title, item["description"]) or "education"

            job = JobPosting(
                title=title,
                description=item["description"][:20000],  # HTML description
                company=company,
                posted_by=user,
                location=location,
                job_category=category,
                job_type=job_type,
                salary_min=s_min,
                salary_max=s_max,
                salary_currency=s_cur,
                salary_type=s_type,
                salary_raw_text=s_raw,
                external_source=SOURCE,
                external_url=item["url"],
                posted_ago="",
                date_posted=None,
                skills=item["skills"][:200] if item["skills"] else "",  # Skills field
                preferred_skills=item["preferred_skills"][:200] if item["preferred_skills"] else "",  # Preferred skills field
                job_closing_date=item.get("closing_date", "") or "",  # Closing date field
                tags=", ".join(JobCategorizationService.get_job_keywords(title, item.get("description_text", item["description"])))[:200],
                additional_info={
                    "job_type_raw": item["job_type_raw"],
                    "salary_raw": item["salary_raw"],
                    "date_advertised": item["date_advertised"],
                    "closing_date": item["closing_date"],
                    "skills_extracted": item["skills"],
                    "preferred_skills_extracted": item["preferred_skills"],
                    "kv_pairs": item["kv"],
                },
            )
            job.save()
            inserted += 1
            
            # Log closing date specifically for debugging
            closing_date_info = f" | Closing: {item.get('closing_date', 'None')}" if item.get('closing_date') else " | Closing: None"
            logger.info("Saved: %s @ %s [%s]%s", title, company.name, item["url"], closing_date_info)
        except Exception as e:
            logger.exception("Failed to save job %s: %s", item.get("url"), e)

    logger.info("Done. Inserted %d new jobs from %s. Log: %s", inserted, SOURCE, LOG_FILE)


if __name__ == "__main__":
    # Optional CLI: arg1 = max_jobs, arg2 = max_pages
    mj = 100
    mp = 9999
    try:
        if len(sys.argv) >= 2 and sys.argv[1].isdigit():
            mj = int(sys.argv[1])
        if len(sys.argv) >= 3 and sys.argv[2].isdigit():
            mp = int(sys.argv[2])
    except Exception:
        pass
    scrape_teaching_jobs(max_jobs=mj, max_pages=mp)


def run(max_jobs=None, max_pages=9999):
    """Automation entrypoint for TeachingJobs scraper."""
    try:
        scrape_teaching_jobs(max_jobs=max_jobs, max_pages=max_pages)
        return {
            'success': True,
            'message': f'TeachingJobs scraping completed (limit {max_jobs}, pages {max_pages})'
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
