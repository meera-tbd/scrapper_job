import os
import sys
import re
from datetime import datetime
from decimal import Decimal
from typing import Tuple
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


def get_or_create_company(logo_url: str = "") -> Company:
    company, created = Company.objects.get_or_create(
        name="Staff Australia",
        defaults={
            "website": "https://staffaus.com.au",
            "description": "Staff Australia recruitment",
            "logo": logo_url,
        },
    )
    # Update logo if company exists but logo is empty and we have a new one
    if not created and logo_url and not company.logo:
        company.logo = logo_url
        company.save(update_fields=['logo'])
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
    company = get_or_create_company(data.get('company_logo', ''))
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
            "skills": data.get("skills", ""),
            "preferred_skills": data.get("preferred_skills", ""),
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
        obj.skills = data.get("skills", obj.skills)
        obj.preferred_skills = data.get("preferred_skills", obj.preferred_skills)
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


def extract_description_html_from_heading(soup: BeautifulSoup) -> str:
    """Extract HTML description preserving formatting from the 'Job Description' section."""
    heading = None
    for tag in soup.find_all(["h1", "h2", "h3", "strong"]):
        if tag.get_text(strip=True).lower().startswith("job description"):
            heading = tag
            break
    if not heading:
        return ""

    # Collect HTML elements after the heading
    elements = []
    stop_tags = {"h1", "h2"}
    stop_texts = {"similar jobs", "details", "employers", "job seekers"}

    for sib in heading.find_all_next():
        if sib is heading:
            continue
        
        txt = sib.get_text(" ", strip=True)
        if not txt:
            continue
            
        # Stop at next major section
        if (sib.name in stop_tags and sib is not heading) or any(t in txt.lower() for t in stop_texts):
            break
            
        # Include relevant content elements
        if sib.name in {"p", "ul", "ol", "li", "div", "strong", "em", "b", "i"}:
            elements.append(str(sib))
        elif sib.name in {"span"} and len(txt) > 20:
            # Convert long spans to paragraphs
            elements.append(f"<p>{txt}</p>")
        
        # Cap runaway capture
        if len("".join(elements)) > 8000:
            break

    html_content = "\n".join(elements)
    return html_content.strip()


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


def sanitize_description_html(html: str) -> str:
    """Return clean, safe HTML from raw description content while preserving formatting."""
    if not html:
        return ""
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # Remove unwanted elements entirely
        unwanted_selectors = [
            "script", "style", "form", "nav", "header", "footer",
            ".sidebar", ".related", ".share", ".apply", ".application",
            ".newsletter", ".footer", ".navigation"
        ]
        
        for sel in unwanted_selectors:
            for n in soup.select(sel):
                n.decompose()
        
        # Be more selective about what we remove - preserve job content
        elements_to_remove = []
        
        for element in soup.find_all(['p', 'div', 'span', 'ul', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            element_text = element.get_text(strip=True).lower()
            
            # Only remove obvious navigation/footer content, NOT job-related content
            unwanted_patterns = [
                'privacy policy', 'terms of use', 'diversity and inclusion', 'human rights',
                'sign up to our newsletter', 'register for job alerts', 'follow us',
                'social media', 'newsletter', 'subscribe', 'copyright', '© 20',
                'all rights reserved'
            ]
            
            # Remove ONLY if it's clearly navigation/footer AND short
            if (any(pattern in element_text for pattern in unwanted_patterns) and 
                len(element_text) < 150):
                elements_to_remove.append(element)
                continue
            
            # Remove very short navigation elements but preserve job content
            if (len(element_text) < 5 and element.name not in ['li', 'h2', 'h3', 'h4', 'h5', 'h6']):
                elements_to_remove.append(element)
        
        for element in elements_to_remove:
            try:
                element.decompose()
            except:
                pass
        
        # Clean up structure and keep only safe tags
        allowed_tags = {
            "p", "ul", "ol", "li", "strong", "em", "b", "i",
            "br", "h1", "h2", "h3", "h4", "h5", "h6", "a"
        }
        
        # Convert divs with content to paragraphs
        for div in soup.find_all('div'):
            div_text = div.get_text(strip=True)
            if div_text and len(div_text) > 10 and not div.find(['p', 'ul', 'ol', 'h1', 'h2', 'h3']):
                div.name = 'p'
        
        # Remove disallowed tags but keep their content
        for tag in list(soup.find_all(True)):
            if tag.name not in allowed_tags:
                tag.unwrap()
                continue
            
            # Clean all attributes except href for links
            attrs = dict(tag.attrs)
            for attr in attrs:
                if tag.name == "a" and attr == "href":
                    continue
                del tag.attrs[attr]
        
        # Normalize heading levels to h3
        for h in soup.find_all(['h1', 'h2', 'h4', 'h5', 'h6']):
            h.name = 'h3'
        
        # Get cleaned HTML
        html_clean = str(soup)
        
        # Remove html/body wrappers
        html_clean = re.sub(r'^\s*<(?:html|body)[^>]*>|</(?:html|body)>\s*$', '', html_clean, flags=re.I)
        
        # Clean up whitespace
        html_clean = re.sub(r'\n{3,}', '\n\n', html_clean)
        html_clean = re.sub(r'>\s+<', '><', html_clean)
        
        # Remove empty tags
        html_clean = re.sub(r'<(\w+)[^>]*>\s*</\1>', '', html_clean)
        
        # If result is empty or too short, return some basic content
        if not html_clean.strip() or len(html_clean.strip()) < 50:
            return "<p>Job description content not available</p>"
        
        return html_clean.strip()
        
    except Exception as e:
        logger.warning(f"Error sanitizing HTML: {e}")
        # Fallback: convert to text and wrap in paragraphs
        if html:
            text = BeautifulSoup(html, "html.parser").get_text()
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if lines:
                return '\n'.join(f'<p>{ln}</p>' for ln in lines[:10])  # Limit to first 10 lines
        return "<p>Job description content not available</p>"


def extract_skills_from_description(description: str, title: str = "") -> Tuple[str, str]:
    """Extract skills and preferred skills from job description and title."""
    if not description:
        return "", ""
    
    # Convert HTML to text for analysis
    text = re.sub(r'<[^>]+>', ' ', description).lower()
    text = re.sub(r'\s+', ' ', text).strip()
    title_text = title.lower() if title else ""
    
    # Comprehensive skills database for Australia
    technical_skills = [
        # Office & Business Software
        'microsoft office', 'excel', 'word', 'powerpoint', 'outlook', 'teams', 'sharepoint',
        'google workspace', 'gmail', 'google drive', 'google docs', 'google sheets',
        'adobe', 'pdf', 'photoshop', 'canva', 'crm', 'salesforce', 'hubspot',
        
        # Programming & Tech
        'python', 'java', 'javascript', 'html', 'css', 'sql', 'database', 'mysql', 'postgresql',
        'web development', 'software development', 'programming', 'coding', 'git', 'github',
        'aws', 'azure', 'cloud computing', 'api', 'rest api', 'json', 'xml',
        
        # Industry Specific
        'healthcare', 'nursing', 'patient care', 'medical records', 'clinical', 'pharmacy',
        'construction', 'building', 'safety', 'whs', 'occupational health', 'first aid', 'cpr',
        'engineering', 'mechanical', 'electrical', 'civil', 'autocad', 'solidworks',
        'finance', 'accounting', 'bookkeeping', 'payroll', 'tax', 'financial analysis',
        'retail', 'customer service', 'sales', 'pos', 'inventory', 'merchandising',
        'logistics', 'supply chain', 'warehouse', 'forklift', 'transport', 'delivery',
        'education', 'teaching', 'training', 'curriculum', 'lesson planning',
        'hospitality', 'food service', 'cooking', 'kitchen', 'rsa', 'food safety',
        
        # Business Skills
        'project management', 'agile', 'scrum', 'lean', 'six sigma', 'process improvement',
        'data analysis', 'reporting', 'analytics', 'business intelligence', 'tableau', 'power bi',
        'marketing', 'digital marketing', 'social media', 'seo', 'content marketing',
        'human resources', 'recruitment', 'payroll', 'performance management',
        
        # Certifications & Qualifications
        'certificate iii', 'certificate iv', 'cert iii', 'cert iv', 'diploma', 'bachelor',
        'masters', 'phd', 'professional development', 'continuing education',
        'white card', 'blue card', 'working with children', 'police check',
        'drivers license', 'forklift license', 'crane license', 'trade license'
    ]
    
    soft_skills = [
        'communication', 'verbal communication', 'written communication', 'listening',
        'teamwork', 'collaboration', 'team player', 'interpersonal', 'relationship building',
        'leadership', 'management', 'supervision', 'mentoring', 'coaching',
        'problem solving', 'analytical thinking', 'critical thinking', 'decision making',
        'time management', 'organisation', 'planning', 'prioritisation', 'multitasking',
        'adaptability', 'flexibility', 'resilience', 'stress management',
        'attention to detail', 'accuracy', 'quality', 'thoroughness',
        'creativity', 'innovation', 'initiative', 'proactive', 'self-motivated',
        'customer focus', 'client service', 'stakeholder management',
        'negotiation', 'conflict resolution', 'influencing', 'persuasion',
        'cultural awareness', 'diversity', 'inclusion', 'empathy'
    ]
    
    all_skills = technical_skills + soft_skills
    found_skills = []
    
    # Find skills in text with pattern matching
    for skill in all_skills:
        skill_lower = skill.lower()
        
        # Exact word boundary match
        pattern = r'\b' + re.escape(skill_lower) + r'\b'
        if re.search(pattern, text) or re.search(pattern, title_text):
            found_skills.append(skill.title())
            continue
        
        # Partial match for compound skills
        if ' ' in skill_lower:
            skill_parts = skill_lower.split()
            if all(part in text or part in title_text for part in skill_parts):
                found_skills.append(skill.title())
    
    # Remove duplicates while preserving order
    found_skills = list(dict.fromkeys(found_skills))
    
    # Split skills between required and preferred based on context
    required_skills = []
    preferred_skills = []
    
    # Look for sections that indicate required vs preferred
    required_indicators = [
        'essential', 'required', 'must have', 'mandatory', 'necessary',
        'minimum requirements', 'key requirements', 'you will need',
        'successful candidate will', 'candidate must', 'experience in'
    ]
    
    preferred_indicators = [
        'preferred', 'desirable', 'advantageous', 'beneficial', 'nice to have',
        'would be an advantage', 'highly regarded', 'valued', 'plus',
        'bonus', 'additional', 'ideal candidate', 'would be great'
    ]
    
    # Analyze context for each skill
    for skill in found_skills:
        skill_lower = skill.lower()
        skill_context = ""
        
        # Find sentences containing the skill
        sentences = re.split(r'[.!?]+', text)
        for sentence in sentences:
            if skill_lower in sentence:
                skill_context += sentence + " "
        
        # Determine if it's required or preferred based on context
        is_required = any(indicator in skill_context for indicator in required_indicators)
        is_preferred = any(indicator in skill_context for indicator in preferred_indicators)
        
        if is_required and not is_preferred:
            required_skills.append(skill)
        elif is_preferred and not is_required:
            preferred_skills.append(skill)
        else:
            # Default to required if unclear
            required_skills.append(skill)
    
    # Ensure we have some skills, add defaults if none found
    if not required_skills and not preferred_skills:
        required_skills = ['Communication', 'Teamwork', 'Problem Solving', 'Time Management']
        preferred_skills = ['Leadership', 'Initiative', 'Attention To Detail', 'Customer Focus']
    
    # If we only have one category, create the other from it
    if required_skills and not preferred_skills:
        preferred_skills = required_skills[-2:] if len(required_skills) > 2 else required_skills
    elif preferred_skills and not required_skills:
        required_skills = preferred_skills[:3] if len(preferred_skills) > 3 else preferred_skills
    
    # Limit to reasonable numbers and ensure uniqueness
    required_skills = list(dict.fromkeys(required_skills))[:8]
    preferred_skills = list(dict.fromkeys(preferred_skills))[:8]
    
    return ', '.join(required_skills), ', '.join(preferred_skills)


def extract_company_logo(page) -> str:
    """Extract company logo URL from the page."""
    try:
        # Try multiple selectors for logo
        logo_selectors = [
            'img[alt*="Staff Australia" i]',
            'img[src*="logo" i]',
            '.logo img',
            '.header img',
            '.brand img',
            'img[alt*="logo" i]'
        ]
        
        for selector in logo_selectors:
            try:
                logo_element = page.locator(selector).first
                if logo_element.count() > 0:
                    logo_url = logo_element.get_attribute('src')
                    if logo_url:
                        # Convert relative URLs to absolute
                        if logo_url.startswith('//'):
                            logo_url = 'https:' + logo_url
                        elif logo_url.startswith('/'):
                            logo_url = 'https://staffaus.com.au' + logo_url
                        elif not logo_url.startswith('http'):
                            logo_url = 'https://staffaus.com.au/' + logo_url
                        
                        # Validate it's a reasonable logo URL
                        if any(ext in logo_url.lower() for ext in ['.png', '.jpg', '.jpeg', '.svg', '.gif']):
                            return logo_url
            except Exception:
                continue
        
        # Fallback logo
        return 'https://staffaus.com.au/wp-content/uploads/2020/07/Staff-Australia-Logo.png'
        
    except Exception as e:
        logger.warning(f"Error extracting company logo: {e}")
        return 'https://staffaus.com.au/wp-content/uploads/2020/07/Staff-Australia-Logo.png'


def extract_posting_date(soup: BeautifulSoup, page) -> datetime:
    """Extract job posting date from the page."""
    try:
        # Try to find date in common locations
        date_selectors = [
            '.job-meta .date',
            '.posted-date',
            '.job-posted',
            '.meta-date',
            'time[datetime]',
            '.entry-meta .date'
        ]
        
        # First try with playwright selectors
        try:
            for selector in date_selectors:
                try:
                    element = page.locator(selector).first
                    if element.count() > 0:
                        date_text = element.inner_text().strip()
                        if date_text:
                            parsed_date = parse_date_text(date_text)
                            if parsed_date:
                                return parsed_date
                except Exception:
                    continue
        except Exception:
            pass
        
        # Then try with BeautifulSoup
        for selector in date_selectors:
            try:
                elements = soup.select(selector)
                for element in elements:
                    date_text = element.get_text(strip=True)
                    if date_text:
                        parsed_date = parse_date_text(date_text)
                        if parsed_date:
                            return parsed_date
                    
                    # Check for datetime attribute
                    datetime_attr = element.get('datetime')
                    if datetime_attr:
                        parsed_date = parse_date_text(datetime_attr)
                        if parsed_date:
                            return parsed_date
            except Exception:
                continue
        
        # Look for date patterns in text
        date_patterns = [
            r'posted\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
            r'(\w+\s+\d{1,2},?\s+\d{4})'
        ]
        
        page_text = soup.get_text()
        for pattern in date_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                parsed_date = parse_date_text(match)
                if parsed_date:
                    return parsed_date
        
        return None
        
    except Exception as e:
        logger.warning(f"Error extracting posting date: {e}")
        return None


def parse_date_text(date_text: str) -> datetime:
    """Parse various date formats into datetime object."""
    if not date_text:
        return None
    
    date_text = date_text.strip()
    
    try:
        # Handle relative dates
        if 'ago' in date_text.lower():
            from datetime import timedelta
            now = datetime.now()
            
            if 'day' in date_text:
                days = re.search(r'(\d+)', date_text)
                if days:
                    return now - timedelta(days=int(days.group(1)))
            elif 'week' in date_text:
                weeks = re.search(r'(\d+)', date_text)
                if weeks:
                    return now - timedelta(weeks=int(weeks.group(1)))
            elif 'month' in date_text:
                months = re.search(r'(\d+)', date_text)
                if months:
                    return now - timedelta(days=int(months.group(1)) * 30)
            elif 'hour' in date_text:
                hours = re.search(r'(\d+)', date_text)
                if hours:
                    return now - timedelta(hours=int(hours.group(1)))
        
        # Handle various date formats
        date_formats = [
            '%d %B %Y',          # 17 September 2025
            '%B %d, %Y',         # September 17, 2025
            '%d/%m/%Y',          # 17/09/2025
            '%d-%m-%Y',          # 17-09-2025
            '%Y-%m-%d',          # 2025-09-17
            '%d %b %Y',          # 17 Sep 2025
            '%b %d, %Y',         # Sep 17, 2025
            '%Y-%m-%dT%H:%M:%S', # ISO format
            '%Y-%m-%d %H:%M:%S'  # Standard datetime
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_text, fmt)
            except ValueError:
                continue
        
        return None
        
    except Exception:
        return None


def extract_job_from_detail(html: str, url: str, page=None):
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

    # Extract COMPLETE job description content including all sections
    description_html = ""
    
    # Strategy 1: Look for the main content area that contains the full job description
    main_content_selectors = [
        ".elementor-widget-theme-post-content .elementor-widget-container",
        ".entry-content .elementor-section",
        ".post-content", 
        "article .elementor-widget-text-editor .elementor-widget-container",
        ".elementor-text-editor",
        ".elementor-widget-container > div",
        ".post-content .elementor-section",
        "article .entry-content"
    ]
    
    # Try to find the container with the most comprehensive content
    best_container = None
    best_content_length = 0
    
    for selector in main_content_selectors:
        containers = soup.select(selector)
        for container in containers:
            container_text = container.get_text(strip=True)
            
            # Look for containers that have substantial content AND key job description markers
            job_indicators = [
                'job description', 'forklift driver', 'hours:', 'key responsibilities', 
                'about you', 'benefits', 'how to apply', 'why join', 'opportunity',
                'warehouse', 'shift', 'experience', 'responsibilities', 'duties'
            ]
            
            indicator_count = sum(1 for indicator in job_indicators if indicator in container_text.lower())
            
            # Prefer containers with more job indicators and substantial content
            if (len(container_text) > 200 and indicator_count >= 2 and 
                len(container_text) > best_content_length):
                best_container = container
                best_content_length = len(container_text)
    
    if best_container:
        description_html = str(best_container)
    
    # Strategy 2: Comprehensive fallback - collect ALL relevant content sections
    if not description_html:
        # Collect ALL elements that could be part of the job description
        job_elements = []
        content_sections = []
        
        # First, try to find the main article or content container
        article_containers = soup.find_all(['article', 'main', '.post', '.content'])
        if not article_containers:
            article_containers = [soup]  # Use entire page as fallback
        
        for container in article_containers:
            # Look for ALL content elements, being more inclusive
            for element in container.find_all(['p', 'ul', 'ol', 'li', 'h2', 'h3', 'h4', 'h5', 'h6', 'div', 'span']):
                element_text = element.get_text(strip=True)
                
                # Skip very short content (but allow shorter headings)
                if len(element_text) < 10 and element.name not in ['h2', 'h3', 'h4', 'h5', 'h6']:
                    continue
                
                # Skip obvious navigation and footer content
                skip_patterns = [
                    'privacy policy', 'terms of use', 'job search', 'saved jobs',
                    'staff australia today', 'call us on', 'visit www.', 'email us at',
                    'follow us', 'social media', 'newsletter', 'subscribe',
                    'copyright', '© 20', 'all rights reserved'
                ]
                
                if any(skip_pattern in element_text.lower() for skip_pattern in skip_patterns):
                    continue
                
                # Include content that's likely part of job description
                # Be very inclusive - capture everything that could be relevant
                job_keywords = [
                    'forklift', 'driver', 'warehouse', 'opportunity', 'experience', 
                    'duties', 'responsibilities', 'requirements', 'benefits', 'hours',
                    'shifts', 'rates', 'training', 'location', 'successful', 'position', 
                    'candidate', 'what\'s on offer', 'main duties', 'about you', 
                    'key responsibilities', 'how to apply', 'why join', 'staff australia',
                    'per hour', 'per annum', 'monday to friday', 'full time', 'contract',
                    'immediate start', 'ongoing', 'permanent', 'apply', 'beverage',
                    'operations', 'supply', 'reach', 'pallets', 'inventory', 'control',
                    'kpi', 'licence', 'punctual', 'working rights', 'career', 'growth'
                ]
                
                # Include if it contains job-related keywords OR is a heading/list item
                if (any(keyword in element_text.lower() for keyword in job_keywords) or 
                    element.name in ['h2', 'h3', 'h4', 'h5', 'h6', 'li'] or
                    len(element_text) > 50):  # Include longer text blocks
                    
                    # Avoid duplicating the same content
                    element_html = str(element)
                    if element_html not in job_elements:
                        job_elements.append(element_html)
        
        if job_elements:
            description_html = "\n".join(job_elements)
    
    # Final fallback: Use the heading-based approach
    if not description_html:
        description_html = extract_description_html_from_heading(soup)
    
    # Sanitize the HTML description
    description_clean = sanitize_description_html(description_html)
    
    # Extract posting date
    posting_date = None
    if page:
        posting_date = extract_posting_date(soup, page)
    
    # Extract company logo
    company_logo = ""
    if page:
        company_logo = extract_company_logo(page)
    
    # Extract skills and preferred skills from description
    skills, preferred_skills = extract_skills_from_description(
        description_clean, title or ""
    )

    return {
        "title": text_or_none(title) or "",
        "location": text_or_none(location) or "",
        "salary": text_or_none(salary_text) or "",
        "description": description_clean or "",
        "job_type": job_type or "full_time",
        "work_mode": work_mode or "",
        "external_url": url,
        "date_posted": posting_date,
        "company_logo": company_logo,
        "skills": skills,
        "preferred_skills": preferred_skills,
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
            data = extract_job_from_detail(html, url, page)

            obj, created = upsert_job(data)
            created_count += int(created)
            updated_count += int(not created)
            processed += 1
            logger.info("Saved: %s (%s) | Skills: %s | Preferred: %s", 
                       obj.title, "created" if created else "updated",
                       data.get('skills', '')[:50] + '...' if len(data.get('skills', '')) > 50 else data.get('skills', ''),
                       data.get('preferred_skills', '')[:50] + '...' if len(data.get('preferred_skills', '')) > 50 else data.get('preferred_skills', ''))

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



