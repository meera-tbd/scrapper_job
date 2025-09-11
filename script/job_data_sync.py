#!/usr/bin/env python3
"""
Job Data Synchronization Script
===============================

This script fetches job data from your scrapper database and syncs it to multiple job portals:
- evoljobs.com (live)
- flyoverseas.ai (live) 
- Local LAN job portal

Features:
- Database connection to scrapper
- Multi-portal data pushing
- Data transformation for each portal
- Batch processing with rate limiting
- Comprehensive error handling and logging
- Duplicate detection and incremental sync
"""

import json
import os
import sys
import time
import logging
import hashlib
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import sqlite3
from dataclasses import dataclass

# Load environment variables
load_dotenv()

@dataclass
class JobData:
    """Job data structure for consistent handling across portals."""
    job_id: str
    title: str
    company: str
    location: str
    description: str
    salary: Optional[str]
    job_type: str  # full-time, part-time, contract, etc.
    experience_level: str
    skills: List[str]
    posted_date: datetime
    application_url: str
    source_site: str
    category: str
    remote_allowed: bool
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'job_id': self.job_id,
            'title': self.title,
            'company': self.company,
            'location': self.location,
            'description': self.description,
            'salary': self.salary,
            'job_type': self.job_type,
            'experience_level': self.experience_level,
            'skills': self.skills,
            'posted_date': self.posted_date.isoformat() if self.posted_date else None,
            'application_url': self.application_url,
            'source_site': self.source_site,
            'category': self.category,
            'remote_allowed': self.remote_allowed
        }

class DatabaseConnector:
    """Handle connections to different database types."""
    
    def __init__(self, db_config: Dict):
        self.config = db_config
        self.connection = None
        # Normalize and keep db type for later use (param style, etc.)
        self.db_type = self.config.get('type', 'sqlite').lower()
        # Optional explicit table names for direct SQL mode
        self.table_names: Dict[str, str] = self.config.get('tables', {}) if isinstance(self.config.get('tables', {}), dict) else {}
        
    def _setup_django(self):
        """Ensure Django is configured for ORM access."""
        # Add project root to sys.path and setup settings if missing
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        if not os.getenv("DJANGO_SETTINGS_MODULE"):
            os.environ.setdefault("DJANGO_SETTINGS_MODULE", "australia_job_scraper.settings")
        try:
            import django  # type: ignore
            django.setup()
        except Exception as exc:
            logging.error(f"Failed to setup Django: {exc}")
            raise

    def connect(self):
        """Establish database connection based on type."""
        db_type = self.db_type
        
        try:
            if db_type == 'sqlite':
                self.connection = sqlite3.connect(self.config['database'])
                self.connection.row_factory = sqlite3.Row
            elif db_type == 'django':
                # Use Django ORM; no direct DB-API connection is required
                self._setup_django()
                self.connection = True
            elif db_type == 'mysql':
                # Lazy import so environments without mysql don't error
                import mysql.connector  # type: ignore
                self.connection = mysql.connector.connect(
                    host=self.config['host'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database'],
                    port=self.config.get('port', 3306)
                )
            elif db_type == 'postgresql':
                # Lazy import so environments without psycopg2 don't error
                import psycopg2  # type: ignore
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database'],
                    port=self.config.get('port', 5432)
                )
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
                
            logging.info(f"Connected to {db_type} database successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            return False
    
    def fetch_jobs(self, limit: Optional[int] = None, since: Optional[datetime] = None) -> List[Dict]:
        """Fetch job data from database."""
        if not self.connection:
            raise ConnectionError("Database not connected")
        
        # Django ORM branch
        if self.db_type == 'django':
            try:
                from apps.jobs.models import JobPosting  # type: ignore
                # Build queryset
                qs = JobPosting.objects.select_related('company', 'location', 'posted_by')
                if since:
                    qs = qs.filter(updated_at__gte=since)
                qs = qs.order_by('-scraped_at')
                if limit:
                    qs = qs[:int(limit)]

                jobs: List[Dict[str, Any]] = []
                for obj in qs:
                    # Salary text preference
                    try:
                        salary_text = obj.salary_raw_text or obj.salary_display
                    except Exception:
                        salary_text = obj.salary_raw_text or ''

                    # Remote flag derived from work_mode
                    try:
                        remote_allowed = 'remote' in (obj.work_mode or '').lower()
                    except Exception:
                        remote_allowed = False

                    # Helper to ISO-serialize datetimes
                    def iso(dt):
                        try:
                            return dt.isoformat() if dt else None
                        except Exception:
                            return None

                    job_payload: Dict[str, Any] = {
                        # Identifiers
                        'job_id': str(obj.pk),
                        'id': str(obj.pk),
                        'slug': obj.slug,

                        # Basic fields
                        'title': obj.title,
                        'company': getattr(obj.company, 'name', ''),
                        'company_id': obj.company_id,
                        'location': getattr(obj.location, 'name', '') if obj.location_id else '',
                        'location_id': obj.location_id,
                        'description': obj.description or '',

                        # Job details
                        'category': obj.job_category or 'other',
                        'job_type': obj.job_type or 'full_time',
                        'experience_level': obj.experience_level or '',
                        'work_mode': obj.work_mode or '',

                        # Salary details
                        'salary': salary_text,
                        'salary_min': float(obj.salary_min) if obj.salary_min is not None else None,
                        'salary_max': float(obj.salary_max) if obj.salary_max is not None else None,
                        'salary_currency': obj.salary_currency,
                        'salary_type': obj.salary_type,
                        'salary_raw_text': obj.salary_raw_text,
                        'salary_display': None,  # include for completeness; may be None if property fails

                        # External source
                        'source_site': obj.external_source or 'scraper',
                        'application_url': obj.external_url or '',
                        'external_id': obj.external_id or '',

                        # Meta/status
                        'status': obj.status,
                        'posted_ago': obj.posted_ago or '',
                        'posted_date': iso(obj.date_posted or obj.scraped_at),
                        'expired_at': iso(obj.expired_at),
                        'scraped_at': iso(obj.scraped_at),
                        'updated_at': iso(obj.updated_at),
                        'created_at': iso(obj.scraped_at),

                        # Tags/skills
                        'tags': obj.tags or '',
                        'skills': [t.strip() for t in (obj.tags or '').split(',') if t.strip()],

                        # Associations
                        'posted_by_id': obj.posted_by_id,
                        'posted_by': getattr(obj.posted_by, 'username', '') if getattr(obj, 'posted_by_id', None) else '',

                        # Additional
                        'additional_info': obj.additional_info or {},

                        # Derived
                        'remote_allowed': remote_allowed,
                    }

                    # Try to include a user-friendly salary_display if available
                    try:
                        job_payload['salary_display'] = obj.salary_display
                    except Exception:
                        pass

                    jobs.append(job_payload)

                logging.info(f"Fetched {len(jobs)} jobs from Django ORM")
                return jobs
            except Exception as exc:
                logging.error(f"Failed to fetch via Django ORM: {exc}")
                raise

        # Choose DB-API placeholder style
        placeholder = '?' if self.db_type == 'sqlite' else '%s'

        # Build query for direct SQL mode
        if self.db_type in ('postgresql', 'mysql'):
            jp_table = self.table_names.get('jobposting', 'jobs_jobposting')
            company_table = self.table_names.get('company', 'companies_company')
            location_table = self.table_names.get('location', 'core_location')

            query = f"""
            SELECT
                jp.id AS job_id,
                jp.title AS title,
                COALESCE(c.name, '') AS company,
                COALESCE(l.name, '') AS location,
                COALESCE(jp.description, '') AS description,
                COALESCE(jp.salary_raw_text, '') AS salary,
                COALESCE(jp.job_type, 'full_time') AS job_type,
                COALESCE(jp.experience_level, '') AS experience_level,
                COALESCE(jp.tags, '') AS skills,
                COALESCE(jp.date_posted, jp.scraped_at) AS posted_date,
                COALESCE(jp.external_url, '') AS application_url,
                COALESCE(jp.external_source, 'scraper') AS source_site,
                COALESCE(jp.job_category, 'other') AS category,
                (LOWER(COALESCE(jp.work_mode, '')) LIKE '%remote%') AS remote_allowed,
                jp.scraped_at AS created_at,
                jp.updated_at AS updated_at
            FROM {jp_table} jp
            LEFT JOIN {company_table} c ON c.id = jp.company_id
            LEFT JOIN {location_table} l ON l.id = jp.location_id
            WHERE 1=1
            """
            order_field = 'jp.scraped_at'
        else:
            # Generic fallback schema (SQLite default example)
            query = """
            SELECT 
                id as job_id,
                title,
                company,
                location,
                description,
                salary,
                job_type,
                experience_level,
                skills,
                posted_date,
                application_url,
                source_site,
                category,
                remote_allowed,
                created_at,
                updated_at
            FROM jobs 
            WHERE 1=1
            """
            order_field = 'created_at'
        
        params = []
        
        # Add date filter if specified
        if since:
            query += f" AND ({order_field} >= {placeholder} OR updated_at >= {placeholder})"
            params.extend([since, since])
        
        # Add ordering and limit
        query += f" ORDER BY {order_field} DESC"
        if limit:
            query += f" LIMIT {placeholder}"
            params.append(limit)
        
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        
        jobs = []
        for row in cursor.fetchall():
            # Convert row to dict (handling different DB types)
            if isinstance(row, sqlite3.Row):  # sqlite Row
                job_dict = dict(row)
            else:  # mysql/postgresql
                job_dict = dict(zip([desc[0] for desc in cursor.description], row))
            
            # Normalize skills from tags text or JSON string
            if isinstance(job_dict.get('skills'), str):
                raw = job_dict['skills']
                try:
                    parsed = json.loads(raw)
                    job_dict['skills'] = parsed if isinstance(parsed, list) else [str(parsed)]
                except Exception:
                    job_dict['skills'] = [t.strip() for t in raw.split(',') if t.strip()] if raw else []

            # Ensure posted_date is ISO string for JSON serialization
            if isinstance(job_dict.get('posted_date'), datetime):
                job_dict['posted_date'] = job_dict['posted_date'].isoformat()
            
            jobs.append(job_dict)
        
        cursor.close()
        logging.info(f"Fetched {len(jobs)} jobs from database")
        return jobs
    
    def close(self):
        """Close database connection."""
        # For Django ORM mode, there's no DB-API connection to close
        if getattr(self, 'db_type', '').lower() == 'django':
            self.connection = None
            return
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass

class JobPortalAdapter:
    """Base class for job portal adapters."""
    
    def __init__(self, name: str, config: Dict):
        self.name = name
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry strategy."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'JobDataSync/1.0'
        }
        
        if self.config.get('api_key'):
            headers['Authorization'] = f"Bearer {self.config['api_key']}"
        elif self.config.get('auth_token'):
            headers['Authorization'] = f"Token {self.config['auth_token']}"

        # Allow custom headers from config (e.g., X-API-Secret-Key)
        custom_headers = self.config.get('headers')
        if isinstance(custom_headers, dict):
            # Normalize keys to strings and only include simple str/int/bool values
            for key, value in custom_headers.items():
                try:
                    if value is None:
                        continue
                    headers[str(key)] = str(value)
                except Exception:
                    continue

        session.headers.update(headers)
        return session
    
    def transform_job_data(self, job_data: Dict) -> Dict:
        """Transform job data for this portal's format. Override in subclasses."""
        return job_data
    
    def push_job(self, job_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """Push single job to portal. Override in subclasses."""
        raise NotImplementedError
    
    def push_jobs_batch(self, jobs: List[Dict]) -> List[Tuple[bool, Optional[Dict]]]:
        """Push multiple jobs to portal."""
        results = []
        for job in jobs:
            transformed_job = self.transform_job_data(job)
            result = self.push_job(transformed_job)
            results.append(result)
            time.sleep(0.1)  # Small delay between requests
        return results

class EvolJobsAdapter(JobPortalAdapter):
    """Adapter for evoljobs.com portal."""
    
    def transform_job_data(self, job_data: Dict) -> Dict:
        """Transform data for EvolJobs format."""
        return {
            'title': job_data['title'],
            'company_name': job_data['company'],
            'location': job_data['location'],
            'job_description': job_data['description'],
            'salary_range': job_data.get('salary', ''),
            'employment_type': job_data.get('job_type', 'full-time'),
            'experience_required': job_data.get('experience_level', 'entry'),
            'required_skills': job_data.get('skills', []),
            'posted_on': job_data.get('posted_date'),
            'apply_url': job_data.get('application_url', ''),
            'job_category': job_data.get('category', 'other'),
            'is_remote': job_data.get('remote_allowed', False),
            'source': job_data.get('source_site', 'scrapper'),
            'external_id': job_data['job_id']
        }
    
    def push_job(self, job_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """Push job to EvolJobs."""
        try:
            url = f"{self.config['base_url']}"
            response = self.session.post(url, json=job_data, timeout=30)
            response.raise_for_status()
            
            return True, response.json() if response.content else None
        except Exception as e:
            logging.error(f"Failed to push job to EvolJobs: {e}")
            return False, {'error': str(e)}



class FlyoverseasAdapter(JobPortalAdapter):
    """Adapter for flyoverseas.ai portal."""

    def transform_job_data(self, job_data: Dict) -> Dict:
        """Transform data for Flyoverseas format (see README)."""
        return {
            'job_title': job_data.get('title'),
            'employer': job_data.get('company'),
            'job_location': job_data.get('location'),
            'description': job_data.get('description'),
            'compensation': job_data.get('salary', ''),
            'position_type': job_data.get('job_type', 'full-time'),
            'skills_required': job_data.get('skills', []),
            'remote_work': job_data.get('remote_allowed', False),
            'apply_url': job_data.get('application_url', ''),
            'posted_at': job_data.get('posted_date'),
            'source': job_data.get('source_site', 'scraper'),
            'external_id': job_data.get('job_id')
        }

    def push_job(self, job_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """Push job to Flyoverseas endpoint."""
        try:
            base = f"{self.config['base_url']}".rstrip('/')
            endpoint_path = self.config.get('endpoint_path')
            url = f"{base}{endpoint_path}" if endpoint_path else base
            response = self.session.post(url, json=job_data, timeout=30)
            # Accept any 2xx as success
            if 200 <= response.status_code < 300:
                try:
                    return True, response.json() if response.content else {'status_code': response.status_code}
                except Exception:
                    return True, {'status_code': response.status_code, 'text': response.text[:200]}
            else:
                return False, {'status_code': response.status_code, 'text': response.text[:500]}
        except Exception as e:
            logging.error(f"Failed to push job to Flyoverseas: {e}")
            return False, {'error': str(e)}

class LocalPortalAdapter(JobPortalAdapter):
    """Adapter for local LAN job portal."""
    
    def transform_job_data(self, job_data: Dict) -> Dict:
        """Transform data for local portal format."""
        transformed: Dict[str, Any] = dict(job_data)

        company_value: str = (transformed.get('company') or transformed.get('name') or '').strip()
        if not company_value:
            company_value = 'Unknown'
        transformed['company_name'] = company_value

        location_value: str = (transformed.get('location') or '').strip()
        default_country: str = str(self.config.get('default_country') or 'Australia')
        default_location: str = str(self.config.get('default_location') or default_country)
        if not location_value:
            location_value = default_location
        transformed['location'] = location_value

        # Country: enforce max 3 characters (use ISO-like 3-letter code). Allow override via config.
        country_raw: str = str(transformed.get('country') or self.config.get('default_country') or default_country)
        country_clean: str = country_raw.strip().upper()
        # Map common full names to 3-letter codes
        country_map: Dict[str, str] = {
            'AUSTRALIA': 'AUS',
            'UNITED STATES': 'USA',
            'UNITED STATES OF AMERICA': 'USA',
            'UNITED KINGDOM': 'GBR',
            'UK': 'GBR',
            'INDIA': 'IND',
            'CANADA': 'CAN',
            'NEW ZEALAND': 'NZL'
        }
        if len(country_clean) > 3:
            country_clean = country_map.get(country_clean, country_clean[:3])
        transformed['country'] = country_clean

        raw_experience: str = str(transformed.get('experience_level') or '').strip().lower()
        experience_map: Dict[str, str] = {
            'senior': 'senior',
            'sr': 'senior',
            'mid': 'mid',
            'intermediate': 'mid',
            'middle': 'mid',
            'junior': 'entry',
            'entry': 'entry',
            'fresher': 'entry',
        }
        normalized_experience: str = ''
        for key, mapped in experience_map.items():
            if key in raw_experience:
                normalized_experience = mapped
                break
        if not normalized_experience:
            normalized_experience = str(self.config.get('default_experience_level') or 'entry')
        transformed['experience_level'] = normalized_experience

        field_map_cfg: Optional[Dict[str, str]] = self.config.get('field_map') if isinstance(self.config.get('field_map'), dict) else None
        if field_map_cfg:
            for source_field, target_field in field_map_cfg.items():
                try:
                    if source_field in transformed and target_field:
                        transformed[target_field] = transformed[source_field]
                except Exception:
                    continue

        return transformed
    
    def push_job(self, job_data: Dict) -> Tuple[bool, Optional[Dict]]:
        """Push job to local portal."""
        try:
            # Optional file-output mode (no HTTP). Configure portals.local.write_to
            write_to = self.config.get('write_to')
            if write_to:
                try:
                    directory = os.path.dirname(write_to)
                    if directory:
                        os.makedirs(directory, exist_ok=True)
                    with open(write_to, 'a', encoding='utf-8') as f:
                        json.dump(job_data, f, ensure_ascii=False)
                        f.write('\n')
                    return True, {'written_to': write_to}
                except Exception as exc:
                    logging.error(f"File write failed: {exc}")
                    return False, {'error': str(exc)}

            # Optional dry-run flag (log only, no HTTP)
            if self.config.get('dry_run'):
                try:
                    logging.info(f"[DRY RUN] Local payload: {json.dumps(job_data)[:1000]}")
                except Exception:
                    logging.info("[DRY RUN] Local payload logged")
                return True, {'dry_run': True}

            # Default: send to HTTP endpoint
            base = f"{self.config['base_url']}".rstrip('/')
            endpoint_path = self.config.get('endpoint_path')
            url = f"{base}{endpoint_path}" if endpoint_path else base
            response = self.session.post(url, json=job_data, timeout=30)
            # Treat any 2xx as success regardless of response body format
            if 200 <= response.status_code < 300:
                resp_payload: Optional[Dict] = None
                if response.content:
                    try:
                        parsed = response.json()
                        if isinstance(parsed, dict):
                            resp_payload = parsed
                        else:
                            resp_payload = {'response': parsed}
                    except Exception:
                        # Non-JSON body; keep a short text preview
                        text_preview = response.text[:200]
                        resp_payload = {'status_code': response.status_code, 'text': text_preview}
                else:
                    resp_payload = {'status_code': response.status_code}
                return True, resp_payload
            else:
                text_preview = response.text[:500]
                logging.error(f"Local portal returned {response.status_code}: {text_preview}")
                return False, {'status_code': response.status_code, 'text': text_preview}
        except Exception as e:
            logging.error(f"Failed to push job to Local Portal: {e}")
            return False, {'error': str(e)}

class JobDataSynchronizer:
    """Main class to orchestrate job data synchronization."""
    
    def __init__(self, config_file: Optional[str] = None):
        self.setup_logging()
        self.load_config(config_file)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.portals = self._initialize_portals()
        
    def _ensure_iso_z(self, value: Any) -> str:
        """Return an ISO 8601 string with 'Z' suffix when timezone info is missing."""
        try:
            if isinstance(value, datetime):
                iso_str = value.isoformat()
            else:
                iso_str = str(value) if value is not None else ''
            # If already has timezone offset or ends with Z, keep as-is
            if iso_str.endswith('Z') or ('+' in iso_str and not iso_str.endswith('+')):
                return iso_str
            # If looks like ISO without timezone, append Z
            if iso_str and iso_str[4] == '-' and 'T' in iso_str and 'Z' not in iso_str and '+' not in iso_str:
                return iso_str + 'Z'
            return iso_str
        except Exception:
            # Fallback to now
            return datetime.utcnow().isoformat() + 'Z'

    def _pick_avatar(self, seed: str) -> str:
        """Pick a deterministic placeholder avatar URL based on a seed string."""
        try:
            seed_text = seed or 'default'
            h = hashlib.md5(seed_text.encode('utf-8')).hexdigest()
            idx = int(h, 16) % 20 + 1  # 1..20
            return f"https://cdn.jsdelivr.net/gh/faker-js/assets-person-portrait/male/512/{idx}.jpg"
        except Exception:
            return "https://cdn.jsdelivr.net/gh/faker-js/assets-person-portrait/male/512/1.jpg"

    def _normalize_job_payload(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure required fields exist with sensible defaults for downstream portals."""
        normalized: Dict[str, Any] = dict(job)  # shallow copy

        # Ensure stable id
        job_id_value = normalized.get('job_id') or normalized.get('id') or ''
        normalized['id'] = str(job_id_value) if job_id_value is not None else ''

        # createdAt preference: created_at/scraped_at/posted_date -> now
        created_src = (
            normalized.get('createdAt')
            or normalized.get('created_at')
            or normalized.get('scraped_at')
            or normalized.get('posted_date')
            or None
        )
        if created_src is None:
            normalized['createdAt'] = datetime.utcnow().isoformat() + 'Z'
        else:
            normalized['createdAt'] = self._ensure_iso_z(created_src)

        # Ensure posted_date
        if normalized.get('posted_date'):
            normalized['posted_date'] = self._ensure_iso_z(normalized['posted_date'])
        else:
            normalized['posted_date'] = normalized['createdAt']

        # Ensure textual fields exist
        for key in ['title', 'company', 'location', 'description']:
            if normalized.get(key) is None:
                normalized[key] = ''

        # Friendly name (fallback to company)
        if not normalized.get('name'):
            normalized['name'] = normalized.get('company') or 'Unknown'

        # Avatar placeholder
        if not normalized.get('avatar'):
            normalized['avatar'] = self._pick_avatar(normalized.get('id', ''))

        # Salary default
        if normalized.get('salary') is None:
            normalized['salary'] = ''

        # Job type default
        if not normalized.get('job_type'):
            normalized['job_type'] = 'full_time'

        # Experience default
        if normalized.get('experience_level') is None:
            normalized['experience_level'] = ''

        # Skills normalization
        skills_value = normalized.get('skills')
        if isinstance(skills_value, list):
            normalized['skills'] = skills_value
        elif isinstance(skills_value, str):
            try:
                parsed = json.loads(skills_value)
                normalized['skills'] = parsed if isinstance(parsed, list) else [str(parsed)]
            except Exception:
                normalized['skills'] = [t.strip() for t in skills_value.split(',') if t.strip()] if skills_value else []
        else:
            normalized['skills'] = []

        # URL defaults
        if not normalized.get('application_url'):
            normalized['application_url'] = ''

        if not normalized.get('source_site'):
            normalized['source_site'] = 'scraper'

        if not normalized.get('category'):
            normalized['category'] = 'other'

        # Remote flag default
        normalized['remote_allowed'] = bool(normalized.get('remote_allowed', False))

        return normalized

    def setup_logging(self):
        """Setup logging configuration."""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('job_sync.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_config(self, config_file: Optional[str] = None):
        """Load configuration from file or environment."""
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        else:
            # Default configuration from environment variables
            self.config = {
                'database': {
                    'type': os.getenv('DB_TYPE', 'sqlite'),
                    'database': os.getenv('DB_NAME', 'jobs.db'),
                    'host': os.getenv('DB_HOST', 'localhost'),
                    'user': os.getenv('DB_USER', ''),
                    'password': os.getenv('DB_PASSWORD', ''),
                    'port': int(os.getenv('DB_PORT', '5432'))
                },
                'portals': {
                    'local': {
                        'enabled': os.getenv('LOCAL_ENABLED', 'true').lower() == 'true',
                        'base_url': os.getenv('LOCAL_URL', 'http://127.0.0.1:8002'),
                        'api_key': os.getenv('LOCAL_API_KEY', '')
                    }
                },
                'sync': {
                    'batch_size': int(os.getenv('BATCH_SIZE', '50')),
                    'incremental': os.getenv('INCREMENTAL_SYNC', 'true').lower() == 'true',
                    'sync_interval_minutes': int(os.getenv('SYNC_INTERVAL', '60'))
                }
            }
    
    def _initialize_portals(self) -> Dict[str, JobPortalAdapter]:
        """Initialize portal adapters from config, supporting multiple entries.

        Each entry under `portals` may specify a `type` to choose adapter:
        - "http" or "local": LocalPortalAdapter (generic HTTP/file writer)
        - "flyoverseas": FlyoverseasAdapter
        - "evoljobs": EvolJobsAdapter

        If `type` is omitted, the entry name is used to infer the adapter
        (backward compatible with existing 'local' and 'flyoverseas' keys).
        """
        portals: Dict[str, JobPortalAdapter] = {}

        portal_configs = self.config.get('portals', {}) or {}

        for portal_name, portal_cfg in portal_configs.items():
            # Skip non-dict entries or disabled portals
            if not isinstance(portal_cfg, dict) or not portal_cfg.get('enabled'):
                continue

            portal_type = (portal_cfg.get('type') or portal_name).strip().lower()

            adapter: Optional[JobPortalAdapter] = None
            if portal_type in ('http', 'local'):
                adapter = LocalPortalAdapter(portal_name, portal_cfg)
            else:
                self.logger.warning(f"Unknown portal type '{portal_type}' for '{portal_name}', skipping")
                continue

            portals[portal_name] = adapter

        self.logger.info(f"Initialized {len(portals)} portal adapters: {list(portals.keys())}")
        return portals
    
    def sync_jobs(self, limit: Optional[int] = None, incremental: bool = True) -> Dict:
        """Synchronize jobs from database to all portals."""
        start_time = datetime.now()
        
        try:
            # Connect to database
            if not self.db_connector.connect():
                raise ConnectionError("Failed to connect to database")
            
            # Determine sync period for incremental sync
            since = None
            if incremental:
                sync_interval = timedelta(minutes=self.config['sync']['sync_interval_minutes'])
                since = now_dt - sync_interval
                self.logger.info(f"Performing incremental sync since {since}")
            
            # Fetch jobs from database
            jobs = self.db_connector.fetch_jobs(limit=limit, since=since)
            
            if not jobs:
                self.logger.info("No new jobs to sync")
                return {'status': 'success', 'jobs_synced': 0, 'portals': {}}
            
            # Sync to each portal
            portal_results = {}
            total_synced = 0
            
            for portal_name, portal_adapter in self.portals.items():
                self.logger.info(f"Syncing {len(jobs)} jobs to {portal_name}")
                
                # Process in batches
                batch_size = self.config['sync']['batch_size']
                portal_success = 0
                portal_failed = 0
                
                for i in range(0, len(jobs), batch_size):
                    batch = jobs[i:i + batch_size]
                    # Normalize each job to guarantee required fields
                    normalized_batch = [self._normalize_job_payload(j) for j in batch]
                    batch_results = portal_adapter.push_jobs_batch(normalized_batch)
                    
                    # Count successes and failures
                    for success, _ in batch_results:
                        if success:
                            portal_success += 1
                        else:
                            portal_failed += 1
                    
                    # Small delay between batches
                    time.sleep(1)
                
                portal_results[portal_name] = {
                    'success': portal_success,
                    'failed': portal_failed,
                    'success_rate': portal_success / len(jobs) if jobs else 0
                }
                
                total_synced += portal_success
                self.logger.info(f"{portal_name}: {portal_success} success, {portal_failed} failed")
            
            # Summary
            summary = {
                'status': 'success',
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - start_time).total_seconds(),
                'jobs_fetched': len(jobs),
                'total_synced': total_synced,
                'portals': portal_results
            }
            
            self.logger.info(f"Sync completed - {len(jobs)} jobs fetched, {total_synced} total synced")
            return summary
            
        except Exception as e:
            self.logger.error(f"Sync failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            }
        finally:
            self.db_connector.close()

def main():
    """Main function to run the synchronizer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Synchronize job data from scrapper to portals')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--limit', type=int, help='Limit number of jobs to sync')
    parser.add_argument('--full', action='store_true', help='Full sync (not incremental)')
    parser.add_argument('--output', help='Save results to JSON file')
    
    args = parser.parse_args()
    
    # Create synchronizer
    sync = JobDataSynchronizer(config_file=args.config)
    
    # Run synchronization
    incremental = not args.full
    results = sync.sync_jobs(limit=args.limit, incremental=incremental)
    
    # Save results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")
    
    # Print summary
    if results['status'] == 'success':
        print("\n" + "="*60)
        print("JOB SYNCHRONIZATION SUMMARY")
        print("="*60)
        print(f"Jobs Fetched: {results['jobs_fetched']}")
        print(f"Total Synced: {results['total_synced']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print("\nPortal Results:")
        for portal, stats in results['portals'].items():
            print(f"  {portal}: {stats['success']} success, {stats['failed']} failed ({stats['success_rate']:.1%})")
    else:
        print(f"Sync failed: {results['error']}")

if __name__ == "__main__":
    main()
