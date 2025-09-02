import concurrent.futures
import logging
import re
from datetime import timedelta

import requests
from django.db import transaction
from django.utils import timezone

from apps.jobs.models import JobPosting


logger = logging.getLogger(__name__)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


# Per-source TTL fallback (in days)
TTL_BY_SOURCE = {
    "seek.com.au": 30,
    "jora": 30,
    "jooble": 30,
    "workday": 60,
    "greenhouse": 60,
    "default": 45,
}


# Phrases that often indicate a closed/expired job page
CLOSED_PATTERNS = [
    re.compile(r"job (no longer|is no longer) (available|active|open)", re.I),
    re.compile(r"this (position|job) (has )?closed", re.I),
    re.compile(r"we are no longer accepting applications", re.I),
    re.compile(r"was not found|page not found", re.I),
]


def _requests_session(timeout_seconds: int = 5) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.timeout = timeout_seconds
    return session


def _url_indicates_closed(session: requests.Session, url: str) -> bool:
    try:
        # Try HEAD first
        resp = session.head(url, allow_redirects=True, timeout=5)
        status = resp.status_code
        if status in (404, 410):
            return True
        if status in (301, 302, 303, 307, 308):
            # If redirected, a GET on the final URL may reveal a closure message
            pass

        # Fallback to GET when inconclusive
        resp = session.get(url, allow_redirects=True, timeout=5)
        status = resp.status_code
        if status in (404, 410):
            return True
        if status == 200:
            html = resp.text
            for pat in CLOSED_PATTERNS:
                if pat.search(html):
                    return True
        # 5xx should be treated as transient
        return False
    except requests.RequestException:
        # Network error: do not expire based solely on error
        return False


def _ttl_expired(job: JobPosting, now) -> bool:
    posted = job.date_posted or job.scraped_at or job.updated_at
    ttl_days = TTL_BY_SOURCE.get(job.external_source, TTL_BY_SOURCE["default"])
    if not posted:
        return False
    return posted < now - timedelta(days=ttl_days)


def _expire_job(job: JobPosting) -> None:
    now = timezone.now()
    job.status = "expired"
    # Set expired_at only once; keep original if already set
    if not getattr(job, "expired_at", None):
        job.expired_at = now
    job.updated_at = now
    job.save(update_fields=["status", "expired_at", "updated_at"])


def run(batch_size: int = 1000, retention_days: int = 90, parallelism: int = 16):
    """
    Expire outdated jobs automatically and cleanup long-expired rows.

    Returns a summary dict.
    """
    now = timezone.now()
    session = _requests_session()

    # 1) URL-first checks for older active jobs (skip very fresh to reduce noise)
    url_check_cutoff = now - timedelta(days=3)

    candidates = (
        JobPosting.objects.filter(status="active", scraped_at__lt=url_check_cutoff)
        .order_by("scraped_at")
        .values("id", "external_url")[:batch_size]
    )

    url_check_ids = [c["id"] for c in candidates]
    url_map = {c["id"]: c["external_url"] for c in candidates}

    expired_by_url = 0
    if url_check_ids:
        def check_one(job_id: int) -> tuple[int, bool]:
            url = url_map[job_id]
            is_closed = _url_indicates_closed(session, url)
            return job_id, is_closed

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as pool:
            for job_id, is_closed in pool.map(check_one, url_check_ids):
                if is_closed:
                    try:
                        job = JobPosting.objects.get(id=job_id)
                        if job.status == "active":
                            _expire_job(job)
                            expired_by_url += 1
                    except JobPosting.DoesNotExist:
                        pass

    # 2) Age-based TTL fallback for remaining active jobs
    expired_by_age = 0
    remaining = JobPosting.objects.filter(status="active").only(
        "id", "external_source", "date_posted", "scraped_at", "updated_at"
    )
    for job in remaining:
        if _ttl_expired(job, now):
            _expire_job(job)
            expired_by_age += 1

    # 3) Cleanup: delete expired older than retention window
    deleted = 0
    with transaction.atomic():
        qs = JobPosting.objects.filter(
            status="expired",
            updated_at__lt=now - timedelta(days=retention_days),
        )
        deleted = qs.count()
        if deleted:
            qs.delete()

    summary = {
        "checked_url": len(url_check_ids),
        "expired_by_url": expired_by_url,
        "expired_by_age": expired_by_age,
        "deleted": deleted,
        "at": now.isoformat(),
    }
    logger.info("expire_jobs summary: %s", summary)
    return summary


