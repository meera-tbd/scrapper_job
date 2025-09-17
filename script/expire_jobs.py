import concurrent.futures
import logging
from datetime import datetime, time, timedelta

import requests
from dateutil import parser as date_parser
from django.db.models import Q
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


def _requests_session(timeout_seconds: int = 5) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.timeout = timeout_seconds
    return session


def _url_is_404(session: requests.Session, url: str) -> bool:
    """Return True only when the URL clearly returns HTTP 404/410 (missing/gone).

    We do not parse page content; only status codes matter for this rule.
    """
    try:
        resp = session.head(url, allow_redirects=True, timeout=5)
        if resp.status_code in (404, 410):
            return True
        # Fallback to GET for sites not supporting HEAD correctly
        resp = session.get(url, allow_redirects=True, timeout=5)
        if resp.status_code in (404, 410):
            return True
    except requests.RequestException:
        # Network or TLS error: treat as inconclusive
        return False
    return False


def _parse_closing_date(raw_value: str) -> datetime | None:
    """Parse the `job_closing_date` string into a timezone-aware datetime.

    - Accepts many formats via dateutil.parser
    - If time is missing, assume end-of-day (23:59:59)
    - Returns None when parsing fails
    """
    if not raw_value:
        return None
    try:
        dt = date_parser.parse(raw_value, dayfirst=True, fuzzy=True)
    except (ValueError, TypeError):
        return None

    # If parsed value has no time (midnight), interpret as end-of-day
    if dt.time() == time(0, 0):
        dt = datetime.combine(dt.date(), time(23, 59, 59))

    # Make timezone-aware if naive
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _set_job_status(job: JobPosting, new_status: str) -> None:
    now = timezone.now()
    job.status = new_status
    if new_status == "expired" and not getattr(job, "expired_at", None):
        job.expired_at = now
        job.updated_at = now
        job.save(update_fields=["status", "expired_at", "updated_at"])
    else:
        job.updated_at = now
        job.save(update_fields=["status", "updated_at"])


def run(batch_size: int = 1000, retention_days: int = 90, parallelism: int = 16):
    """
    Update job statuses based on closing date and external URL 404 checks.

    Rules:
    - If `job_closing_date` is present and in the past -> status "expired"
    - If no `job_closing_date` and external URL returns 404/410 -> status "inactive"
    - Otherwise -> status "active" (does not revive already expired/filled jobs)

    Returns a summary dict.
    """
    now = timezone.now()
    session = _requests_session()

    # 1) Closing-date based updates
    expired_by_closing_date = 0
    set_active_by_closing_date = 0
    closing_qs = (
        JobPosting.objects
        .filter(job_closing_date__isnull=False)
        .exclude(job_closing_date__exact="")
        .only("id", "job_closing_date", "status", "expired_at", "updated_at")
        [:batch_size]
    )
    for job in closing_qs:
        closing_dt = _parse_closing_date(job.job_closing_date)
        if closing_dt and closing_dt <= now:
            # Do not override 'filled' jobs
            if job.status not in ("expired", "filled"):
                _set_job_status(job, "expired")
                expired_by_closing_date += 1
        else:
            # Not past closing date -> keep active unless it is already expired/filled
            if job.status not in ("active", "expired", "filled"):
                _set_job_status(job, "active")
                set_active_by_closing_date += 1

    # 2) For jobs without a closing date: URL 404/410 -> inactive, else active
    inactive_by_404 = 0
    set_active_by_url = 0

    no_date_q = Q(job_closing_date__isnull=True) | Q(job_closing_date__exact="")
    status_q = Q(status__in=["active", "inactive"])  # do not touch expired/filled
    url_qs = (
        JobPosting.objects
        .filter(no_date_q & status_q)
        .only("id", "external_url", "status", "updated_at")
        [:batch_size]
    )

    id_to_url = {j.id: j.external_url for j in url_qs}
    if id_to_url:
        def check(job_id: int) -> tuple[int, bool]:
            return job_id, _url_is_404(session, id_to_url[job_id])

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as pool:
            for job_id, is_404 in pool.map(check, list(id_to_url.keys())):
                try:
                    job = JobPosting.objects.get(id=job_id)
                except JobPosting.DoesNotExist:
                    continue

                if is_404:
                    if job.status != "inactive":
                        _set_job_status(job, "inactive")
                        inactive_by_404 += 1
                else:
                    if job.status != "active":
                        _set_job_status(job, "active")
                        set_active_by_url += 1

    # 3) Optional cleanup: delete very old expired rows
    # Keep this behavior to prevent DB bloat; can be disabled by setting a large retention
    deleted = 0
    expired_cutoff = now - timedelta(days=retention_days)
    old_expired = JobPosting.objects.filter(status="expired", updated_at__lt=expired_cutoff)
    deleted = old_expired.count()
    if deleted:
        old_expired.delete()

    summary = {
        "expired_by_closing_date": expired_by_closing_date,
        "set_active_by_closing_date": set_active_by_closing_date,
        "inactive_by_404": inactive_by_404,
        "set_active_by_url": set_active_by_url,
        "deleted": deleted,
        "at": now.isoformat(),
    }
    logger.info("expire_jobs summary: %s", summary)
    return summary


