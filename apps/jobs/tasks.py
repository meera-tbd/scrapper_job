import importlib
import logging
from typing import Callable

from celery import shared_task
from django.utils import timezone
from asgiref.sync import async_to_sync, sync_to_async

from .models import JobScheduler

logger = logging.getLogger(__name__)


def _import_callable(path: str) -> Callable:
    if ':' in path:
        module_path, attr = path.split(':', 1)
    else:
        # Support dotted path to a callable named `run`
        module_path, attr = path, 'run'
    module = importlib.import_module(module_path)
    func = getattr(module, attr)
    if not callable(func):
        raise TypeError(f"Target {path} is not callable")
    return func


def _load_scheduler_data(scheduler_id: int):
    """Fetch scheduler-related data in a sync context.

    Returns a dict with required fields or None if missing.
    """
    try:
        scheduler = JobScheduler.objects.select_related('script').get(id=scheduler_id)
    except JobScheduler.DoesNotExist:
        return None
    return {
        'enabled': scheduler.enabled,
        'script_is_active': scheduler.script.is_active,
        'module_path': scheduler.script.module_path,
    }


def _update_last_run_timestamp(scheduler_id: int):
    """Update last_run_at for the given scheduler in a sync context."""
    JobScheduler.objects.filter(id=scheduler_id).update(last_run_at=timezone.now())


@shared_task(bind=True, name='jobs.execute_script')
def execute_script(self, scheduler_id: int) -> dict:
    """Execute the configured scraper callable and record run metadata."""
    data = async_to_sync(sync_to_async(_load_scheduler_data, thread_sensitive=True))(scheduler_id)
    if data is None:
        logger.warning("Scheduler %s no longer exists; skipping", scheduler_id)
        return {'skipped': True, 'reason': 'scheduler_missing'}
    if not data['enabled'] or not data['script_is_active']:
        logger.info("Scheduler %s disabled or script inactive; skipping", scheduler_id)
        return {'skipped': True}

    target_path = data['module_path']
    try:
        func = _import_callable(target_path)
        logger.info("Executing scraper: %s", target_path)
        result = func()  # Expect the callable to do its work and return dict/summary
        async_to_sync(sync_to_async(_update_last_run_timestamp, thread_sensitive=True))(scheduler_id)
        return {'ok': True, 'result': result}
    except Exception as exc:
        logger.exception("Error executing scraper %s", target_path)
        return {'ok': False, 'error': str(exc)}


