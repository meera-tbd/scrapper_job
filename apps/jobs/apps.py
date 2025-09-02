from django.apps import AppConfig
import logging
from datetime import time


logger = logging.getLogger(__name__)


class JobsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.jobs'
    def ready(self):
        from . import signals  # noqa: F401
        # Auto-register and schedule the expiration job (idempotent)
        try:
            from .models import JobScript, JobScheduler

            # Upsert JobScript for expiration
            script_name = 'Expire Jobs'
            module_path = 'script.expire_jobs:run'
            script, _ = JobScript.objects.get_or_create(
                name=script_name,
                defaults={
                    'module_path': module_path,
                    'description': 'Automatically expire closed jobs and cleanup old records',
                    'is_active': True,
                },
            )
            if script.module_path != module_path or not script.is_active:
                script.module_path = module_path
                script.is_active = True
                script.save(update_fields=['module_path', 'is_active', 'updated_at'])

            # Ensure an hourly scheduler exists and enabled
            # Runs at minute 15 of every hour
            default_time = time(hour=0, minute=15)
            scheduler, created = JobScheduler.objects.get_or_create(
                script=script,
                frequency='hourly',
                defaults={
                    'time_of_day': default_time,
                    'enabled': True,
                },
            )
            # If it existed but was disabled or time not set, fix it
            updates = []
            if not scheduler.enabled:
                scheduler.enabled = True
                updates.append('enabled')
            if not scheduler.time_of_day:
                scheduler.time_of_day = default_time
                updates.append('time_of_day')
            if updates:
                scheduler.save(update_fields=updates + ['updated_at'])
        except Exception as e:
            # During migrations or early startup, tables may not exist yet; ignore quietly
            logger.debug("JobsConfig.ready() scheduling skipped: %s", e)