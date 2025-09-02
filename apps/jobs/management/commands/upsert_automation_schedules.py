import json
from typing import List

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone as dj_timezone

from django_celery_beat.models import CrontabSchedule, PeriodicTask

from apps.jobs.models import JobScheduler


class Command(BaseCommand):
    help = "Upsert django-celery-beat schedules for JobScheduler entries in IST (Asia/Kolkata)"

    # [cursor:reason] Ensure all schedules use IST by default; allow override via settings/env
    target_tz: str = getattr(settings, "TIME_ZONE", "Asia/Kolkata") or "Asia/Kolkata"

    def handle(self, *args, **options):
        created: List[int] = []
        updated: List[int] = []
        normalized: int = 0

        # Activate timezone context for consistency
        dj_timezone.activate(self.target_tz)

        for sched in JobScheduler.objects.select_related("script").all():
            # Compute desired crontab kwargs and force timezone to target_tz
            kwargs = sched.compute_cron_kwargs()
            kwargs["timezone"] = self.target_tz  # [cursor:reason] Force IST for beat execution timing

            crontab, _ = CrontabSchedule.objects.get_or_create(**kwargs)

            task_name = f"execute_{sched.script.name}_{sched.id}"
            task_kwargs = json.dumps({"scheduler_id": sched.id})

            if sched.periodic_task_id:
                # Update existing PeriodicTask
                PeriodicTask.objects.filter(id=sched.periodic_task_id).update(
                    name=task_name,
                    task="jobs.execute_script",
                    crontab=crontab,
                    kwargs=task_kwargs,
                    enabled=sched.enabled,
                )
                updated.append(sched.id)
            else:
                pt = PeriodicTask.objects.create(
                    name=task_name,
                    task="jobs.execute_script",
                    crontab=crontab,
                    kwargs=task_kwargs,
                    enabled=sched.enabled,
                )
                JobScheduler.objects.filter(pk=sched.pk).update(periodic_task=pt)
                created.append(sched.id)

            # Normalize any pre-existing crontab timezone mismatches
            if sched.crontab_id and sched.crontab.timezone != self.target_tz:
                CrontabSchedule.objects.filter(id=sched.crontab_id).update(timezone=self.target_tz)
                normalized += 1

        self.stdout.write(self.style.SUCCESS(
            f"Schedules upserted. created={len(created)} updated={len(updated)} normalized_tz={normalized} tz={self.target_tz}"
        ))
        if created:
            self.stdout.write(f"Created for JobScheduler IDs: {created}")
        if updated:
            self.stdout.write(f"Updated for JobScheduler IDs: {updated}")


