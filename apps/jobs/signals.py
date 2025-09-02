import json
from django.db.models.signals import post_save, post_delete, pre_save
from django.dispatch import receiver
from django_celery_beat.models import CrontabSchedule, PeriodicTask

from .models import JobScheduler


def _ensure_crontab(schedule: JobScheduler) -> CrontabSchedule:
    kwargs = schedule.compute_cron_kwargs()
    crontab, _ = CrontabSchedule.objects.get_or_create(**kwargs)
    return crontab


@receiver(pre_save, sender=JobScheduler)
def ensure_crontab_on_save(sender, instance: JobScheduler, **kwargs):
    """Assign the crontab before saving to avoid recursive post_save updates."""
    crontab = _ensure_crontab(instance)
    instance.crontab = crontab


@receiver(post_save, sender=JobScheduler)
def upsert_periodic_task(sender, instance: JobScheduler, created, **kwargs):
    crontab = instance.crontab or _ensure_crontab(instance)

    task_name = f"execute_{instance.script.name}_{instance.id}"
    task_kwargs = json.dumps({'scheduler_id': instance.id})

    if instance.periodic_task_id:
        # Update existing task
        PeriodicTask.objects.filter(id=instance.periodic_task_id).update(
            name=task_name,
            task='jobs.execute_script',
            crontab=crontab,
            kwargs=task_kwargs,
            enabled=instance.enabled,
        )
    else:
        pt = PeriodicTask.objects.create(
            name=task_name,
            task='jobs.execute_script',
            crontab=crontab,
            kwargs=task_kwargs,
            enabled=instance.enabled,
        )
        # Avoid recursion by updating via queryset (no signals fired)
        JobScheduler.objects.filter(pk=instance.pk).update(periodic_task=pt)


@receiver(post_delete, sender=JobScheduler)
def delete_periodic_task(sender, instance: JobScheduler, **kwargs):
    if instance.periodic_task_id:
        PeriodicTask.objects.filter(id=instance.periodic_task_id).delete()


