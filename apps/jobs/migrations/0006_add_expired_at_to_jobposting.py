from django.db import migrations, models
from django.utils import timezone


def backfill_expired_at(apps, schema_editor):
    JobPosting = apps.get_model('jobs', 'JobPosting')
    # Backfill: set expired_at to updated_at if status is expired and expired_at is null
    for job in JobPosting.objects.filter(status='expired', expired_at__isnull=True):
        # Use updated_at if available; else fallback to now to avoid nulls
        job.expired_at = job.updated_at or timezone.now()
        job.save(update_fields=['expired_at'])


class Migration(migrations.Migration):

    dependencies = [
        ('jobs', '0005_add_hourly_minute_frequency'),
    ]

    operations = [
        migrations.AddField(
            model_name='jobposting',
            name='expired_at',
            field=models.DateTimeField(blank=True, null=True, help_text='When the job was marked expired'),
        ),
        migrations.RunPython(backfill_expired_at, migrations.RunPython.noop),
    ]


