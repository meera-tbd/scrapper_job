import os
import platform

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings_dev')

app = Celery('australia_job_scraper')

app.config_from_object('django.conf:settings', namespace='CELERY')

# Use Django's app discovery
app.autodiscover_tasks()

# [cursor:reason] Ensure Celery and process environment use IST (Asia/Kolkata) without altering brokers/backends
os.environ.setdefault('TZ', 'Asia/Kolkata')
try:
    import time as _time
    _time.tzset()
except Exception:
    # tzset may be unavailable on some platforms; safe to ignore in containers
    pass

app.conf.timezone = os.getenv('CELERY_TIMEZONE', 'Asia/Kolkata')
app.conf.enable_utc = False  # [cursor:reason] Treat schedules as local time (IST)

# Ensure a Windows-safe worker pool to avoid prefork issues on Windows
if platform.system() == 'Windows':
    # "solo" avoids multiprocessing/fork problems on Windows
    app.conf.worker_pool = 'solo'
    app.conf.worker_concurrency = 1

# Alternative: force load tasks after Django is ready
@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """Ensure tasks are loaded after configuration."""
    try:
        import django
        django.setup()
        from apps.jobs import tasks  # Force task registration
    except Exception:
        pass


@app.task(bind=True)
def debug_task(self):
    return {'request': str(self.request)}


