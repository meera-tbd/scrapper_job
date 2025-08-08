"""
ASGI config for australia_job_scraper project.
"""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'australia_job_scraper.settings')

application = get_asgi_application()