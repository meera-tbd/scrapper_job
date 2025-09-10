# settings_dev.py
from .settings import *  # keep base defaults, then override below
import os

# Database configuration: Same PostgreSQL for both Docker and Local
if os.getenv('DB_HOST'):
    # Docker environment - connects to local PostgreSQL via host.docker.internal
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.getenv('DB_NAME', 'australia_job_scraper'),
            'USER': os.getenv('DB_USER', 'postgres'),
            'PASSWORD': os.getenv('DB_PASSWORD', 'Evolgroup@123'),
            'HOST': os.getenv('DB_HOST', 'localhost'),
            'PORT': os.getenv('DB_PORT', '5432'),
        }
    }
else:
    # Local environment - connects directly to local PostgreSQL
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'australia_job_scraper',
            'USER': 'postgres',
            'PASSWORD': 'Evolgroup@123',
            'HOST': 'localhost',
            'PORT': '5432',
        }
    }

DEBUG = True
# Allow all hosts in development to avoid DisallowedHost when accessing via LAN IP
ALLOWED_HOSTS = ['*']

# -----------------------------------------------------------------------------
# Dev security (relaxed)
# -----------------------------------------------------------------------------
SECURE_SSL_REDIRECT = False
SECURE_BROWSER_XSS_FILTER = False
SECURE_CONTENT_TYPE_NOSNIFF = False
X_FRAME_OPTIONS = "SAMEORIGIN"

# Session and CSRF settings for network access
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "False").lower() in ["true", "1"]
CSRF_COOKIE_SECURE = os.getenv("CSRF_COOKIE_SECURE", "False").lower() in ["true", "1"]
SESSION_COOKIE_HTTPONLY = True
CSRF_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'  # Allow cross-origin requests for network access
CSRF_COOKIE_SAMESITE = 'Lax'

# Static files (use same as production for consistency)
STATIC_ROOT = os.getenv("STATIC_ROOT", str(BASE_DIR / "staticfiles"))
