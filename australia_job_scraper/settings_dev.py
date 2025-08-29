# settings_dev.py
from .settings import *  # keep base defaults, then override below
import os

# -----------------------------------------------------------------------------
# Debug / Hosts / CSRF / CORS
# -----------------------------------------------------------------------------
DEBUG = os.getenv("DEBUG", "1") in ["1", "true", "True"]

# Example: ALLOWED_HOSTS=localhost,127.0.0.1,192.168.0.49
ALLOWED_HOSTS = [h.strip() for h in os.getenv("ALLOWED_HOSTS", "localhost,127.0.0.1").split(",") if h.strip()]

# Example: CSRF_TRUSTED_ORIGINS=http://192.168.0.49:8001,http://localhost:8001
CSRF_TRUSTED_ORIGINS = [o.strip() for o in os.getenv("CSRF_TRUSTED_ORIGINS", "").split(",") if o.strip()]

# CORS configuration is inherited from base settings.py

# -----------------------------------------------------------------------------
# Database (shared PostgreSQL - Docker or local access)
# -----------------------------------------------------------------------------
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("DB_NAME", "australia_job_scraper"),
        "USER": os.getenv("DB_USER", "postgres"),
        "PASSWORD": os.getenv("DB_PASSWORD", "Evolgroup@123"),
        "HOST": os.getenv("DB_HOST", "localhost"),  # localhost for both Docker and local
        "PORT": os.getenv("DB_PORT", "5433"),  # Use 5433 for local, 5432 inside Docker
    }
}

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
