from .settings import *
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
            'HOST': os.getenv('DB_HOST', 'host.docker.internal'),
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
ALLOWED_HOSTS = ['localhost', '127.0.0.1', 'localhost:8000', 'localhost:8001', '127.0.0.1:8000', '127.0.0.1:8001']