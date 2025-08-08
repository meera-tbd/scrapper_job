from .settings import *
PG_USER = 'postgres'
# PG_PASSWORD = 'Technobits@123'    // server password :==== Evolgroup@123
# new_db_name = technobits_latest
PG_HOST = 'localhost'
PG_PORT = '5432'
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'australia_job_scraper',
        'USER': PG_USER,
        'PASSWORD': 'Evolgroup@123',
        'HOST': PG_HOST,
        'PORT': PG_PORT,
    }
}
DEBUG = True
ALLOWED_HOSTS = ['*']