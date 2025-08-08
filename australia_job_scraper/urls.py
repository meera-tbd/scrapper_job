"""
URL configuration for australia_job_scraper project.
"""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('apps.jobs.urls')),
]