"""
Main API URL configuration for the job scraper project.
"""

from django.urls import path, include
from rest_framework import permissions
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.reverse import reverse


@api_view(['GET'])
def api_root(request, format=None):
    """
    API root endpoint that provides links to the main API endpoints.
    """
    jobs_url = reverse('job-list', request=request, format=format)
    
    return Response({
        'jobs': jobs_url,
        'companies': reverse('company-list', request=request, format=format),
        'locations': reverse('location-list', request=request, format=format),
        'external_sources': reverse('job-external-sources', request=request, format=format),
        'jobs_from_seek': f"{jobs_url}?external_source=seek.com.au",
    })


urlpatterns = [
    # API root
    path('', api_root, name='api-root'),
    
    # App-specific API endpoints
    path('', include('apps.jobs.api_urls')),
    path('', include('apps.companies.api_urls')),
    path('', include('apps.core.api_urls')),
    
    # DRF browsable API authentication
    path('auth/', include('rest_framework.urls')),
]
