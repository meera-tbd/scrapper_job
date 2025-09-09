"""
Main API URL configuration for the job scraper project.
"""

from django.urls import path, include
from rest_framework import permissions
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.reverse import reverse
from django.utils import timezone
from rest_framework import permissions
from apps.core.auth_views import RegisterView, MeView


@api_view(['GET'])
@permission_classes([permissions.AllowAny])
def api_root(request, format=None):
    """
    API root endpoint that provides links to the main API endpoints.
    """
    jobs_url = reverse('job-list', request=request, format=format)
    current_year = timezone.now().year
    
    return Response({
        'jobs': jobs_url,
        'companies': reverse('company-list', request=request, format=format),
        'locations': reverse('location-list', request=request, format=format),
        'external_sources': reverse('job-external-sources', request=request, format=format),
        'jobs_from_seek': f"{jobs_url}?external_source=seek.com.au",
        'jobs_in_september': f"{jobs_url}?month=9&year={current_year}",

        # Job automation & scheduler endpoints
        'job_scripts': reverse('jobscript-list', request=request, format=format),
        'job_schedulers': reverse('jobscheduler-list', request=request, format=format),

        # django-celery-beat management endpoints (read-only)
        'beat_periodic_tasks': reverse('beat-periodic-task-list', request=request, format=format),
        'beat_crontabs': reverse('beat-crontab-list', request=request, format=format),
        'beat_intervals': reverse('beat-interval-list', request=request, format=format),
        'beat_solar_events': reverse('beat-solar-list', request=request, format=format),
        'beat_clocked': reverse('beat-clocked-list', request=request, format=format),

        # Authentication endpoints
        'auth_register': reverse('auth-register', request=request, format=format),
        'auth_login': reverse('token_obtain_pair', request=request, format=format),
        'auth_refresh': reverse('token_refresh', request=request, format=format),
        'auth_me': reverse('auth-me', request=request, format=format),
    })


urlpatterns = [
    # API root
    path('', api_root, name='api-root'),
    # JWT auth endpoints
    path('token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    # Registration and current user endpoints
    path('auth/register/', RegisterView.as_view(), name='auth-register'),
    path('auth/me/', MeView.as_view(), name='auth-me'),
    
    # App-specific API endpoints
    path('', include('apps.jobs.api_urls')),
    path('', include('apps.companies.api_urls')),
    path('', include('apps.core.api_urls')),
    
    # DRF browsable API authentication (login/logout for admin/testing)
    path('auth/', include('rest_framework.urls')),
]
