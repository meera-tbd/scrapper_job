"""
API URL configuration for jobs app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import JobPostingViewSet

router = DefaultRouter()
router.register(r'jobs', JobPostingViewSet, basename='job')

urlpatterns = [
    path('', include(router.urls)),
]
