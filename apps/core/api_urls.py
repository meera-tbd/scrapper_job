"""
API URL configuration for core app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import LocationViewSet

router = DefaultRouter()
router.register(r'locations', LocationViewSet, basename='location')

urlpatterns = [
    path('', include(router.urls)),
]
