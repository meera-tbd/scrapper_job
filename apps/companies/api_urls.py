"""
API URL configuration for companies app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import CompanyViewSet

router = DefaultRouter()
router.register(r'companies', CompanyViewSet, basename='company')

urlpatterns = [
    path('', include(router.urls)),
]
