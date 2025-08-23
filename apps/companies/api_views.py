"""
API views for the companies app.
"""

from rest_framework import viewsets, filters
from .models import Company
from .serializers import (
    CompanyListSerializer, 
    CompanyDetailSerializer
)


class CompanyViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Company model with search and filtering.
    
    Provides:
    - List all companies with full details
    - Retrieve individual company details
    """
    
    queryset = Company.objects.prefetch_related('job_postings').all()
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    # Search fields
    search_fields = ['name', 'description']
    
    # Ordering fields
    ordering_fields = ['name', 'created_at', 'updated_at']
    ordering = ['name']  # Default ordering
    
    def get_serializer_class(self):
        """Return appropriate serializer based on action."""
        if self.action == 'list':
            return CompanyListSerializer
        return CompanyDetailSerializer
    

