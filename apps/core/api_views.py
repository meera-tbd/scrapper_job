"""
API views for the core app.
"""

from rest_framework import viewsets, filters
from .models import Location
from .serializers import (
    LocationListSerializer, 
    LocationDetailSerializer
)


class LocationViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Location model with search and filtering.
    
    Provides:
    - List all locations with full details
    - Retrieve individual location details
    """
    
    queryset = Location.objects.prefetch_related('jobs').all()
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    # Search fields
    search_fields = ['name', 'city', 'state', 'country']
    
    # Ordering fields
    ordering_fields = ['name', 'city', 'state', 'country', 'created_at', 'updated_at']
    ordering = ['country', 'state', 'city', 'name']  # Default ordering
    
    def get_serializer_class(self):
        """Return appropriate serializer based on action."""
        if self.action == 'list':
            return LocationListSerializer
        return LocationDetailSerializer
    

