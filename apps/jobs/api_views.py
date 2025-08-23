"""
API views for the jobs app.
"""

from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Q, Count
from .models import JobPosting
from .serializers import (
    JobPostingListSerializer, 
    JobPostingDetailSerializer
)


class JobPostingViewSet(viewsets.ModelViewSet):
    """
    ViewSet for JobPosting model with external_source filtering.
    
    Provides:
    - List all jobs with external_source filter
    - Retrieve individual job details
    - External sources listing
    """
    
    queryset = JobPosting.objects.select_related('company', 'location', 'posted_by').all()
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    # Search fields
    search_fields = [
        'title', 'description', 'company__name', 'location__name', 
        'location__city', 'location__state', 'tags'
    ]
    
    # Ordering fields
    ordering_fields = [
        'title', 'scraped_at', 'date_posted', 'salary_min', 'salary_max', 
        'company__name', 'location__name'
    ]
    ordering = ['-scraped_at']  # Default ordering
    
    def get_serializer_class(self):
        """Return appropriate serializer based on action."""
        if self.action == 'list':
            return JobPostingListSerializer
        return JobPostingDetailSerializer
    
    def get_queryset(self):
        """
        Optionally restricts the returned jobs by filtering against
        query parameters in the URL.
        """
        queryset = self.queryset
        
        # Filter by active status by default, unless explicitly requested
        status_param = self.request.query_params.get('status', None)
        if status_param is None:
            queryset = queryset.filter(status='active')
        
        # External source filter - ONLY filter available
        external_source = self.request.query_params.get('external_source', None)
        if external_source:
            queryset = queryset.filter(external_source__icontains=external_source)
        
        return queryset
    

    
    @action(detail=False, methods=['get'])
    def external_sources(self, request):
        """Get all external sources with job counts."""
        sources = JobPosting.objects.values('external_source').annotate(
            job_count=Count('id'),
            active_jobs=Count('id', filter=Q(status='active'))
        ).order_by('-active_jobs')
        
        return Response(list(sources))
