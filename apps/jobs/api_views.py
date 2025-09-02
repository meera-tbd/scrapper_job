"""
API views for the jobs app.
"""

from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Q, Count
from .models import JobPosting, JobScript, JobScheduler
from django_celery_beat.models import (
    CrontabSchedule,
    IntervalSchedule,
    PeriodicTask,
    SolarSchedule,
    ClockedSchedule,
)
from .serializers import (
    JobPostingListSerializer, 
    JobPostingDetailSerializer,
    JobScriptListSerializer,
    JobSchedulerListSerializer,
    CrontabScheduleSerializer,
    IntervalScheduleSerializer,
    PeriodicTaskSerializer,
    SolarScheduleSerializer,
    ClockedScheduleSerializer,
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

        # Month/Year filter (e.g., ?month=9&year=2025). Defaults to current year if only month is provided.
        month_param = self.request.query_params.get('month')
        year_param = self.request.query_params.get('year')
        if month_param:
            try:
                month_int = int(month_param)
                if 1 <= month_int <= 12:
                    from django.utils import timezone
                    year_int = int(year_param) if year_param else timezone.now().year
                    # Filter by date_posted month/year; fallback to scraped_at if date_posted missing
                    queryset = queryset.filter(
                        Q(date_posted__year=year_int, date_posted__month=month_int)
                        | Q(date_posted__isnull=True, scraped_at__year=year_int, scraped_at__month=month_int)
                    )
            except ValueError:
                # Ignore invalid month/year values silently to avoid breaking existing clients
                pass
        
        return queryset
    

    
    @action(detail=False, methods=['get'])
    def external_sources(self, request):
        """Get all external sources with job counts."""
        sources = JobPosting.objects.values('external_source').annotate(
            job_count=Count('id'),
            active_jobs=Count('id', filter=Q(status='active'))
        ).order_by('-active_jobs')
        
        return Response(list(sources))


class ReadOnlyListViewSet(viewsets.ReadOnlyModelViewSet):
    """Base class for read-only list/retrieve endpoints."""
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]


class JobScriptViewSet(ReadOnlyListViewSet):
    """List/retrieve JobScript entries."""
    queryset = JobScript.objects.all()
    serializer_class = JobScriptListSerializer
    search_fields = ['name', 'module_path', 'description']
    ordering_fields = ['name', 'created_at', 'updated_at']
    ordering = ['name']


class JobSchedulerViewSet(ReadOnlyListViewSet):
    """List/retrieve JobScheduler entries."""
    queryset = JobScheduler.objects.select_related('script').all()
    serializer_class = JobSchedulerListSerializer
    search_fields = ['script__name', 'frequency', 'day_of_week', 'days_of_month']
    ordering_fields = ['created_at', 'updated_at', 'last_run_at', 'enabled']
    ordering = ['-created_at']


class CrontabScheduleViewSet(ReadOnlyListViewSet):
    queryset = CrontabSchedule.objects.all()
    serializer_class = CrontabScheduleSerializer
    search_fields = ['minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year', 'timezone']
    ordering_fields = ['id']


class IntervalScheduleViewSet(ReadOnlyListViewSet):
    queryset = IntervalSchedule.objects.all()
    serializer_class = IntervalScheduleSerializer
    search_fields = ['every', 'period']
    ordering_fields = ['every', 'period']


class SolarScheduleViewSet(ReadOnlyListViewSet):
    queryset = SolarSchedule.objects.all()
    serializer_class = SolarScheduleSerializer
    search_fields = ['event', 'latitude', 'longitude']
    ordering_fields = ['id']


class ClockedScheduleViewSet(ReadOnlyListViewSet):
    queryset = ClockedSchedule.objects.all()
    serializer_class = ClockedScheduleSerializer
    search_fields = ['clocked_time']
    ordering_fields = ['clocked_time']


class PeriodicTaskViewSet(ReadOnlyListViewSet):
    queryset = PeriodicTask.objects.select_related('crontab', 'solar', 'clocked').all()
    serializer_class = PeriodicTaskSerializer
    search_fields = ['name', 'task', 'description', 'queue']
    ordering_fields = ['last_run_at', 'total_run_count', 'date_changed', 'enabled']
    ordering = ['-date_changed']
