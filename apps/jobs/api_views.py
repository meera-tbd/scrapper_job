"""
API views for the jobs app.
"""

from rest_framework import viewsets, filters, permissions
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Q, Count
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from .models import JobPosting, JobScript, JobScheduler
from django.http import StreamingHttpResponse
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

    @action(
        detail=False,
        methods=['get'],
        url_path='feed',
        permission_classes=[permissions.AllowAny]
    )
    def feed(self, request):
        """Public, read-only job feed for network sharing.

        Query params:
        - since: ISO8601 datetime (e.g., 2025-09-04T00:00:00Z) or UNIX epoch seconds
        - limit: max items to return (default 100, max 500)
        - offset: pagination offset (default 0)
        - status: filter by status (default 'active')
        - external_source: optional source filter (icontains)
        """
        # Parse 'since'
        since_param = request.query_params.get('since')
        since_dt = None
        if since_param:
            try:
                # Try epoch seconds
                if since_param.isdigit():
                    since_dt = timezone.datetime.fromtimestamp(int(since_param), tz=timezone.utc)
                else:
                    parsed = parse_datetime(since_param)
                    if parsed is not None:
                        since_dt = parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed, timezone.utc)
            except Exception:
                since_dt = None

        # limit/offset with bounds
        try:
            limit = int(request.query_params.get('limit', '100'))
        except ValueError:
            limit = 100
        limit = max(1, min(500, limit))

        try:
            offset = int(request.query_params.get('offset', '0'))
        except ValueError:
            offset = 0
        offset = max(0, offset)

        status_param = request.query_params.get('status')
        external_source = request.query_params.get('external_source')

        qs = JobPosting.objects.select_related('company', 'location', 'posted_by')
        if status_param is None:
            qs = qs.filter(status='active')
        elif status_param:
            qs = qs.filter(status=status_param)
        if external_source:
            qs = qs.filter(external_source__icontains=external_source)
        if since_dt:
            qs = qs.filter(Q(updated_at__gte=since_dt) | Q(scraped_at__gte=since_dt))

        qs = qs.order_by('-updated_at', '-scraped_at')
        items = list(qs[offset:offset + limit])

        def to_feed_item(obj: JobPosting):
            # Salary
            salary_text = obj.salary_raw_text or ''
            if not salary_text:
                try:
                    salary_text = obj.salary_display
                except Exception:
                    salary_text = ''
            # Remote flag
            remote_allowed = False
            try:
                remote_allowed = 'remote' in (obj.work_mode or '').lower()
            except Exception:
                remote_allowed = False
            # Posted date
            posted_dt = obj.date_posted or obj.scraped_at
            posted_iso = posted_dt.isoformat() if posted_dt else None
            return {
                'job_id': str(obj.pk),
                'title': obj.title,
                'company': getattr(obj.company, 'name', ''),
                'location': getattr(obj.location, 'name', '') if obj.location_id else '',
                'description': obj.description or '',
                'salary': salary_text,
                'job_type': obj.job_type or 'full_time',
                'experience_level': obj.experience_level or '',
                'skills': obj.tags_list,
                'posted_date': posted_iso,
                'application_url': obj.external_url or '',
                'source_site': obj.external_source or 'scraper',
                'category': obj.job_category or 'other',
                'remote_allowed': remote_allowed,
                'updated_at': obj.updated_at.isoformat() if obj.updated_at else None,
            }

        data = [to_feed_item(obj) for obj in items]
        return Response({
            'count': len(data),
            'offset': offset,
            'limit': limit,
            'since': since_dt.isoformat() if since_dt else None,
            'server_time': timezone.now().isoformat(),
            'results': data,
        })

    @action(
        detail=False,
        methods=['get'],
        url_path='export',
        permission_classes=[permissions.AllowAny]
    )
    def export(self, request):
        """Stream ALL job data over the network.

        Query params:
        - format: ndjson (default) or json
        - external_source: optional icontains filter
        - status: optional exact match filter (if omitted, includes all statuses)
        """
        fmt = (request.query_params.get('format') or 'ndjson').lower()
        external_source = request.query_params.get('external_source')
        status_param = request.query_params.get('status')

        qs = JobPosting.objects.select_related('company', 'location', 'posted_by')
        if external_source:
            qs = qs.filter(external_source__icontains=external_source)
        if status_param:
            qs = qs.filter(status=status_param)
        qs = qs.order_by('id')  # stable ordering for full export

        def serialize(obj: JobPosting):
            # Salary text
            salary_text = obj.salary_raw_text or ''
            if not salary_text:
                try:
                    salary_text = obj.salary_display
                except Exception:
                    salary_text = ''
            # Remote flag
            remote_allowed = False
            try:
                remote_allowed = 'remote' in (obj.work_mode or '').lower()
            except Exception:
                remote_allowed = False

            return {
                'id': obj.pk,
                'title': obj.title,
                'slug': obj.slug,
                'description': obj.description or '',
                'company': getattr(obj.company, 'name', ''),
                'company_id': getattr(obj.company, 'id', None),
                'location': getattr(obj.location, 'name', '') if obj.location_id else '',
                'location_id': obj.location_id,
                'posted_by': str(getattr(obj.posted_by, 'username', '')),
                'job_category': obj.job_category,
                'job_type': obj.job_type,
                'experience_level': obj.experience_level or '',
                'work_mode': obj.work_mode or '',
                'salary_min': obj.salary_min,
                'salary_max': obj.salary_max,
                'salary_currency': obj.salary_currency,
                'salary_type': obj.salary_type,
                'salary_raw_text': obj.salary_raw_text or '',
                'salary_display': salary_text,
                'external_source': obj.external_source,
                'external_url': obj.external_url,
                'external_id': obj.external_id,
                'status': obj.status,
                'posted_ago': obj.posted_ago or '',
                'date_posted': obj.date_posted.isoformat() if obj.date_posted else None,
                'expired_at': obj.expired_at.isoformat() if obj.expired_at else None,
                'tags': obj.tags or '',
                'tags_list': obj.tags_list,
                'additional_info': obj.additional_info or {},
                'scraped_at': obj.scraped_at.isoformat() if obj.scraped_at else None,
                'updated_at': obj.updated_at.isoformat() if obj.updated_at else None,
                'remote_allowed': remote_allowed,
            }

        if fmt == 'json':
            import json as _json

            def json_stream():
                yield '['
                first = True
                for obj in qs.iterator(chunk_size=1000):
                    item = serialize(obj)
                    if first:
                        first = False
                    else:
                        yield ','
                    yield _json.dumps(item, ensure_ascii=False)
                yield ']'

            resp = StreamingHttpResponse(json_stream(), content_type='application/json; charset=utf-8')
            resp['Content-Disposition'] = 'attachment; filename="jobs_export.json"'
            return resp

        # Default: NDJSON (one JSON object per line)
        import json as _json

        def ndjson_stream():
            for obj in qs.iterator(chunk_size=1000):
                yield _json.dumps(serialize(obj), ensure_ascii=False) + "\n"

        resp = StreamingHttpResponse(ndjson_stream(), content_type='application/x-ndjson; charset=utf-8')
        resp['Content-Disposition'] = 'attachment; filename="jobs_export.ndjson"'
        return resp


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
