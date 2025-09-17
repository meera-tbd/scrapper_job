"""
Serializers for the jobs app API.
"""

from rest_framework import serializers
from .models import JobPosting, JobScript, JobScheduler
from django_celery_beat.models import (
    CrontabSchedule,
    IntervalSchedule,
    PeriodicTask,
    SolarSchedule,
    ClockedSchedule,
)
from apps.companies.models import Company
from apps.core.models import Location

# Add missing sync model imports
from .models import JobSyncRun, JobSyncPortalResult, JobSyncJobResult


class LocationSerializer(serializers.ModelSerializer):
    """Serializer for Location model."""
    
    class Meta:
        model = Location
        fields = ['id', 'name', 'city', 'state', 'country', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']


class CompanySerializer(serializers.ModelSerializer):
    """Serializer for Company model - essential fields only."""
    
    class Meta:
        model = Company
        fields = ['id', 'name', 'slug', 'description', 'created_at', 'updated_at']
        read_only_fields = ['id', 'slug', 'created_at', 'updated_at']


class JobPostingListSerializer(serializers.ModelSerializer):
    """Enhanced serializer for job listing views with more comprehensive fields."""
    
    company = CompanySerializer(read_only=True)
    location = LocationSerializer(read_only=True)
    posted_by = serializers.StringRelatedField(read_only=True)
    salary_display = serializers.ReadOnlyField()
    tags_list = serializers.ReadOnlyField()
    
    class Meta:
        model = JobPosting
        fields = [
            'id', 'company', 'location', 'posted_by', 'salary_display',
            'title', 'slug', 'description', 'job_category', 'job_type', 
            'experience_level', 'work_mode', 'job_closing_date',
            'salary_min', 'salary_max', 'salary_currency', 'salary_type', 'salary_raw_text',
            'external_source', 'external_url', 'external_id', 'status',
            'posted_ago', 'date_posted', 'expired_at',
            'tags', 'skills', 'preferred_skills', 'tags_list',
            'scraped_at', 'updated_at'
        ]
        depth = 1  # Include related data with depth
        read_only_fields = [
            'id', 'slug', 'posted_by', 'salary_display', 'tags_list',
            'scraped_at', 'updated_at'
        ]


class JobPostingDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for individual job views."""
    
    company = CompanySerializer(read_only=True)
    location = LocationSerializer(read_only=True)
    posted_by = serializers.StringRelatedField(read_only=True)
    salary_display = serializers.ReadOnlyField()
    tags_list = serializers.ReadOnlyField()
    
    class Meta:
        model = JobPosting
        fields = [
            'id', 'title', 'slug', 'description', 'company', 'location', 
            'posted_by', 'job_category', 'job_type', 'experience_level', 
            'work_mode', 'job_closing_date', 'salary_min', 'salary_max', 'salary_currency', 
            'salary_type', 'salary_raw_text', 'salary_display', 
            'external_source', 'external_url', 'external_id', 'status', 
            'posted_ago', 'date_posted', 'expired_at', 'tags', 'skills', 'preferred_skills', 'tags_list', 
            'additional_info', 'scraped_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'slug', 'posted_by', 'salary_display', 'tags_list',
            'scraped_at', 'updated_at'
        ]


class JobPostingFullSerializer(serializers.ModelSerializer):
    """Serializer that exposes ALL model fields, plus helpful computed fields.

    - Returns nested `company` and `location` objects
    - Also includes `company_id` and `location_id` for convenience
    - Includes computed `salary_display` and `tags_list`
    """

    company = CompanySerializer(read_only=True)
    location = LocationSerializer(read_only=True)
    posted_by = serializers.StringRelatedField(read_only=True)
    salary_display = serializers.ReadOnlyField()
    tags_list = serializers.ReadOnlyField()
    company_id = serializers.IntegerField(source='company.id', read_only=True)
    location_id = serializers.IntegerField(source='location.id', read_only=True)

    class Meta:
        model = JobPosting
        fields = '__all__'
        read_only_fields = ['id', 'slug', 'scraped_at', 'updated_at']

class JobScriptListSerializer(serializers.ModelSerializer):
    """Serializer for listing JobScript entries."""
    
    class Meta:
        model = JobScript
        fields = [
            'id', 'name', 'module_path', 'description', 'is_active',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class JobSchedulerListSerializer(serializers.ModelSerializer):
    """Serializer for listing JobScheduler entries."""
    
    script = JobScriptListSerializer(read_only=True)
    
    class Meta:
        model = JobScheduler
        fields = [
            'id', 'script', 'frequency', 'time_of_day', 'day_of_week',
            'days_of_month', 'enabled', 'last_run_at', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class CrontabScheduleSerializer(serializers.ModelSerializer):
    """Serializer for django-celery-beat CrontabSchedule."""
    
    # Ensure timezone is JSON serializable (ZoneInfo/pytz -> string)
    timezone = serializers.SerializerMethodField()

    def get_timezone(self, obj):
        tz = getattr(obj, 'timezone', None)
        if tz is None:
            return None
        return getattr(tz, 'key', getattr(tz, 'zone', str(tz)))

    class Meta:
        model = CrontabSchedule
        fields = '__all__'
        read_only_fields = ['id']


class IntervalScheduleSerializer(serializers.ModelSerializer):
    """Serializer for django-celery-beat IntervalSchedule."""
    
    class Meta:
        model = IntervalSchedule
        fields = '__all__'
        read_only_fields = ['id']


class SolarScheduleSerializer(serializers.ModelSerializer):
    """Serializer for django-celery-beat SolarSchedule."""
    
    class Meta:
        model = SolarSchedule
        fields = '__all__'
        read_only_fields = ['id']


class ClockedScheduleSerializer(serializers.ModelSerializer):
    """Serializer for django-celery-beat ClockedSchedule."""
    
    class Meta:
        model = ClockedSchedule
        fields = '__all__'
        read_only_fields = ['id']


class PeriodicTaskSerializer(serializers.ModelSerializer):
    """Serializer for django-celery-beat PeriodicTask."""
    
    class Meta:
        model = PeriodicTask
        fields = '__all__'
        read_only_fields = ['id']


# New serializers for job data sync models
class JobSyncRunSerializer(serializers.ModelSerializer):
    """Serializer for JobSyncRun executions."""

    class Meta:
        model = JobSyncRun
        fields = [
            'id', 'started_at', 'finished_at', 'incremental',
            'jobs_fetched', 'total_synced', 'status', 'error_message'
        ]
        read_only_fields = ['id', 'started_at', 'finished_at']


class JobSyncPortalResultSerializer(serializers.ModelSerializer):
    """Serializer for aggregated portal results within a sync run."""

    class Meta:
        model = JobSyncPortalResult
        fields = [
            'id', 'run', 'portal_name', 'target_url', 'batch_size',
            'success_count', 'failure_count', 'success_rate'
        ]
        read_only_fields = ['id']


class JobSyncJobResultSerializer(serializers.ModelSerializer):
    """Serializer for per-job push results."""

    class Meta:
        model = JobSyncJobResult
        fields = [
            'id', 'run', 'portal_result', 'job_id', 'request_url',
            'request_headers', 'request_payload', 'response_status',
            'response_body', 'was_success', 'error', 'created_at'
        ]
        read_only_fields = ['id', 'created_at']