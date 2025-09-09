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
    """Clean serializer for job listing views with essential fields only."""
    
    company = CompanySerializer(read_only=True)
    location = LocationSerializer(read_only=True)
    posted_by = serializers.StringRelatedField(read_only=True)
    salary_display = serializers.ReadOnlyField()
    
    class Meta:
        model = JobPosting
        fields = [
            'id', 'company', 'location', 'posted_by', 'salary_display',
            'title', 'slug', 'description', 'job_category', 'job_type',
            'salary_raw_text', 'external_source', 'external_url', 'status',
            'posted_ago', 'date_posted', 'scraped_at', 'updated_at'
        ]
        depth = 1  # Include related data with depth
        read_only_fields = [
            'id', 'slug', 'posted_by', 'salary_display',
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
            'work_mode', 'salary_min', 'salary_max', 'salary_currency', 
            'salary_type', 'salary_raw_text', 'salary_display', 
            'external_source', 'external_url', 'external_id', 'status', 
            'posted_ago', 'date_posted', 'tags', 'tags_list', 
            'additional_info', 'scraped_at', 'updated_at'
        ]
        read_only_fields = [
            'id', 'slug', 'posted_by', 'salary_display', 'tags_list',
            'scraped_at', 'updated_at'
        ]


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