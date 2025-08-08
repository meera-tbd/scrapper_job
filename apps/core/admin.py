"""
Admin configuration for core models.
"""

from django.contrib import admin
from .models import Location


@admin.register(Location)
class LocationAdmin(admin.ModelAdmin):
    """Admin configuration for Location model."""
    list_display = ['name', 'city', 'state', 'country', 'job_count', 'created_at']
    list_filter = ['country', 'state']
    search_fields = ['name', 'city', 'state', 'country']
    ordering = ['country', 'state', 'city', 'name']
    readonly_fields = ['created_at', 'updated_at']
    
    def job_count(self, obj):
        """Display the number of jobs for this location."""
        return obj.jobs.count()
    job_count.short_description = 'Number of Jobs'