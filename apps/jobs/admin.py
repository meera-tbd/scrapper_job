"""
Admin configuration for job models.
"""

from django.contrib import admin
from django.utils.html import format_html
from .models import JobPosting


@admin.register(JobPosting)
class JobPostingAdmin(admin.ModelAdmin):
    """Admin configuration for JobPosting model."""
    list_display = [
        'title', 
        'company', 
        'location', 
        'job_category',
        'job_type',
        'salary_display_admin',
        'status',
        'external_source',
        'scraped_at'
    ]
    
    list_filter = [
        'job_category',
        'job_type', 
        'status',
        'external_source',
        'work_mode',
        'salary_currency',
        'salary_type',
        'scraped_at',
        'company__company_size',
        'location__country'
    ]
    
    search_fields = [
        'title', 
        'company__name', 
        'description', 
        'tags',
        'location__name',
        'location__city'
    ]
    
    readonly_fields = ['slug', 'scraped_at', 'updated_at', 'external_url_link']
    
    date_hierarchy = 'scraped_at'
    ordering = ['-scraped_at']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('title', 'slug', 'description', 'company', 'posted_by')
        }),
        ('Job Details', {
            'fields': ('job_category', 'job_type', 'experience_level', 'work_mode', 'location')
        }),
        ('Salary Information', {
            'fields': ('salary_min', 'salary_max', 'salary_currency', 'salary_type', 'salary_raw_text'),
            'classes': ('collapse',)
        }),
        ('External Source', {
            'fields': ('external_source', 'external_url_link', 'external_id')
        }),
        ('Metadata', {
            'fields': ('status', 'posted_ago', 'date_posted', 'tags'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('scraped_at', 'updated_at'),
            'classes': ('collapse',)
        }),
        ('Additional Data', {
            'fields': ('additional_info',),
            'classes': ('collapse',)
        }),
    )
    
    def salary_display_admin(self, obj):
        """Display salary information in list view."""
        return obj.salary_display
    salary_display_admin.short_description = 'Salary'
    
    def external_url_link(self, obj):
        """Display clickable external URL."""
        if obj.external_url:
            return format_html(
                '<a href="{}" target="_blank" rel="noopener">{}</a>',
                obj.external_url,
                obj.external_url
            )
        return 'No URL'
    external_url_link.short_description = 'External URL'
    
    # Custom actions
    actions = ['mark_as_inactive', 'mark_as_active', 'export_selected_jobs']
    
    def mark_as_inactive(self, request, queryset):
        """Mark selected jobs as inactive."""
        count = queryset.update(status='inactive')
        self.message_user(request, f'{count} jobs marked as inactive.')
    mark_as_inactive.short_description = 'Mark selected jobs as inactive'
    
    def mark_as_active(self, request, queryset):
        """Mark selected jobs as active."""
        count = queryset.update(status='active')
        self.message_user(request, f'{count} jobs marked as active.')
    mark_as_active.short_description = 'Mark selected jobs as active'
    
    def export_selected_jobs(self, request, queryset):
        """Export selected jobs."""
        count = queryset.count()
        self.message_user(request, f'{count} jobs ready for export.')
    export_selected_jobs.short_description = 'Export selected jobs'


# Customize admin site headers
admin.site.site_header = "Job Scraper Admin"
admin.site.site_title = "Job Scraper"
admin.site.index_title = "Welcome to Job Scraper Administration"