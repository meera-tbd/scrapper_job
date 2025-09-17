"""
Admin configuration for company models.
"""

from django.contrib import admin
from .models import Company


@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    """Admin configuration for Company model."""
    list_display = ['name', 'company_size', 'website', 'city', 'state', 'phone', 'email', 'job_count', 'created_at']
    list_filter = ['company_size', 'state', 'country', 'created_at']
    search_fields = ['name', 'description', 'city', 'state', 'phone', 'email']
    prepopulated_fields = {'slug': ('name',)}
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'slug', 'description')
        }),
        ('Details', {
            'fields': ('website', 'company_size', 'logo', 'phone', 'email', 'details_url')
        }),
        ('Address', {
            'fields': (
                'address_line1', 'address_line2', 'city', 'state', 'postcode', 'country'
            )
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def job_count(self, obj):
        """Display the number of job postings for this company."""
        return obj.job_postings.count()
    job_count.short_description = 'Job Postings'


