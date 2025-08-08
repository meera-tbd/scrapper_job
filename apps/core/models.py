"""
Core models for the job scraper application.
"""

from django.db import models


class Location(models.Model):
    """Model to store location information."""
    name = models.CharField(max_length=100, unique=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True)
    country = models.CharField(max_length=100, default='Australia')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['country', 'state', 'city', 'name']
        
    def __str__(self):
        return self.name