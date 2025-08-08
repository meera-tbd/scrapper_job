"""
Company models for the job scraper application.
"""

from django.db import models
from django.utils.text import slugify


class Company(models.Model):
    """Model to store company information."""
    
    COMPANY_SIZE_CHOICES = [
        ('startup', 'Startup (1-10)'),
        ('small', 'Small (11-50)'),
        ('medium', 'Medium (51-200)'),
        ('large', 'Large (201-1000)'),
        ('enterprise', 'Enterprise (1000+)'),
    ]
    
    name = models.CharField(max_length=200)
    slug = models.SlugField(max_length=250, unique=True)
    description = models.TextField(blank=True)
    website = models.URLField(blank=True)
    company_size = models.CharField(max_length=20, choices=COMPANY_SIZE_CHOICES, default='medium')
    logo = models.URLField(blank=True, help_text="URL to company logo")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['name']
        verbose_name_plural = 'Companies'
        
    def __str__(self):
        return self.name
    
    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.name)
        super().save(*args, **kwargs)