"""
Job models for the job scraper application.
"""

from django.db import models
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from apps.companies.models import Company
from apps.core.models import Location

User = get_user_model()


class JobPosting(models.Model):
    """Main model for storing job postings."""
    
    JOB_TYPE_CHOICES = [
        ('full_time', 'Full Time'),
        ('part_time', 'Part Time'),
        ('casual', 'Casual'),
        ('contract', 'Contract'),
        ('temporary', 'Temporary'),
        ('internship', 'Internship'),
        ('freelance', 'Freelance'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('expired', 'Expired'),
        ('filled', 'Filled'),
    ]
    
    SALARY_TYPE_CHOICES = [
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('yearly', 'Yearly'),
    ]
    
    CURRENCY_CHOICES = [
        ('AUD', 'Australian Dollar'),
        ('USD', 'US Dollar'),
        ('EUR', 'Euro'),
        ('GBP', 'British Pound'),
    ]
    
    JOB_CATEGORY_CHOICES = [
        # Core/general
        ('technology', 'Technology'),
        ('finance', 'Finance'),
        ('healthcare', 'Healthcare'),
        ('marketing', 'Marketing'),
        ('sales', 'Sales'),
        ('hr', 'Human Resources'),
        ('education', 'Education'),
        ('retail', 'Retail'),
        ('hospitality', 'Hospitality'),
        ('construction', 'Construction'),
        ('manufacturing', 'Manufacturing'),
        ('consulting', 'Consulting'),
        ('legal', 'Legal'),
        # Extended to match Australian boards like Chandler Macleod
        ('office_support', 'Office Support'),
        ('drivers_operators', 'Drivers & Operators'),
        ('technical_engineering', 'Technical & Engineering'),
        ('production_workers', 'Production Workers'),
        ('transport_logistics', 'Transport & Logistics'),
        ('mining_resources', 'Mining & Resources'),
        ('sales_marketing', 'Sales & Marketing'),
        ('executive', 'Executive'),
        ('other', 'Other'),
    ]

    # Allow runtime extension of choices for new categories encountered during scraping
    # Admin/forms will render newly appended choices without migrations
    
    # Basic Information
    title = models.CharField(max_length=200)
    slug = models.SlugField(max_length=250, unique=True)
    description = models.TextField()
    
    # Relationships
    company = models.ForeignKey(Company, on_delete=models.CASCADE, related_name='job_postings')
    posted_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='posted_jobs')
    location = models.ForeignKey(Location, on_delete=models.SET_NULL, null=True, blank=True, related_name='jobs')
    
    # Job Details
    job_category = models.CharField(max_length=50, choices=JOB_CATEGORY_CHOICES, default='other')
    job_type = models.CharField(max_length=20, choices=JOB_TYPE_CHOICES, default='full_time')
    experience_level = models.CharField(max_length=100, blank=True)
    work_mode = models.CharField(max_length=50, blank=True, help_text="Remote, Hybrid, On-site, etc.")
    
    # Salary Information
    salary_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=3, choices=CURRENCY_CHOICES, default='AUD')
    salary_type = models.CharField(max_length=10, choices=SALARY_TYPE_CHOICES, default='yearly')
    salary_raw_text = models.CharField(max_length=200, blank=True, help_text="Original salary text")
    
    # External Source Information
    external_source = models.CharField(max_length=100, default='seek.com.au')
    external_url = models.URLField(unique=True, help_text="Original job posting URL")
    external_id = models.CharField(max_length=100, blank=True, help_text="External system job ID")
    
    # Metadata
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    posted_ago = models.CharField(max_length=50, blank=True, help_text="Relative date like '2 days ago'")
    date_posted = models.DateTimeField(null=True, blank=True)
    tags = models.TextField(blank=True, help_text="Comma-separated tags or skills")
    
    # Timestamps
    scraped_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Additional Data
    additional_info = models.JSONField(default=dict, blank=True, help_text="Store any additional scraped data")
    
    class Meta:
        ordering = ['-scraped_at']
        verbose_name = 'Job Posting'
        verbose_name_plural = 'Job Postings'
        indexes = [
            models.Index(fields=['external_source', 'status']),
            models.Index(fields=['job_category', 'location']),
            models.Index(fields=['company', 'status']),
        ]
    
    def __str__(self):
        return f"{self.title} at {self.company.name}"
    
    def save(self, *args, **kwargs):
        if not self.slug:
            base_slug = slugify(self.title)
            unique_slug = base_slug
            counter = 1
            while JobPosting.objects.filter(slug=unique_slug).exists():
                unique_slug = f"{base_slug}-{counter}"
                counter += 1
            self.slug = unique_slug
        super().save(*args, **kwargs)
    
    @property
    def tags_list(self):
        """Return tags as a list."""
        return [tag.strip() for tag in self.tags.split(',') if tag.strip()] if self.tags else []
    
    @property
    def salary_display(self):
        """Return formatted salary string."""
        if self.salary_min and self.salary_max:
            if self.salary_min == self.salary_max:
                return f"{self.salary_currency} {self.salary_min:,.0f} per {self.salary_type}"
            else:
                return f"{self.salary_currency} {self.salary_min:,.0f} - {self.salary_max:,.0f} per {self.salary_type}"
        elif self.salary_min:
            return f"{self.salary_currency} {self.salary_min:,.0f} per {self.salary_type}"
        elif self.salary_raw_text:
            return self.salary_raw_text
        return "Salary not specified"