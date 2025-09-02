"""
Job models for the job scraper application.
"""

from django.db import models
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from apps.companies.models import Company
from apps.core.models import Location
from django_celery_beat.models import PeriodicTask, CrontabSchedule
from django.utils import timezone

User = get_user_model()


class JobPosting(models.Model):
    """Main model for storing job postings."""
    
    JOB_TYPE_CHOICES = [
        ('full_time', 'Full Time'),
        ('part_time', 'Part Time'),
        ('casual', 'Casual'),
        ('contract', 'Contract'),
        ('temporary', 'Temporary'),
        ('permanent', 'Permanent'),
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
    expired_at = models.DateTimeField(null=True, blank=True, help_text="When the job was marked expired")
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


class JobScript(models.Model):
    """Metadata for a scraping script that can be scheduled and executed."""
    name = models.CharField(max_length=120, unique=True)
    module_path = models.CharField(
        max_length=255,
        help_text="Python import path to callable, e.g. script.seek_job_scraper_advanced:run",
        unique=True,
    )
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['name']

    def __str__(self):
        return self.name


class JobScheduler(models.Model):
    """User-defined schedule that maps to a django-celery-beat PeriodicTask."""
    FREQUENCY_CHOICES = [
        ('minute', 'Every Minute'),
        ('hourly', 'Hourly'),
        ('daily', 'Daily'),
        ('weekly', 'Weekly'),
        ('monthly', 'Monthly'),
        ('custom_days', 'Custom Days Of Month'),
    ]

    script = models.ForeignKey(JobScript, on_delete=models.CASCADE, related_name='schedules')
    frequency = models.CharField(max_length=20, choices=FREQUENCY_CHOICES, default='daily')
    time_of_day = models.TimeField(help_text="Local time in TIME_ZONE to run")
    # For weekly
    day_of_week = models.CharField(
        max_length=20,
        blank=True,
        help_text="0-6 (0=Sunday) or mon,tue,... (django-celery-beat format)"
    )
    # For monthly or custom days
    days_of_month = models.CharField(
        max_length=60,
        blank=True,
        help_text="Comma-separated day numbers like 1,8,15 or */2 for every 2 days"
    )
    enabled = models.BooleanField(default=True)
    last_run_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Link to Beat schedule entries
    crontab = models.ForeignKey(CrontabSchedule, on_delete=models.SET_NULL, null=True, blank=True)
    periodic_task = models.OneToOneField(PeriodicTask, on_delete=models.SET_NULL, null=True, blank=True)

    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Job Scheduler'
        verbose_name_plural = 'Job Schedulers'

    def __str__(self):
        return f"{self.script.name} @ {self.frequency} {self.time_of_day}"

    def compute_cron_kwargs(self):
        """Build CrontabSchedule kwargs from frequency and time_of_day."""
        minute = f"{self.time_of_day.minute}"
        hour = f"{self.time_of_day.hour}"
        # Defaults
        day_of_week = '*'
        day_of_month = '*'
        month_of_year = '*'

        if self.frequency == 'minute':
            # Run every minute (ignore time_of_day for this option)
            minute = '*'
            hour = '*'
        elif self.frequency == 'hourly':
            # Run every hour at the specified minute (ignore hour from time_of_day)
            hour = '*'
        elif self.frequency == 'daily':
            pass
        elif self.frequency == 'weekly':
            day_of_week = self.day_of_week or 'mon'
        elif self.frequency == 'monthly':
            day_of_month = self.days_of_month or '1'
        elif self.frequency == 'custom_days':
            day_of_month = self.days_of_month or '1,8,15,22'

        return {
            'minute': minute,
            'hour': hour,
            'day_of_week': day_of_week,
            'day_of_month': day_of_month,
            'month_of_year': month_of_year,
            'timezone': timezone.get_current_timezone_name(),
        }