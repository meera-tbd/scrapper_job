# 🔧 Code Reference - What We Built

## 📁 **File Structure & Purpose**

```
australia_job_scraper/
├── settings.py              # Added REST_FRAMEWORK config
├── urls.py                  # Added API routing
└── api_urls.py              # NEW: Main API endpoint definitions

apps/jobs/
├── models.py                # JobPosting model (existing)
├── serializers.py           # NEW: Job data formatting
├── api_views.py             # NEW: Job API logic
└── api_urls.py              # NEW: Jobs API routing

apps/companies/
├── models.py                # Company model (existing)
├── serializers.py           # NEW: Company data formatting
├── api_views.py             # NEW: Company API logic
└── api_urls.py              # NEW: Companies API routing

apps/core/
├── models.py                # Location model (existing)
├── serializers.py           # NEW: Location data formatting
├── api_views.py             # NEW: Location API logic
└── api_urls.py              # NEW: Locations API routing
```

---

## ⚙️ **Configuration Files**

### **requirements.txt** (Added packages)
```txt
djangorestframework>=3.14.0
django-filter>=23.0
```

### **settings.py** (Added configuration)
```python
INSTALLED_APPS = [
    # ... existing apps
    'rest_framework',  # Added this
]

# Added this entire section
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer',
    ],
    'DEFAULT_FILTER_BACKENDS': [
        'rest_framework.filters.SearchFilter',
        'rest_framework.filters.OrderingFilter',
    ],
    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema',
}
```

---

## 🔗 **URL Configuration**

### **australia_job_scraper/urls.py**
```python
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('australia_job_scraper.api_urls')),  # Added this line
]
```

### **australia_job_scraper/api_urls.py** (NEW FILE)
```python
"""
Main API URL configuration for the job scraper project.
"""

from django.urls import path, include
from rest_framework import permissions
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.reverse import reverse


@api_view(['GET'])
def api_root(request, format=None):
    """
    API root endpoint that provides links to the main API endpoints.
    """
    jobs_url = reverse('job-list', request=request, format=format)
    
    return Response({
        'jobs': jobs_url,
        'companies': reverse('company-list', request=request, format=format),
        'locations': reverse('location-list', request=request, format=format),
        'external_sources': reverse('job-external-sources', request=request, format=format),
        'jobs_from_seek': f"{jobs_url}?external_source=seek.com.au",
    })


urlpatterns = [
    # API root
    path('', api_root, name='api-root'),
    
    # App-specific API endpoints
    path('', include('apps.jobs.api_urls')),
    path('', include('apps.companies.api_urls')),
    path('', include('apps.core.api_urls')),
    
    # DRF browsable API authentication
    path('auth/', include('rest_framework.urls')),
]
```

### **apps/jobs/api_urls.py** (NEW FILE)
```python
"""
API URL configuration for jobs app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import JobPostingViewSet

router = DefaultRouter()
router.register(r'jobs', JobPostingViewSet, basename='job')

urlpatterns = [
    path('', include(router.urls)),
]
```

### **apps/companies/api_urls.py** (NEW FILE)
```python
"""
API URL configuration for companies app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import CompanyViewSet

router = DefaultRouter()
router.register(r'companies', CompanyViewSet, basename='company')

urlpatterns = [
    path('', include(router.urls)),
]
```

### **apps/core/api_urls.py** (NEW FILE)
```python
"""
API URL configuration for core app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .api_views import LocationViewSet

router = DefaultRouter()
router.register(r'locations', LocationViewSet, basename='location')

urlpatterns = [
    path('', include(router.urls)),
]
```

---

## 🗃️ **Models (Database Structure)**

### **apps/jobs/models.py** (EXISTING)
```python
class JobPosting(models.Model):
    """Main model for storing job postings."""
    
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
    work_mode = models.CharField(max_length=50, blank=True)
    
    # Salary Information
    salary_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    salary_currency = models.CharField(max_length=3, choices=CURRENCY_CHOICES, default='AUD')
    salary_type = models.CharField(max_length=10, choices=SALARY_TYPE_CHOICES, default='yearly')
    salary_raw_text = models.CharField(max_length=200, blank=True)
    
    # External Source Information
    external_source = models.CharField(max_length=100, default='seek.com.au')
    external_url = models.URLField(unique=True)
    external_id = models.CharField(max_length=100, blank=True)
    
    # Metadata
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    posted_ago = models.CharField(max_length=50, blank=True)
    date_posted = models.DateTimeField(null=True, blank=True)
    tags = models.TextField(blank=True)
    
    # Timestamps
    scraped_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Additional Data
    additional_info = models.JSONField(default=dict, blank=True)
```

### **apps/companies/models.py** (EXISTING)
```python
class Company(models.Model):
    """Model to store company information."""
    
    name = models.CharField(max_length=200)
    slug = models.SlugField(max_length=250, unique=True)
    description = models.TextField(blank=True)
    website = models.URLField(blank=True)
    company_size = models.CharField(max_length=20, choices=COMPANY_SIZE_CHOICES, default='medium')
    logo = models.URLField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### **apps/core/models.py** (EXISTING)
```python
class Location(models.Model):
    """Model to store location information."""
    name = models.CharField(max_length=100, unique=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True)
    country = models.CharField(max_length=100, default='Australia')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

---

## 📊 **Serializers (Data Formatting)**

### **apps/jobs/serializers.py** (NEW FILE)
```python
from rest_framework import serializers
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location

class LocationSerializer(serializers.ModelSerializer):
    """Serializer for Location model - essential fields only."""
    
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
    
    class Meta:
        model = JobPosting
        fields = '__all__'
        depth = 1
        read_only_fields = [
            'id', 'slug', 'posted_by', 'salary_display',
            'scraped_at', 'updated_at'
        ]
```

### **apps/companies/serializers.py** (NEW FILE)
```python
from rest_framework import serializers
from apps.companies.models import Company

class CompanyListSerializer(serializers.ModelSerializer):
    """Clean serializer for company listing views with essential fields only."""
    
    class Meta:
        model = Company
        fields = ['id', 'name', 'slug', 'description', 'created_at', 'updated_at']
        depth = 1  # Include related data with depth
        read_only_fields = ['id', 'slug', 'created_at', 'updated_at']

class CompanyDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for individual company views."""
    
    class Meta:
        model = Company
        fields = [
            'id', 'name', 'slug', 'description', 'website', 'company_size', 
            'logo', 'created_at', 'updated_at'
        ]
        depth = 1  # Include related data with depth
        read_only_fields = ['id', 'slug', 'created_at', 'updated_at']
```

### **apps/core/serializers.py** (NEW FILE)
```python
"""
Serializers for the core app API.
"""

from rest_framework import serializers
from .models import Location


class LocationListSerializer(serializers.ModelSerializer):
    """Complete serializer for location listing views with all fields."""
    
    class Meta:
        model = Location
        fields = '__all__'  # Include all fields from the model
        depth = 1  # Include related data with depth
        read_only_fields = ['id', 'created_at', 'updated_at']


class LocationDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for individual location views."""
    
    class Meta:
        model = Location
        fields = [
            'id', 'name', 'city', 'state', 'country', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
```

---

## 🎮 **ViewSets (API Logic)**

### **apps/jobs/api_views.py** (NEW FILE)
```python
"""
API views for the jobs app.
"""

from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Q, Count
from .models import JobPosting
from .serializers import (
    JobPostingListSerializer, 
    JobPostingDetailSerializer
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
        
        return queryset
    
    @action(detail=False, methods=['get'])
    def external_sources(self, request):
        """Get all external sources with job counts."""
        sources = JobPosting.objects.values('external_source').annotate(
            job_count=Count('id'),
            active_jobs=Count('id', filter=Q(status='active'))
        ).order_by('-active_jobs')
        
        return Response(list(sources))
```

### **apps/companies/api_views.py** (NEW FILE)
```python
"""
API views for the companies app.
"""

from rest_framework import viewsets, filters
from .models import Company
from .serializers import (
    CompanyListSerializer, 
    CompanyDetailSerializer
)


class CompanyViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Company model with search and filtering.
    
    Provides:
    - List all companies with full details
    - Retrieve individual company details
    """
    
    queryset = Company.objects.prefetch_related('job_postings').all()
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    # Search fields
    search_fields = ['name', 'description']
    
    # Ordering fields
    ordering_fields = ['name', 'created_at', 'updated_at']
    ordering = ['name']  # Default ordering
    
    def get_serializer_class(self):
        """Return appropriate serializer based on action."""
        if self.action == 'list':
            return CompanyListSerializer
        return CompanyDetailSerializer
```

### **apps/core/api_views.py** (NEW FILE)
```python
"""
API views for the core app.
"""

from rest_framework import viewsets, filters
from .models import Location
from .serializers import (
    LocationListSerializer, 
    LocationDetailSerializer
)


class LocationViewSet(viewsets.ModelViewSet):
    """
    ViewSet for Location model with search and filtering.
    
    Provides:
    - List all locations with full details
    - Retrieve individual location details
    """
    
    queryset = Location.objects.prefetch_related('jobs').all()
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    
    # Search fields
    search_fields = ['name', 'city', 'state', 'country']
    
    # Ordering fields
    ordering_fields = ['name', 'city', 'state', 'country', 'created_at', 'updated_at']
    ordering = ['country', 'state', 'city', 'name']  # Default ordering
    
    def get_serializer_class(self):
        """Return appropriate serializer based on action."""
        if self.action == 'list':
            return LocationListSerializer
        return LocationDetailSerializer
```

---

## 🚀 **How to Run & Test**

### **Start Development Server**
```bash
# Activate virtual environment (Windows)
venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Run migrations (if needed)
python manage.py makemigrations
python manage.py migrate

# Start server
python manage.py runserver
```

### **Test API Endpoints**
```bash
# PowerShell commands
Invoke-WebRequest -Uri "http://localhost:8000/api/" -Method GET
Invoke-WebRequest -Uri "http://localhost:8000/api/jobs/" -Method GET
Invoke-WebRequest -Uri "http://localhost:8000/api/companies/" -Method GET
Invoke-WebRequest -Uri "http://localhost:8000/api/locations/" -Method GET
Invoke-WebRequest -Uri "http://localhost:8000/api/jobs/external_sources/" -Method GET
Invoke-WebRequest -Uri "http://localhost:8000/api/jobs/?external_source=seek.com.au" -Method GET
```

### **Browser Testing**
Visit these URLs in your browser:
- `http://localhost:8000/api/` - API Root
- `http://localhost:8000/api/jobs/` - Jobs List
- `http://localhost:8000/api/companies/` - Companies List
- `http://localhost:8000/api/locations/` - Locations List

---

## 🎯 **Key Features Implemented**

✅ **5 API Endpoints**: Jobs, Companies, Locations, External Sources, Jobs from Seek  
✅ **Clean Data Models**: Essential fields only, optimized for performance  
✅ **Custom Filtering**: External source filter for jobs  
✅ **Search & Ordering**: Built-in search and sorting capabilities  
✅ **Nested Relationships**: Company and location data in job responses  
✅ **No Pagination**: Direct JSON arrays for faster loading  
✅ **Browsable API**: Interactive web interface for testing  

---

## 📊 **Current Project Statistics**

Based on the terminal logs, your API currently serves:
- **789+ active jobs** from multiple sources
- **528+ companies** with clean data structure
- **97+ locations** across Australia
- **Multiple external sources** including Seek, Adecco, Talent, Michael Page, etc.

### **API Performance**
- Jobs API: ~3.9MB response (all jobs)
- Companies API: ~232KB response (essential fields only)
- Locations API: ~58KB response (clean structure)
- External Sources API: ~17KB response (source statistics)

### **Data Sources Active**
- `seek.com.au` - Primary job source
- `adecco.com.au` - Recruitment agency
- `au.talent.com` - Job aggregator
- `michaelpage.com.au` - Professional recruitment
- And many more...

---

## 🎓 **What You've Learned**

### **Django Fundamentals**
- Project structure and app organization
- Models, serializers, and ViewSets
- URL routing and API configuration
- Database relationships and queries

### **REST API Design**
- Clean data models with essential fields
- Performance optimization techniques
- Custom filtering and search implementation
- Proper HTTP status codes and responses

### **Django REST Framework**
- ModelSerializer and ModelViewSet usage
- Custom actions and endpoints
- Filter backends and search functionality
- Browsable API interface

This is your complete Django REST API implementation! 🚀
