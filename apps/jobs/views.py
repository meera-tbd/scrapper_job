from django.shortcuts import render
from django.core.paginator import Paginator
from .models import JobPosting


def index(request):
    """Display all scraped jobs from the professional database structure."""
    jobs = JobPosting.objects.all().order_by('-scraped_at')
    
    # Add pagination (10 jobs per page)
    paginator = Paginator(jobs, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    context = {
        'jobs': page_obj,
        'total_jobs': jobs.count(),
        'total_locations': JobPosting.objects.values('location').distinct().count(),
        'total_companies': JobPosting.objects.values('company').distinct().count(),
    }
    
    return render(request, 'jobs/professional_index.html', context)