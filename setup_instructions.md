# 🌐 Multi-Website Job Scraper Setup Instructions

Complete setup guide for the comprehensive job scraper suite supporting **Seek.com.au**, **Jora Australia**, **JobServe Australia**, and **Workforce Australia**.

## 📋 Prerequisites

### System Requirements
- **Python 3.8 or higher**
- **pip** (Python package installer)
- **Git** (optional, for version control)
- **8GB+ RAM** (recommended for multiple browser instances)
- **Stable internet connection**
- **Windows/macOS/Linux** (cross-platform compatible)

### Recommended Development Environment
- **Code Editor**: VS Code, PyCharm, or similar
- **Terminal**: Command Prompt, PowerShell, or Bash
- **Browser**: Chrome/Chromium (for debugging)

## 🚀 Installation & Setup

### Step 1: Clone or Download Project
```bash
# Option 1: Clone with Git
git clone <repository-url>
cd seek_scraper_project

# Option 2: Download and extract ZIP
# Extract to desired directory and navigate to it
```

### Step 2: Install Python Dependencies
```bash
# Install all required packages
pip install -r requirements.txt
```

**Key Dependencies Installed:**
- `Django` - Web framework and database ORM
- `playwright` - Browser automation for all scrapers
- `beautifulsoup4` - HTML parsing backup
- `requests` - HTTP requests
- `python-dateutil` - Date parsing for job postings

### Step 3: Install Playwright Browsers
```bash
# Install Chromium browser for automation (required)
playwright install chromium

# Optional: Install all browsers for comprehensive testing
playwright install
```

### Step 4: Set up Django Database
```bash
# Create database migrations for all apps
python manage.py makemigrations companies
python manage.py makemigrations core  
python manage.py makemigrations jobs

# Apply all migrations to create database tables
python manage.py migrate

# Create a superuser for admin access (highly recommended)
python manage.py createsuperuser
```

## 🔧 Running Individual Scrapers

### 🇦🇺 **Seek.com.au Scraper**
```bash
# Scrape 30 jobs from Seek Australia (recommended for testing)
python seek_job_scraper_advanced.py 30

# Scrape 100 jobs  
python seek_job_scraper_advanced.py 100

# Scrape all available jobs (no limit - can be 1000+)
python seek_job_scraper_advanced.py
```

**Expected Output:**
- High job volume (1000+ available)
- Comprehensive salary data
- Detailed location information
- Success rate: 95%+

### 🔍 **Jora Australia Scraper**
```bash
# Scrape 50 jobs from Jora (recommended for testing)
python jora_job_scraper_advanced.py 50

# Scrape 200 jobs
python jora_job_scraper_advanced.py 200

# Scrape all available jobs (no limit)
python jora_job_scraper_advanced.py
```

**Expected Output:**
- Medium-high job volume (500+ available)
- Fast scraping speed
- Good job categorization
- Success rate: 90%+

### 💼 **JobServe Australia Scraper**
```bash
# Scrape 10 jobs from JobServe (recommended - limited job volume)
python jobserve_australia_scraper_advanced.py 10

# Scrape 25 jobs
python jobserve_australia_scraper_advanced.py 25

# Scrape all available jobs (typically 20-50 jobs)
python jobserve_australia_scraper_advanced.py
```

**Expected Output:**
- Low job volume but high quality
- IT and healthcare focus
- International opportunities
- Success rate: 100%

### 🏛️ **Workforce Australia Scraper**
```bash
# Scrape 15 jobs from Workforce Australia (recommended)
python workforce_australia_scraper_advanced.py 15

# Scrape 50 jobs
python workforce_australia_scraper_advanced.py 50

# Scrape all available jobs (typically 100-500 jobs)
python workforce_australia_scraper_advanced.py
```

**Expected Output:**
- Government and public sector jobs
- Official Australian government positions
- Slower scraping due to site complexity
- Success rate: 85%+

## 🔄 Running Multiple Scrapers

### Sequential Execution (Recommended)
```bash
# Run all four scrapers with reasonable limits
python seek_job_scraper_advanced.py 50 && \
python jora_job_scraper_advanced.py 30 && \
python jobserve_australia_scraper_advanced.py 15 && \
python workforce_australia_scraper_advanced.py 20
```

### Quick Test Run
```bash
# Small batch testing across all sites
python seek_job_scraper_advanced.py 10 && \
python jora_job_scraper_advanced.py 10 && \
python jobserve_australia_scraper_advanced.py 5 && \
python workforce_australia_scraper_advanced.py 10
```

### Production Data Collection
```bash
# Large-scale data collection
python seek_job_scraper_advanced.py 200 && \
python jora_job_scraper_advanced.py 150 && \
python jobserve_australia_scraper_advanced.py && \
python workforce_australia_scraper_advanced.py 100
```

## 🖥️ Access Django Admin Panel

### Start Development Server
```bash
# Start Django development server
python manage.py runserver

# Server will start at http://127.0.0.1:8000/
```

### Access Admin Interface
1. **Navigate to**: http://127.0.0.1:8000/admin/
2. **Login** with your superuser credentials
3. **Browse scraped data** in organized sections:
   - **Jobs > Job postings** (all scraped jobs)
   - **Companies > Companies** (company information)  
   - **Core > Locations** (location data)

### Admin Panel Features
- **Filter and Search**: Find jobs by source, category, location
- **Bulk Actions**: Manage multiple job entries
- **Data Export**: Export filtered results
- **Statistics**: View job counts by source and category

## ⚙️ Configuration Options

### Scraper Mode Configuration
Each scraper can be configured by editing the respective Python files:

```python
# In any scraper file, look for:
browser = p.chromium.launch(
    headless=False,  # Change to True for headless mode
    args=[...]
)
```

**Headless Mode Benefits:**
- ✅ Faster execution
- ✅ Lower resource usage  
- ✅ Can run in background
- ❌ No visual monitoring

**Visible Mode Benefits:**
- ✅ Visual progress monitoring
- ✅ Easier debugging
- ✅ Can see anti-detection in action
- ❌ Slower execution

### Database Configuration

#### SQLite (Default)
```python
# In seek_scraper/settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}
```

#### PostgreSQL (Production Recommended)
```bash
# Install PostgreSQL dependencies
pip install psycopg2-binary
```

```python
# Update seek_scraper/settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'job_scraper_db',
        'USER': 'your_username',
        'PASSWORD': 'your_password', 
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
```

### Anti-Detection Configuration
Adjust timing and behavior in scraper files:

```python
# Human-like delays (edit in scraper files)
self.human_delay(2, 5)  # Wait 2-5 seconds (conservative)
self.human_delay(1, 3)  # Wait 1-3 seconds (moderate)
self.human_delay(0.5, 2)  # Wait 0.5-2 seconds (aggressive)
```

## 📊 Usage Guide

### Understanding Scraper Characteristics

| Scraper | Job Volume | Scraping Speed | Success Rate | Best For |
|---------|------------|----------------|--------------|----------|
| **Seek** | High (1000+) | Medium | 95%+ | Comprehensive job hunting |
| **Jora** | High (500+) | Fast | 90%+ | Quick job discovery |
| **JobServe** | Low (20-100) | Fast | 100% | Specialized IT/Healthcare |
| **Workforce AU** | Medium (100-500) | Slow | 85%+ | Government/Public sector |

### Recommended Scraping Strategy

#### **For Testing & Development:**
```bash
# Small batches for testing
python seek_job_scraper_advanced.py 10
python jora_job_scraper_advanced.py 15  
python jobserve_australia_scraper_advanced.py 5
python workforce_australia_scraper_advanced.py 10
```

#### **For Regular Data Collection:**
```bash
# Moderate batches for regular updates
python seek_job_scraper_advanced.py 100
python jora_job_scraper_advanced.py 75
python jobserve_australia_scraper_advanced.py 20
python workforce_australia_scraper_advanced.py 50
```

#### **For Comprehensive Data Gathering:**
```bash
# Large batches for comprehensive collection
python seek_job_scraper_advanced.py 500
python jora_job_scraper_advanced.py 300
python jobserve_australia_scraper_advanced.py  # All available
python workforce_australia_scraper_advanced.py 200
```

## 📈 Monitoring Scraper Progress

### Real-time Console Output
- **Job Processing Updates**: Live job-by-job progress
- **Success/Error Statistics**: Real-time metrics
- **Anti-Detection Status**: Stealth operation feedback
- **Pagination Progress**: Page-by-page navigation

### Log Files
- `scraper_professional.log` (Seek)
- `jora_scraper_professional.log` (Jora)  
- `jobserve_australia_scraper.log` (JobServe)
- `workforce_australia_scraper.log` (Workforce Australia)

### Admin Panel Monitoring
- **Browse all scraped jobs** by source
- **Filter by category, location, salary**
- **View company and location data**
- **Monitor duplicate detection effectiveness**

## 🗃️ Data Management

### Viewing and Managing Results

#### **Django Admin Interface:**
```bash
# Start server
python manage.py runserver

# Navigate to admin panel
# http://127.0.0.1:8000/admin/

# Available sections:
# - Jobs > Job postings (all scraped jobs)
# - Companies > Companies (company information)  
# - Core > Locations (location data)
```

#### **Django Shell Queries:**
```bash
# Open Django shell
python manage.py shell
```

```python
# Import models
from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location

# Get statistics
total_jobs = JobPosting.objects.count()
seek_jobs = JobPosting.objects.filter(external_source='seek.com.au').count()
jora_jobs = JobPosting.objects.filter(external_source='jora_au').count()
jobserve_jobs = JobPosting.objects.filter(external_source='jobserve_australia').count()
workforce_jobs = JobPosting.objects.filter(external_source='workforce_australia').count()

print(f"Total jobs: {total_jobs}")
print(f"Seek jobs: {seek_jobs}")
print(f"Jora jobs: {jora_jobs}")
print(f"JobServe jobs: {jobserve_jobs}")
print(f"Workforce Australia jobs: {workforce_jobs}")

# Get jobs by category
tech_jobs = JobPosting.objects.filter(job_category='technology')
healthcare_jobs = JobPosting.objects.filter(job_category='healthcare')
finance_jobs = JobPosting.objects.filter(job_category='finance')

# Get remote jobs
remote_jobs = JobPosting.objects.filter(work_mode='remote')

# Get high-salary jobs (above $100k AUD)
high_salary = JobPosting.objects.filter(
    salary_min__gte=100000,
    salary_currency='AUD'
)

# Get recent jobs (last 24 hours)
from datetime import datetime, timedelta
recent_jobs = JobPosting.objects.filter(
    created_at__gte=datetime.now() - timedelta(days=1)
)
```

## 🗄️ Database Schema Deep Dive

### JobPosting Model
**Primary job data with rich metadata:**
```python
class JobPosting(models.Model):
    # Basic Information
    title = models.CharField(max_length=200)
    slug = models.SlugField(unique=True)
    description = models.TextField()
    
    # Relationships
    company = models.ForeignKey(Company)
    location = models.ForeignKey(Location, null=True)
    posted_by = models.ForeignKey(User)
    
    # Job Classification
    job_category = models.CharField(max_length=50)  # Auto-categorized
    job_type = models.CharField(max_length=20)      # full_time, part_time, etc.
    work_mode = models.CharField(max_length=20)     # remote, hybrid, onsite
    experience_level = models.CharField(max_length=20)  # junior, mid, senior
    
    # Compensation
    salary_min = models.DecimalField(null=True)
    salary_max = models.DecimalField(null=True)
    salary_currency = models.CharField(default='AUD')
    salary_type = models.CharField(default='yearly')
    salary_raw_text = models.CharField(max_length=200)
    
    # Source Information
    external_source = models.CharField(max_length=50)
    external_url = models.URLField(unique=True)
    
    # Metadata
    status = models.CharField(default='active')
    date_posted = models.DateField(null=True)
    posted_ago = models.CharField(max_length=50)
    tags = models.TextField(blank=True)
    additional_info = models.JSONField(default=dict)
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### Company Model
**Normalized company information:**
```python
class Company(models.Model):
    name = models.CharField(max_length=200)
    slug = models.SlugField(unique=True)
    description = models.TextField(blank=True)
    website = models.URLField(blank=True)
    company_size = models.CharField(max_length=20)
    industry = models.CharField(max_length=100, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

### Location Model
**Geographical data with Australian focus:**
```python
class Location(models.Model):
    name = models.CharField(max_length=200, unique=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True)
    country = models.CharField(max_length=100, default='Australia')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

## 🛠️ Troubleshooting

### Common Installation Issues

#### **1. "No module named 'django'"**
```bash
# Solution: Install requirements
pip install -r requirements.txt
```

#### **2. "playwright not found"**
```bash
# Solution: Install Playwright browsers
playwright install chromium
```

#### **3. "Database doesn't exist"**
```bash
# Solution: Run migrations
python manage.py makemigrations
python manage.py migrate
```

#### **4. "Permission denied"**
```bash
# Solution: Check directory and permissions
pwd  # Ensure you're in the project directory
ls -la  # Check file permissions
```

### Scraper-Specific Issues

#### **Seek Scraper Issues:**
- **Problem**: CAPTCHA challenges during peak hours
- **Solution**: Run during off-peak hours or increase delays

- **Problem**: Rate limiting on rapid requests  
- **Solution**: Increase `human_delay()` values to 3-8 seconds

#### **Jora Scraper Issues:**
- **Problem**: Empty pages or no job results
- **Solution**: Check internet connection and try again

- **Problem**: Timeout errors
- **Solution**: Increase timeout values in scraper settings

#### **JobServe Scraper Issues:**
- **Problem**: Limited job results (20-50 jobs)
- **Solution**: This is normal for JobServe Australia

- **Problem**: Browser hanging on page load
- **Solution**: Use headless mode or restart scraper

#### **Workforce Australia Issues:**
- **Problem**: Slow loading or maintenance messages
- **Solution**: Wait and retry, government sites have maintenance periods

### Performance Issues

#### **High Memory Usage:**
```bash
# Monitor memory usage
top -p $(pgrep -f python)

# Solutions:
# 1. Use headless mode
# 2. Reduce job limits
# 3. Run scrapers sequentially, not parallel
```

#### **Slow Scraping Speed:**
```bash
# Solutions:
# 1. Reduce delay times (but increase detection risk)
# 2. Use headless mode
# 3. Check internet connection speed
```

#### **Browser Crashes:**
```bash
# Solutions:
# 1. Increase available RAM
# 2. Close other applications
# 3. Use headless mode
# 4. Reduce concurrent operations
```

### Debugging Techniques

#### **Enable Debug Logging:**
```python
# Add to any scraper file
import logging
logging.basicConfig(level=logging.DEBUG)
```

#### **Check Browser Console:**
```python
# In scraper files, add:
page.on("console", lambda msg: print(f"Browser: {msg.text}"))
```

#### **Save Page HTML for Analysis:**
```python
# Add to scraper for debugging
with open('debug_page.html', 'w', encoding='utf-8') as f:
    f.write(page.content())
```

## 📈 Performance Optimization

### Production Settings

#### **For High-Volume Scraping:**
```python
# Recommended settings in scraper files:
headless=True                    # Faster execution
human_delay(2, 4)               # Conservative delays
job_limit=200                   # Moderate batch sizes
```

#### **For Development/Testing:**
```python
# Recommended settings:
headless=False                  # Visual monitoring
human_delay(1, 2)              # Faster testing
job_limit=10                   # Small batches
```

### Scheduled Scraping

#### **Daily Updates with Cron:**
```bash
# Add to crontab (crontab -e)
# Run daily at 2 AM
0 2 * * * cd /path/to/project && python seek_job_scraper_advanced.py 100
30 2 * * * cd /path/to/project && python jora_job_scraper_advanced.py 75
0 3 * * * cd /path/to/project && python jobserve_australia_scraper_advanced.py 20
30 3 * * * cd /path/to/project && python workforce_australia_scraper_advanced.py 50
```

#### **Weekly Full Scraping:**
```bash
# Weekly comprehensive scraping on Sundays at 1 AM
0 1 * * 0 cd /path/to/project && python seek_job_scraper_advanced.py 500
30 1 * * 0 cd /path/to/project && python jora_job_scraper_advanced.py 300
0 2 * * 0 cd /path/to/project && python jobserve_australia_scraper_advanced.py
30 2 * * 0 cd /path/to/project && python workforce_australia_scraper_advanced.py 200
```

## 🔐 Legal & Ethical Considerations

### Best Practices
- ✅ **Respect robots.txt** and terms of service
- ✅ **Use reasonable delays** (2+ seconds between requests)
- ✅ **Limit concurrent connections** (1 browser instance per site)
- ✅ **Monitor server response** and back off if needed
- ✅ **Use for personal/educational purposes** only

### Prohibited Uses
- ❌ **Bulk redistribution** of scraped data
- ❌ **Commercial resale** without permission
- ❌ **Aggressive scraping** that impacts site performance
- ❌ **Bypassing paywalls** or premium content

### Recommended Limits
- **Seek**: Max 500 jobs per session, 2+ hour gaps
- **Jora**: Max 300 jobs per session, 1+ hour gaps  
- **JobServe**: Max all available (typically < 100), daily frequency
- **Workforce Australia**: Max 200 jobs per session, 4+ hour gaps

## 📞 Support & Maintenance

### Regular Maintenance Tasks

#### **Weekly:**
- Check log files for errors
- Monitor database size
- Verify scraper success rates

#### **Monthly:**
- Update dependencies: `pip install -r requirements.txt --upgrade`
- Clean old log files
- Review anti-detection effectiveness

#### **As Needed:**
- Update selectors if websites change
- Adjust delays based on performance
- Add new job sites or categories

### Getting Help

#### **Error Diagnosis:**
1. Check relevant log file
2. Review Django error messages
3. Test with smaller job limits
4. Verify internet connectivity

#### **Performance Issues:**
1. Monitor system resources
2. Check network speed
3. Test headless vs visible mode
4. Review delay configurations

#### **Website Structure Changes:**
1. Check if selectors need updating
2. Compare with debug HTML output
3. Test individual data extraction functions
4. Update selectors in scraper files

---

## 🎉 Success Indicators

### Healthy Scraping Session:
```
✅ Jobs successfully scraped: 50+ 
✅ Duplicate jobs skipped: 5-15%
✅ Errors encountered: 0-2
✅ Success rate: 90%+
✅ No timeouts or crashes
```

### Concerning Patterns:
```
❌ Success rate below 80%
❌ High error counts (5+)
❌ Frequent timeouts
❌ No jobs found repeatedly
❌ Browser crashes
```

If you see concerning patterns, reduce scraping intensity and check for website updates or anti-bot measures.

---

## 🚀 Quick Start Summary

```bash
# 1. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 2. Setup database
python manage.py migrate
python manage.py createsuperuser

# 3. Test all scrapers
python seek_job_scraper_advanced.py 10 && \
python jora_job_scraper_advanced.py 10 && \
python jobserve_australia_scraper_advanced.py 5 && \
python workforce_australia_scraper_advanced.py 10

# 4. View results
python manage.py runserver
# Visit: http://127.0.0.1:8000/admin/
```

Happy multi-website scraping! 🌐✨