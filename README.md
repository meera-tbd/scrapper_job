# 🌐 Multi-Website Job Scraper Suite





























A comprehensive Django + Playwright web scraper collection that extracts job listings from **Seek.com.au**, **Jora Australia**, **JobServe Australia**, and **Workforce Australia** with human-like behavior to avoid detection. Features professional database structure with automatic job categorization and duplicate prevention.

## 🎯 Supported Job Sites

### 🇦🇺 **Seek.com.au** 
- Australia's #1 job site
- Comprehensive job data extraction
- Location-specific scraping
- High volume: 1000+ jobs per session

### 🔍 **Jora Australia**
- Popular Australian job aggregator  
- Enhanced job categorization
- Multi-source job listings
- Fast scraping: 500+ jobs per session

### 💼 **JobServe Australia**
- Professional IT and healthcare focus
- International job opportunities
- Specialized recruitment platform
- Quality focus: 20-100 targeted jobs

### 🏛️ **Workforce Australia**
- Government employment portal
- Official Australian government jobs
- Public sector opportunities
- Comprehensive categories: 100-500 jobs

## ✨ Features

🤖 **Human-like Behavior**
- Visible browser option for monitoring
- Random delays between actions (2-5 seconds)
- Natural scrolling and interaction patterns
- Advanced anti-detection measures
- Rotating user agents and stealth scripts

📊 **Complete Data Extraction**
- Job title, company name, location
- Salary ranges with multi-currency parsing (AUD, USD, GBP, EUR)
- Job type, work mode, experience level
- Job descriptions and summaries
- Automatic job categorization (technology, healthcare, finance, etc.)
- Tags, keywords, and posting dates
- External job URLs for reference

🗃️ **Professional Database Design**
- `JobPosting` model for complete job information
- `Company` model with enhanced company data
- `Location` model for geographical data
- Enhanced duplicate prevention (URL + title+company)
- Thread-safe database operations
- Django admin panel integration

⚡ **Advanced Automation**
- Playwright browser automation
- Dynamic content handling
- Handles dynamic loading and lazy content
- Robust error handling and recovery
- Configurable job limits for testing

🛡️ **Enhanced Anti-Detection**
- Advanced stealth browser configurations
- Human-like browsing patterns
- Cookie handling and CAPTCHA avoidance
- Comprehensive error recovery

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Setup Django Database
```bash
python manage.py makemigrations
python manage.py migrate
python manage.py createsuperuser
```

### 3. Run the Scrapers

#### **Seek.com.au Scraper**
```bash
# Scrape 50 jobs from Seek Australia
python seek_job_scraper_advanced.py 50

# Scrape all available jobs
python seek_job_scraper_advanced.py
```

#### **Jora Australia Scraper**
```bash
# Scrape 100 jobs from Jora
python jora_job_scraper_advanced.py 100

# Scrape all available jobs
python jora_job_scraper_advanced.py
```

#### **JobServe Australia Scraper**
```bash
# Scrape 30 jobs from JobServe Australia
python jobserve_australia_scraper_advanced.py 30

# Scrape all available jobs
python jobserve_australia_scraper_advanced.py
```

#### **Workforce Australia Scraper**
```bash
# Scrape 25 jobs from Workforce Australia
python workforce_australia_scraper_advanced.py 25

# Scrape all available jobs
python workforce_australia_scraper_advanced.py
```

### 4. View Results
```bash
# Start Django development server
python manage.py runserver

# Open browser and navigate to:
# http://127.0.0.1:8000/admin/
```

## 📁 Project Structure

```
australia_job_scraper/
├── manage.py                           # Django management script
├── requirements.txt                    # Python dependencies
├── setup_instructions.md              # Detailed setup guide
├── README.md                          # This file
├── 
├── # SCRAPERS
├── seek_job_scraper_advanced.py       # Seek.com.au scraper
├── jora_job_scraper_advanced.py       # Jora Australia scraper  
├── jobserve_australia_scraper_advanced.py # JobServe Australia scraper
├── workforce_australia_scraper_advanced.py # Workforce Australia scraper
├── adzuna_australia_scraper.py        # Adzuna Australia scraper
├──
├── # DJANGO PROJECT
├── australia_job_scraper/             # Django project settings
│   ├── __init__.py
│   ├── settings.py                    # Django configuration
│   ├── urls.py                       # URL routing
│   ├── wsgi.py                       # WSGI configuration
│   └── asgi.py                       # ASGI configuration
├──
├── # DJANGO APPS
├── apps/
│   ├── companies/                     # Company management app
│   │   ├── models.py                 # Company model
│   │   ├── admin.py                  # Company admin interface
│   │   └── migrations/               # Company database migrations
│   ├── core/                         # Core location app
│   │   ├── models.py                 # Location model
│   │   ├── admin.py                  # Location admin interface
│   │   └── migrations/               # Location database migrations
│   └── jobs/                         # Jobs management app
│       ├── models.py                 # JobPosting model
│       ├── services.py               # Job categorization service
│       ├── admin.py                  # Jobs admin interface
│       ├── views.py                  # Web views
│       ├── urls.py                   # App URL routing
│       ├── migrations/               # Job database migrations
│       └── templates/                # HTML templates
└──
└── # LOGS & DATA
    ├── *.log                         # Scraper log files
    └── db.sqlite3                    # SQLite database
```

## 🗄️ Database Models

### JobPosting
```python
- title: CharField (job title)
- slug: SlugField (unique URL-friendly identifier)
- description: TextField (job description)
- company: ForeignKey to Company
- location: ForeignKey to Location (nullable)
- posted_by: ForeignKey to User
- job_category: CharField (auto-categorized)
- job_type: CharField (full_time, part_time, contract, etc.)
- work_mode: CharField (remote, hybrid, onsite)
- experience_level: CharField (junior, mid, senior)
- salary_min/max: DecimalField (salary range)
- salary_currency: CharField (AUD, USD, GBP, EUR)
- salary_type: CharField (hourly, daily, weekly, monthly, yearly)
- external_source: CharField (seek.com.au, jora_au, jobserve_australia, workforce_australia)
- external_url: URLField (original job URL)
- status: CharField (active, inactive, expired)
- date_posted: DateField (parsed posting date)
- posted_ago: CharField (relative date string)
- tags: TextField (comma-separated keywords)
- additional_info: JSONField (extra scraped data)
```

### Company
```python
- name: CharField (company name)
- slug: SlugField (unique identifier)
- description: TextField (company description)
- website: URLField (company website)
- company_size: CharField (startup, small, medium, large, enterprise)
- industry: CharField (company industry)
- created_at/updated_at: DateTimeField (timestamps)
```

### Location
```python
- name: CharField (full location name)
- city: CharField (city name)
- state: CharField (state/province)
- country: CharField (country)
- created_at/updated_at: DateTimeField (timestamps)
- Unique constraint on name
```

## 📊 Monitoring & Logging

### Scraper Settings
```python
# Job limits for testing
python seek_job_scraper_advanced.py 10        # Scrape 10 jobs
python jora_job_scraper_advanced.py 50        # Scrape 50 jobs
python jobserve_australia_scraper_advanced.py 25  # Scrape 25 jobs
python workforce_australia_scraper_advanced.py 15  # Scrape 15 jobs

# Headless mode (edit in scraper files)
headless=True   # No visible browser
headless=False  # Visible browser for monitoring
```

### Real-time Monitoring

- **Console Output**: Real-time progress and statistics
- **Individual Log Files**: 
  - `scraper_professional.log` (Seek)
  - `jora_scraper_professional.log` (Jora)
  - `jobserve_australia_scraper.log` (JobServe)
  - `workforce_australia_scraper.log` (Workforce Australia)
- **Admin Panel**: Browse and filter scraped job data
- **Statistics**: Track scraped, duplicate, and error counts per site

## 🔧 Advanced Usage

### Run Multiple Scrapers Sequentially
```bash
# Scrape from all four sites
python seek_job_scraper_advanced.py 50 && \
python jora_job_scraper_advanced.py 50 && \
python jobserve_australia_scraper_advanced.py 20 && \
python workforce_australia_scraper_advanced.py 25
```

### Database Queries
```python
# Django shell examples
python manage.py shell

from apps.jobs.models import JobPosting
from apps.companies.models import Company
from apps.core.models import Location

# Get all remote jobs
remote_jobs = JobPosting.objects.filter(work_mode='remote')

# Get jobs by salary range (above $100k AUD)
high_salary_jobs = JobPosting.objects.filter(
    salary_min__gte=100000,
    salary_currency='AUD'
)

# Get jobs by source
seek_jobs = JobPosting.objects.filter(external_source='seek.com.au')
jora_jobs = JobPosting.objects.filter(external_source='jora_au')
jobserve_jobs = JobPosting.objects.filter(external_source='jobserve_australia')
workforce_jobs = JobPosting.objects.filter(external_source='workforce_australia')

# Get jobs by category
tech_jobs = JobPosting.objects.filter(job_category='technology')
healthcare_jobs = JobPosting.objects.filter(job_category='healthcare')

# Get jobs by location
sydney_jobs = JobPosting.objects.filter(location__city__icontains='sydney')
melbourne_jobs = JobPosting.objects.filter(location__city__icontains='melbourne')

# Get companies with most job postings
from django.db.models import Count
top_companies = Company.objects.annotate(
    job_count=Count('jobposting')
).order_by('-job_count')[:10]
```

### Custom Job Categories
The scrapers automatically categorize jobs into:
- `technology` - IT, software, engineering
- `healthcare` - medical, nursing, healthcare
- `finance` - banking, accounting, finance
- `education` - teaching, training, education
- `sales` - sales, marketing, business development
- `management` - management, leadership roles
- `other` - all other job types

## 🌟 Scraper Comparison

| Feature | Seek | Jora | JobServe | Workforce AU |
|---------|------|------|----------|--------------|
| **Job Volume** | High (1000+) | High (500+) | Low-Medium (20-100) | Medium (100-500) |
| **Job Quality** | Excellent | Good | Excellent | Excellent |
| **Industries** | All | All | IT/Healthcare Focus | Government/Public |
| **Salary Data** | Extensive | Limited | Good | Government Grades |
| **Location Data** | Detailed | Good | Basic | Detailed |
| **Anti-Detection** | Advanced | Advanced | Advanced | Conservative |
| **Success Rate** | 95%+ | 90%+ | 100% | 85%+ |

## 🛡️ Anti-Detection Features

### Advanced Stealth Measures
- **Browser Fingerprinting**: Realistic browser configurations
- **User Agent Rotation**: Multiple authentic user agents
- **Human Timing**: Variable delays (2-8 seconds)
- **Natural Scrolling**: Human-like page interaction
- **Cookie Management**: Automatic cookie acceptance
- **Network Patterns**: Realistic request timing

### Error Recovery
- **Connection Timeouts**: Automatic retry with backoff
- **Page Load Failures**: Graceful fallback strategies
- **Element Detection**: Multiple selector strategies
- **Data Validation**: Comprehensive data cleaning

## ⚠️ Legal & Ethical Considerations

- **Terms of Service**: Respect each website's terms of service
- **Rate Limiting**: Use appropriate delays to avoid overloading servers
- **Personal Use**: Intended for educational and personal projects
- **Data Privacy**: Don't redistribute scraped data without permission

## 🐛 Troubleshooting

### Common Issues
1. **"No module named 'django'"**: Run `pip install -r requirements.txt`
2. **"playwright not found"**: Run `playwright install chromium`
3. **Database Errors**: Ensure migrations are applied
4. **Import Errors**: Verify all dependencies are installed
5. **Timeout Errors**: Increase timeout values in scraper settings

### Debug Mode
```python
# Enable detailed logging in any scraper
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Site-Specific Issues
- **Seek**: May require CAPTCHA solving during peak hours
- **Jora**: Can have rate limiting on rapid requests
- **JobServe**: Limited job volume but high success rate
- **Workforce Australia**: Government maintenance periods, slower loading

## 📊 Example Output

```
🇦🇺 Professional Multi-Website Job Scraper
=======================================================
Target: 30 jobs per site
Database: Professional structure with enhanced duplicate detection
Features: Human-like behavior, anti-detection, robust parsing
=======================================================

🔍 SEEK.COM.AU SCRAPER
Starting Seek Australia job scraper...
Scraping page 1...
Found 20 job listings on current page
✓ Saved job: Senior Python Developer at TechCorp
  Category: technology
  Location: Sydney, New South Wales
  Salary: AUD 120,000 - 150,000 per yearly

✓ Saved job: Registered Nurse at Sydney Health
  Category: healthcare
  Location: Sydney, New South Wales
  Salary: AUD 75,000 - 85,000 per yearly

=======================================================
🎉 SEEK SCRAPING COMPLETED!
=======================================================
📊 Pages scraped: 3
✅ Jobs successfully scraped: 50
🔄 Duplicate jobs skipped: 5
❌ Errors encountered: 0
💾 Total Seek jobs in database: 1247
📈 Success rate: 100.0%
=======================================================

💼 JOBSERVE AUSTRALIA SCRAPER
Starting JobServe Australia job scraper...
Found 6 job listings using selector: h3
✓ Saved job: Travel Labor & Delivery Registered Nurse at Fusion Medical
  Category: healthcare
  Salary: USD 2,350 per weekly

=======================================================
🎉 JOBSERVE AUSTRALIA SCRAPING COMPLETED!
=======================================================
📊 Pages scraped: 1
✅ Jobs successfully scraped: 6
🔄 Duplicate jobs skipped: 0
❌ Errors encountered: 0
💾 Total JobServe Australia jobs in database: 15
📈 Success rate: 100.0%
=======================================================

🏛️ WORKFORCE AUSTRALIA SCRAPER
Starting Workforce Australia job scraper...
Found 20 job containers from JobServe jobid links
✓ Saved job: Software Engineer at Australian Government
  Category: technology
  Location: Canberra, Australian Capital Territory

=======================================================
🎉 WORKFORCE AUSTRALIA SCRAPING COMPLETED!
=======================================================
📊 Pages scraped: 1
✅ Jobs successfully scraped: 20
🔄 Duplicate jobs skipped: 8
❌ Errors encountered: 0
💾 Total Workforce Australia jobs in database: 45
📈 Success rate: 71.4%
=======================================================
```

## 🤝 Contributing

Feel free to submit issues, feature requests, or pull requests to improve any of the scrapers!

### Development Priorities
1. **New Job Sites**: Add more Australian job sites
2. **Enhanced Categorization**: Improve automatic job categorization
3. **Data Enrichment**: Add company information lookup
4. **Real-time Monitoring**: Dashboard for scraping statistics
5. **API Integration**: RESTful API for job data access

## 📄 License

This project is for educational purposes. Please respect website terms of service and use responsibly.

---

## 🎉 Recent Updates

### v3.0.0 - Complete Multi-Website Support
- ✅ Added Jora Australia scraper
- ✅ Added JobServe Australia scraper
- ✅ Added Workforce Australia scraper
- ✅ Enhanced database structure
- ✅ Improved duplicate detection
- ✅ Advanced anti-detection measures
- ✅ Automatic job categorization
- ✅ Professional logging and monitoring
- ✅ Comprehensive admin interface

### v2.0.0 - Enhanced Seek Scraper
- ✅ Professional database design
- ✅ Advanced anti-detection
- ✅ Comprehensive data extraction

### v1.0.0 - Initial Release
- ✅ Basic Seek.com.au scraper
- ✅ Simple database structure
- ✅ Django admin integration

---

## 🚀 Quick Multi-Site Test

```bash
# Test all scrapers with small limits
python seek_job_scraper_advanced.py 10 && \
python jora_job_scraper_advanced.py 10 && \
python jobserve_australia_scraper_advanced.py 5 && \
python workforce_australia_scraper_advanced.py 10

# Check results in admin panel
python manage.py runserver
# Visit: http://127.0.0.1:8000/admin/
```

Happy multi-website scraping! 🌐✨