# Job Scraper API Documentation

This document provides comprehensive information about the REST API endpoints for accessing job data from the database.

## Base URL
```
http://localhost:8000/api/
```

## Authentication
Currently, the API is open and doesn't require authentication. For production use, consider adding authentication.

## Response Format
All responses are in JSON format. List endpoints return data directly as an array:
```json
[
    {
        "id": 1,
        "title": "Job Title",
        ...
    },
    {
        "id": 2,
        "title": "Another Job",
        ...
    }
]
```

## API Root
- **GET** `/api/`
- **Description**: Get links to all available API endpoints
- **Response**:
```json
{
    "jobs": "http://localhost:8000/api/jobs/",
    "companies": "http://localhost:8000/api/companies/",
    "locations": "http://localhost:8000/api/locations/",
    "external_sources": "http://localhost:8000/api/jobs/external_sources/",
    "jobs_from_seek": "http://localhost:8000/api/jobs/?external_source=seek.com.au"
}
```

## API Endpoints

### 1. Jobs API (`/api/jobs/`)

#### List Jobs
- **GET** `/api/jobs/`
- **Description**: Get all job postings with essential fields and clean nested company/location data
- **Fields**: `id`, `company`, `location`, `posted_by`, `salary_display`, `title`, `slug`, `description`, `job_category`, `job_type`, `salary_raw_text`, `external_source`, `external_url`, `status`, `posted_ago`, `date_posted`, `scraped_at`, `updated_at`
- **Query Parameters**:
  - `search`: Search in title, description, company name, location, tags
  - `external_source`: Filter by source website (ONLY custom filter available)
  - `status`: Filter by status (default: active only)
  - `ordering`: Sort by field (prefix with `-` for descending)

**Available ordering fields**: `title`, `scraped_at`, `date_posted`, `salary_min`, `salary_max`, `company__name`, `location__name`

**Example Requests**:
```bash
# Get all active jobs
GET /api/jobs/

# Search for Python jobs
GET /api/jobs/?search=python

# Filter by external source
GET /api/jobs/?external_source=seek.com.au

# Sort by salary (descending)
GET /api/jobs/?ordering=-salary_min

# Combine filters
GET /api/jobs/?search=developer&external_source=jora&ordering=-scraped_at
```

#### Get Job Details
- **GET** `/api/jobs/{id}/`
- **Description**: Get detailed information about a specific job with full nested data

#### External Sources
- **GET** `/api/jobs/external_sources/`
- **Description**: Get all external job sources with job counts
- **Response**:
```json
[
    {
        "external_source": "seek.com.au",
        "job_count": 150,
        "active_jobs": 150
    },
    {
        "external_source": "jora_au",
        "job_count": 54,
        "active_jobs": 54
    }
]
```

### 2. Companies API (`/api/companies/`)

#### List Companies
- **GET** `/api/companies/`
- **Description**: Get all companies with essential fields only (optimized for performance)
- **Fields**: `id`, `name`, `slug`, `description`, `created_at`, `updated_at`
- **Query Parameters**:
  - `search`: Search in company name and description
  - `ordering`: Sort by field (available: `name`, `created_at`, `updated_at`)

**Example Requests**:
```bash
# Get all companies
GET /api/companies/

# Search for companies
GET /api/companies/?search=microsoft

# Sort by name
GET /api/companies/?ordering=name
```

#### Get Company Details
- **GET** `/api/companies/{id}/`
- **Description**: Get detailed company information with all fields including `website`, `company_size`, and `logo`

### 3. Locations API (`/api/locations/`)

#### List Locations
- **GET** `/api/locations/`
- **Description**: Get all locations with full details
- **Query Parameters**:
  - `search`: Search in location name, city, state, country
  - `ordering`: Sort by field (available: `name`, `city`, `state`, `country`, `created_at`, `updated_at`)

**Example Requests**:
```bash
# Get all locations
GET /api/locations/

# Search for locations
GET /api/locations/?search=melbourne

# Sort by state and city
GET /api/locations/?ordering=state,city
```

#### Get Location Details
- **GET** `/api/locations/{id}/`
- **Description**: Get detailed location information

## Data Models

### JobPosting Fields (Essential Fields with Clean Nested Data)
```json
{
    "id": 30,
    "company": {
        "id": 31,
        "name": "Animo",
        "slug": "animo",
        "description": "Animo - Jobs from Seek.com.au",
        "created_at": "2025-08-08T23:32:53.000526+10:00",
        "updated_at": "2025-08-08T23:32:53.000526+10:00"
    },
    "location": null,
    "posted_by": "seek_scraper_system",
    "salary_display": "$44.80 per hour",
    "title": "NDIS Mental Health Support Worker",
    "slug": "ndis-mental-health-support-worker",
    "description": "Hiring mental health support workers to support NDIS participants living with mental illness. Great hourly rate and high levels of autonomy.",
    "job_category": "healthcare",
    "job_type": "full_time",
    "salary_raw_text": "$44.80 per hour",
    "external_source": "seek.com.au",
    "external_url": "https://www.seek.com.au/job/80625079?type=standard&ref=search-standalone&origin=cardTitle#sol=65dca5149a6eb87dbf5a29302872b27f7eb314c3",
    "status": "active",
    "posted_ago": "11d ago",
    "date_posted": null,
    "scraped_at": "2025-08-08T23:32:53.005490+10:00",
    "updated_at": "2025-08-08T23:32:53.005490+10:00"
}
```

### Company Fields (List API - Essential Fields Only)
```json
{
    "id": 1,
    "name": "Tech Company",
    "slug": "tech-company",
    "description": "Company description...",
    "created_at": "2025-01-15T10:00:00Z",
    "updated_at": "2025-01-15T10:00:00Z"
}
```

### Company Fields (Detail API - All Fields)
```json
{
    "id": 1,
    "name": "Tech Company",
    "slug": "tech-company",
    "description": "Company description...",
    "website": "https://techcompany.com",
    "company_size": "medium",
    "logo": "https://logo-url.com",
    "created_at": "2025-01-15T10:00:00Z",
    "updated_at": "2025-01-15T10:00:00Z"
}
```

### Location Fields
```json
{
    "id": 1,
    "name": "Melbourne, VIC",
    "city": "Melbourne",
    "state": "VIC",
    "country": "Australia",
    "created_at": "2025-01-15T10:00:00Z",
    "updated_at": "2025-01-15T10:00:00Z"
}
```

## Filter Options

### Job Categories
- `technology`, `finance`, `healthcare`, `marketing`, `sales`, `hr`, `education`, `retail`, `hospitality`, `construction`, `manufacturing`, `consulting`, `legal`, `office_support`, `drivers_operators`, `technical_engineering`, `production_workers`, `transport_logistics`, `mining_resources`, `sales_marketing`, `executive`, `other`

### Job Types
- `full_time`, `part_time`, `casual`, `contract`, `temporary`, `permanent`, `internship`, `freelance`

### Company Sizes
- `startup` (1-10), `small` (11-50), `medium` (51-200), `large` (201-1000), `enterprise` (1000+)

### Job Status
- `active`, `inactive`, `expired`, `filled`

## Error Responses

### 404 Not Found
```json
{
    "detail": "Not found."
}
```

### 400 Bad Request
```json
{
    "field_name": ["This field is required."]
}
```

## Rate Limiting
Currently, no rate limiting is implemented. For production use, consider adding rate limiting.

## Examples

### Get Technology Jobs from Seek
```bash
GET /api/jobs/?search=technology&external_source=seek.com.au
```

### Get All External Sources
```bash
GET /api/jobs/external_sources/
```

### Search for Remote Python Jobs
```bash
GET /api/jobs/?search=python remote
```

### Get Companies with Tech in Name
```bash
GET /api/companies/?search=tech
```

### Get Melbourne Locations
```bash
GET /api/locations/?search=melbourne
```

## Key Features

### ✅ Complete Data Access
- All APIs return complete data with nested relationships
- No pagination - get all data in single requests
- Full field access with `depth=1` for related objects

### ✅ Focused Filtering
- Jobs API: Only `external_source` custom filter (clean and focused)
- Built-in search and ordering on all endpoints
- Status filtering (defaults to active jobs only)

### ✅ External Sources Integration
- Dedicated endpoint to list all job sources
- Direct filtering by external source
- Job counts per source

### ✅ Clean Data Models
- **Jobs List**: Essential job fields with clean nested company/location data
- **Companies List**: Essential fields only (`id`, `name`, `slug`, `description`, timestamps)
- **Companies Detail**: All fields including `website`, `company_size`, `logo`
- **Locations**: Essential location information only
- **Nested Objects**: All nested company/location objects use essential fields only

This API provides comprehensive access to all job data with clean, focused filtering and complete data relationships. The browsable API interface is also available at `http://localhost:8000/api/` for interactive exploration.