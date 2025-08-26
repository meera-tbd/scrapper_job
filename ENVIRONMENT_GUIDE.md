# Docker vs Local Environment Guide

## 🔄 **Shared Database Configuration**
Both Docker and Local environments now use the **SAME PostgreSQL database** so all your data is synchronized!

## 🐳 **Docker Environment**
- **Port:** 8001
- **Database:** Your Local PostgreSQL (shared)
- **Access URLs:**
  - Admin: http://localhost:8001/admin/
  - API: http://localhost:8001/api/jobs/
  - Job Postings: http://localhost:8001/admin/jobs/jobposting/

### Start Docker:
```bash
docker compose up -d
```

### Stop Docker:
```bash
docker compose down
```

### Run Scripts in Docker:
```bash
docker compose exec web python script/jora_job_scraper_advanced.py 5
```

---

## 💻 **Local Environment**
- **Port:** 8000
- **Database:** Your Local PostgreSQL (shared)
- **Access URLs:**
  - Admin: http://localhost:8000/admin/
  - API: http://localhost:8000/api/jobs/
  - Job Postings: http://localhost:8000/admin/jobs/jobposting/

### Start Local:
```bash
python manage.py runserver
```

### Run Scripts Locally:
```bash
python script/jora_job_scraper_advanced.py 5
```

---

## 🔧 **Configuration Summary**

| Aspect | Docker | Local |
|--------|--------|--------|
| Django Port | 8001 | 8000 |
| Database | **Shared Local PostgreSQL (port 5432)** | **Shared Local PostgreSQL (port 5432)** |
| Database Name | australia_job_scraper | australia_job_scraper |
| Data Access | **Same data as Local** | **Same data as Docker** |

---

## ✅ **Benefits of Shared Database**

1. **No Port Conflicts:** Docker (8001) and Local (8000) use different ports
2. **Shared Data:** Both environments access the same job data
3. **Easy Switching:** Switch between environments without losing data
4. **Synchronized:** Jobs scraped in Docker appear in Local and vice versa

---

## 🚨 **Important Rules**

- **⚠️ Only run ONE environment at a time** to avoid port conflicts
- **✅ Same data:** Both environments share the same database
- **🔄 Synchronization:** Data added in one environment appears in the other
- **🚫 No conflicts:** Different ports prevent running both simultaneously
