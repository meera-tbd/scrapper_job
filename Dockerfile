# Simple Dockerfile for Job Scraper Project
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install minimal tools required; Playwright will install its own deps
# [cursor:reason] Install tzdata to support TZ=Asia/Kolkata
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser with system dependencies
RUN playwright install --with-deps chromium || (apt-get update && playwright install --with-deps chromium)

# Copy project files
COPY . .

# Expose port
EXPOSE 8000

# Default command
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

# [cursor:reason] Set default timezone in image; can be overridden by compose env
ENV TZ=Asia/Kolkata
