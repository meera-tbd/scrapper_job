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

# System libraries required by Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgobject-2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libexpat1 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libxshmfence1 \
    libxcb1 \
    libxkbcommon0 \
    libdrm2 \
    libgbm1 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libgio-2.0-0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcups2 \
    libxext6 \
    libxtst6 \
    libgtk-3-0 \
    libasound2 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser (skip OS deps to avoid build failure on Debian variants)
RUN python -m playwright install chromium || true

# Copy project files
COPY . .

# Expose port
EXPOSE 8000

# Default command
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

# [cursor:reason] Set default timezone in image; can be overridden by compose env
ENV TZ=Asia/Kolkata
