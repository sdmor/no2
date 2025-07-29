FROM python:3.10

WORKDIR /app

# Install system dependencies
RUN apt-get update \
 && apt-get install -y --allow-unauthenticated gcc \
 && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY compute_ra.py .
COPY compute_ra_single_day.py .
