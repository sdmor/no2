# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for Fiona and GDAL
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    gcc \
    g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy only the requirements file for dependency installation
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project to the container
COPY . .

# Set environment variables to reduce Python output verbosity
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PORT=8080

# Optional: Set default credentials location (only if you won't mount the key file)
# ENV GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

# Expose the application port
EXPOSE 8080

# Add this line to copy the GeoJSON file into the container
COPY /geojson/ne_110m_admin_0_countries.geojson /app/geojson/

# Specify the main script to run by default
CMD ["python", "date_input.py"]
