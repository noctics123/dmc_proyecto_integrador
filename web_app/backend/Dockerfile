FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create static directory for frontend files
RUN mkdir -p static

# Copy frontend build files (will be populated by cloudbuild)
COPY frontend-build ./temp-frontend/
RUN if [ -f "./temp-frontend/.placeholder" ]; then \
      echo "No frontend build files found - creating placeholder index.html"; \
      echo '<html><body><h1>API Only Mode</h1><p>Frontend will be available after build</p></body></html>' > ./static/index.html; \
    else \
      cp -r ./temp-frontend/* ./static/ && \
      echo "Frontend files copied to static directory"; \
    fi && \
    rm -rf ./temp-frontend

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]