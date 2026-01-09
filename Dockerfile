# Multi-stage Dockerfile for ETL Pipeline Project
# Stage 1: Base image with Python and dependencies
FROM python:3.12-slim as base

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Stage 2: Production image
FROM base as production

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p logs data/raw data/processed data/sample

# Expose ports
# 8501 for Streamlit dashboard
# 8888 for Jupyter notebook
EXPOSE 8501 8888

# Default command (can be overridden in docker-compose)
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]

# Stage 3: Development image (includes Jupyter)
FROM base as development

# Install additional dev dependencies
RUN pip install jupyter jupyterlab ipykernel

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p logs data/raw data/processed data/sample notebooks

# Expose ports
EXPOSE 8501 8888

# Default command for development
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]

