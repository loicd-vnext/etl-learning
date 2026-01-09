#!/bin/bash

# Docker Setup Script for ETL Pipeline Project
# This script sets up the entire project using Docker

set -e

echo "🐳 Setting up ETL Pipeline with Docker..."
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed (V2: docker compose)
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
# Database Configuration
DB_TYPE=postgresql
DB_HOST=postgres
DB_PORT=5432
DB_NAME=etl_demo
DB_USER=postgres
DB_PASSWORD=etl_password

# Pipeline Configuration
PIPELINE_LOG_LEVEL=INFO
PIPELINE_BATCH_SIZE=1000

# API Configuration
API_TIMEOUT=30
API_RETRY_ATTEMPTS=3
EOF
    echo -e "${GREEN}✓${NC} .env file created"
else
    echo -e "${BLUE}ℹ${NC} .env file already exists"
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p data/raw data/processed data/sample logs notebooks
echo -e "${GREEN}✓${NC} Directories created"

# Build Docker images
echo ""
echo "🔨 Building Docker images..."
docker compose build

# Start services
echo ""
echo "🚀 Starting services..."
docker compose up -d postgres

# Wait for PostgreSQL to be ready
echo ""
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 5
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} PostgreSQL is ready"
        break
    fi
    attempt=$((attempt + 1))
    echo -n "."
    sleep 1
done
echo ""

# Setup database schema
echo "🗄️  Setting up database schema..."
docker compose run --rm pipeline python scripts/setup_db.py
echo -e "${GREEN}✓${NC} Database schema created"

# Start all services
echo ""
echo "🚀 Starting all services..."
docker compose up -d

echo ""
echo -e "${GREEN}✅ Setup complete!${NC}"
echo ""
echo "📊 Services:"
echo "  - Dashboard: http://localhost:8501"
echo "  - Jupyter: http://localhost:8888"
echo "  - PostgreSQL: localhost:5432"
echo ""
echo "📝 Useful commands:"
echo "  - View logs: docker compose logs -f"
echo "  - Stop services: docker compose down"
echo "  - Restart services: docker compose restart"
echo "  - Run pipeline: docker compose run --rm pipeline python scripts/run_pipeline.py"
echo "  - Access database: docker compose exec postgres psql -U postgres -d etl_demo"
echo ""

