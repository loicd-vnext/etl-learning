#!/bin/bash
# Script to setup database and run all tests

set -e  # Exit on error

echo "🚀 Setting up ETL Pipeline Test Environment"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << EOF
# Database Configuration
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_demo
DB_USER=postgres
DB_PASSWORD=etl_password

# Pipeline Configuration
PIPELINE_LOG_LEVEL=INFO
PIPELINE_BATCH_SIZE=1000
EOF
    echo "✅ .env file created"
else
    echo "✓ .env file already exists"
fi

# Check Docker
echo ""
echo "🐳 Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi
echo "✓ Docker is running"

# Setup PostgreSQL
echo ""
echo "🐘 Setting up PostgreSQL..."
if docker ps -a | grep -q etl-postgres; then
    echo "⚠️  PostgreSQL container already exists"
    if docker ps | grep -q etl-postgres; then
        echo "✓ Container is running"
    else
        echo "Starting container..."
        docker start etl-postgres
        sleep 5
    fi
else
    echo "Creating PostgreSQL container..."
    docker-compose up -d
    echo "Waiting for PostgreSQL to be ready..."
    sleep 10
fi

# Check if PostgreSQL is ready
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec etl-postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "   Attempt $attempt/$max_attempts..."
    sleep 1
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ PostgreSQL failed to start"
    exit 1
fi

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
if ! python -c "import sqlalchemy" 2>/dev/null; then
    echo "Installing required packages..."
    pip install -q sqlalchemy psycopg2-binary pandas python-dotenv pyyaml requests
    echo "✅ Dependencies installed"
else
    echo "✓ Dependencies already installed"
fi

# Setup database schema
echo ""
echo "🗄️  Setting up database schema..."
python scripts/setup_db.py
echo "✅ Database schema created"

# Run tests
echo ""
echo "🧪 Running all phase tests..."
echo ""
python scripts/test_all_phases.py

echo ""
echo "🎉 Setup and testing completed!"

