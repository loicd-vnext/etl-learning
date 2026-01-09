#!/bin/bash
# Script to setup PostgreSQL using Docker

echo "🚀 Setting up PostgreSQL with Docker..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if container already exists
if docker ps -a | grep -q etl-postgres; then
    echo "⚠️  PostgreSQL container already exists."
    read -p "Do you want to remove and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Removing existing container..."
        docker stop etl-postgres 2>/dev/null
        docker rm etl-postgres 2>/dev/null
    else
        echo "Starting existing container..."
        docker start etl-postgres
        exit 0
    fi
fi

# Start PostgreSQL container
echo "🐘 Starting PostgreSQL container..."
docker-compose up -d postgres

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 5

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
    echo "❌ PostgreSQL failed to start. Check logs with: docker logs etl-postgres"
    exit 1
fi

# Create .env file if it doesn't exist
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

# API Configuration
API_TIMEOUT=30
API_RETRY_ATTEMPTS=3
EOF
    echo "✅ .env file created!"
else
    echo "⚠️  .env file already exists. Please update it manually with PostgreSQL credentials."
fi

echo ""
echo "🎉 PostgreSQL setup complete!"
echo ""
echo "Connection details:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: etl_demo"
echo "  User: postgres"
echo "  Password: etl_password"
echo ""
echo "Next steps:"
echo "  1. Run: python scripts/setup_db.py"
echo "  2. Run: python scripts/test_setup.py"
echo ""

