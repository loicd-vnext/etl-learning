# 🚀 ETL Pipeline Demo Project

Dự án demo đơn giản để học Data Engineering - ETL Pipeline với Docker

## 📖 Tổng Quan

Đây là một dự án demo hoàn chỉnh để hiểu về:

- **ETL Pipeline**: Extract, Transform, Load
- **Data Processing**: Làm sạch, validate, enrich dữ liệu
- **Data Storage**: Data Warehouse (PostgreSQL) và Data Lake (file system)
- **Orchestration**: Quản lý workflow và scheduling
- **Visualization**: Dashboard và BI tools
- **Containerization**: Docker setup cho toàn bộ project

### Use Case: E-commerce Sales Pipeline

Xử lý dữ liệu bán hàng từ nhiều nguồn:
- **Orders CSV**: Thông tin đơn hàng
- **Customers JSON**: Thông tin khách hàng
- **Products JSON**: Thông tin sản phẩm

Pipeline sẽ:
1. Extract dữ liệu từ CSV, JSON
2. Validate và clean data
3. Transform (join, enrich, calculate revenue)
4. Load vào PostgreSQL (Data Warehouse) và Parquet files (Data Lake)
5. Visualize trên Dashboard

## 🏗️ Kiến Trúc

```
┌─────────────┐
│ Data Source │  (CSV, JSON files)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Extract   │  (Trích xuất dữ liệu)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Transform/   │  (Validate, Clean, Enrich)
│  Process   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Load     │  (Data Warehouse + Data Lake)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Analytics  │  (Dashboard, Notebooks, Reports)
└─────────────┘
```

## 📁 Cấu Trúc Project

```
etl/
├── README.md                 # File này
├── requirements.txt          # Python dependencies
├── Dockerfile                # Docker image definition
├── docker-compose.yml        # Docker services orchestration (with live code mounting)
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore rules
│
├── src/                      # Source code
│   ├── extract/              # Data extraction
│   │   ├── csv_extractor.py
│   │   ├── json_extractor.py
│   │   └── api_extractor.py
│   ├── transform/            # Data transformation
│   │   ├── validator.py      # Data validation
│   │   ├── cleaner.py        # Data cleaning
│   │   └── transformer.py    # Data transformation
│   ├── load/                 # Data loading
│   │   └── loader.py         # Database & file loader
│   ├── utils/                # Utilities
│   │   ├── database.py       # Database connection
│   │   └── logger.py         # Logging
│   ├── pipeline.py           # Main ETL pipeline
│   └── dashboard.py          # Dashboard backend
│
├── config/                   # Configuration files
│   └── config.yaml           # Pipeline configuration
│
├── data/                     # Data storage
│   ├── raw/                  # Raw data (from sources)
│   ├── processed/           # Processed data (after transform)
│   └── sample/              # Sample data for testing
│
├── scripts/                  # Utility scripts
│   ├── setup_db.py          # Database schema setup
│   ├── run_pipeline.py      # Pipeline runner
│   ├── docker_setup.sh      # Docker setup script
│   └── run_dashboard.sh      # Dashboard runner
│
├── notebooks/                # Jupyter notebooks
│   ├── analysis.ipynb       # Data analysis
│   └── data_exploration.ipynb
│
├── logs/                     # Log files (gitignored)
│
└── tests/                   # Unit tests
```

## 🚀 Quick Start

### Prerequisites

- **Docker** và **Docker Compose** (V2) - Bắt buộc
- Git (optional)

**⚠️ Lưu ý**: Project này sử dụng Docker để chạy tất cả services. Không cần cài đặt Python, PostgreSQL trên máy local.

### Option 1: Docker Setup (Recommended) 🐳

Tất cả services (PostgreSQL, Dashboard, Jupyter) chạy trong Docker containers.

#### 1. Clone repository (nếu có)

```bash
git clone <repository-url>
cd etl
```

#### 2. Setup tự động

```bash
# Make script executable
chmod +x scripts/docker_setup.sh

# Run setup script (sẽ tự động setup tất cả)
./scripts/docker_setup.sh
```

Script này sẽ:
- ✅ Tạo `.env` file nếu chưa có
- ✅ Build Docker images
- ✅ Start PostgreSQL và setup database schema
- ✅ Start tất cả services (Dashboard, Jupyter)

#### 3. Access services

Sau khi setup xong:

- **Dashboard**: http://localhost:8501
- **Jupyter Lab**: http://localhost:8888
- **PostgreSQL**: localhost:5432

#### 4. Run Pipeline

```bash
# Chạy pipeline từ Dashboard (recommended)
# Mở http://localhost:8501 → Chọn "🚀 Run Pipeline"

# Hoặc chạy từ command line
docker compose run --rm pipeline python scripts/run_pipeline.py
```

### Option 2: Local Development

Nếu muốn chạy trên máy local (không dùng Docker):

#### 1. Setup Python environment

```bash
# Tạo virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
# hoặc venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

#### 2. Setup PostgreSQL với Docker

```bash
# Start PostgreSQL container
docker compose up -d postgres

# Setup database schema
python scripts/setup_db.py
```

#### 3. Create `.env` file

```bash
# Copy template
cp .env.example .env

# Edit .env và update DB_HOST=localhost (thay vì postgres)
```

#### 4. Run services

```bash
# Run dashboard
streamlit run dashboard.py

# Run Jupyter (in another terminal)
jupyter lab
```

## 🔨 Build

### Build Docker Images

```bash
# Build all services
docker compose build

# Build specific service
docker compose build dashboard
docker compose build jupyter

# Build without cache
docker compose build --no-cache
```

**Lưu ý**: Docker compose đã được cấu hình với live code mounting, code changes sẽ reflect ngay lập tức mà không cần rebuild!


## ▶️ Chạy Services

### Start All Services

```bash
# Start tất cả services
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down
```

### Start Individual Services

```bash
# Start PostgreSQL only
docker compose up -d postgres

# Start Dashboard
docker compose up -d dashboard
# Access: http://localhost:8501

# Start Jupyter
docker compose up -d jupyter
# Access: http://localhost:8888
```

### Run Pipeline

```bash
# Run pipeline manually
docker compose run --rm pipeline python scripts/run_pipeline.py

# Run với custom arguments
docker compose run --rm pipeline python scripts/run_pipeline.py \
  --orders-path data/sample/orders.csv \
  --customers-path data/sample/customers.json \
  --products-path data/sample/products.json
```

**Lưu ý**: Docker compose đã được cấu hình với **live code mounting**:
- ✅ Toàn bộ project được mount vào container
- ✅ Code changes được reflect ngay lập tức (không cần rebuild)
- ✅ Phù hợp cho development và debugging


## 📊 Services

| Service | Port | URL | Description |
|---------|------|-----|-------------|
| **Dashboard** | 8501 | http://localhost:8501 | Streamlit dashboard với BI features |
| **Jupyter** | 8888 | http://localhost:8888 | Jupyter Lab cho notebooks |
| **PostgreSQL** | 5432 | localhost:5432 | Database (user: postgres, password: etl_password) |
| **Pipeline** | - | - | ETL pipeline runner (run manually) |

## 🎯 Features

### ETL Pipeline
- ✅ Extract từ CSV, JSON, API
- ✅ Data validation và cleaning
- ✅ Data transformation và enrichment
- ✅ Load vào Data Warehouse (PostgreSQL)
- ✅ Save vào Data Lake (Parquet files)
- ✅ Error handling và logging

### Dashboard
- ✅ Overview với key metrics
- ✅ Customer analytics
- ✅ Product analytics
- ✅ Sales analytics
- ✅ Pipeline status
- ✅ **Run Pipeline** page với configurable options

### Jupyter Notebooks
- ✅ Data exploration
- ✅ Custom analysis
- ✅ Visualization
- ✅ Ad-hoc queries

## 📚 Documentation

- **`DOCKER.md`**: 🐳 Chi tiết về Docker setup và commands
- **`DASHBOARD_QUICKSTART.md`**: Hướng dẫn sử dụng Dashboard
- **`STRATEGY.md`**: Chiến lược và kiến trúc chi tiết
- **`CHECKLIST.md`**: Checklist implementation từng phase
- **`DATA_FLOW.md`**: Sơ đồ luồng dữ liệu và data model

## 🛠️ Tech Stack

- **Language**: Python 3.12
- **Data Processing**: Pandas, NumPy
- **Database**: PostgreSQL 15
- **ORM**: SQLAlchemy 2.0
- **Dashboard**: Streamlit, Plotly
- **Notebooks**: Jupyter Lab
- **Containerization**: Docker, Docker Compose
- **Orchestration**: Prefect (optional)

## 🔄 Workflow

1. **Extract**: Đọc dữ liệu từ CSV, JSON, API
2. **Save Raw**: Lưu raw data vào data lake
3. **Validate**: Kiểm tra schema và business rules
4. **Clean**: Remove duplicates, handle nulls
5. **Transform**: Join, enrich, calculate fields
6. **Save Processed**: Lưu processed data vào data lake
7. **Load**: Load vào PostgreSQL (Data Warehouse)
8. **Analyze**: Query và visualize trên Dashboard

## 🐛 Troubleshooting

### Port Already in Use

```bash
# Check what's using the port
lsof -i :8501
lsof -i :8888
lsof -i :5432

# Change ports in docker-compose.yml
```

### Database Connection Issues

```bash
# Check if PostgreSQL is running
docker compose ps postgres

# Check logs
docker compose logs postgres

# Test connection
docker compose exec postgres pg_isready -U postgres
```

### Container Won't Start

```bash
# Check logs
docker compose logs <service_name>

# Rebuild
docker compose build --no-cache <service_name>

# Remove and recreate
docker compose down
docker compose up -d
```

Xem **`DOCKER.md`** để biết thêm troubleshooting tips.

## 📝 Common Commands

```bash
# View all services status
docker compose ps

# View logs
docker compose logs -f
docker compose logs -f dashboard

# Restart service
docker compose restart dashboard

# Execute command in container
docker compose exec dashboard bash
docker compose exec postgres psql -U postgres -d etl_demo

# Stop all
docker compose down

# Stop and remove volumes (⚠️ deletes data)
docker compose down -v
```

## 🤝 Contributing

Đây là project học tập, tự do modify và experiment!

## 📄 License

MIT

---

**Happy Data Engineering! 🚀**
