# Chiến Lược Dự Án Data Engineering Demo

## 🎯 Mục Tiêu Dự Án

Dự án này sẽ giúp bạn hiểu được:
- Luồng xử lý dữ liệu từ nguồn đến đích (ETL/ELT)
- Các thành phần cơ bản trong data pipeline
- Cách xử lý và chuyển đổi dữ liệu
- Lưu trữ và truy vấn dữ liệu

## 📊 Kiến Trúc Tổng Quan

```
┌─────────────┐
│ Data Source │  (CSV, JSON, API, Database)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Extract   │  (Trích xuất dữ liệu)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Transform/   │  (Làm sạch, validate, enrich)
│  Process    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Load     │  (Lưu vào Data Warehouse/Lake)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Analytics  │  (Query, Report, Dashboard)
└─────────────┘
```

## 🏗️ Các Thành Phần Cần Có

### 1. **Data Sources (Nguồn Dữ Liệu)**
- CSV files (dữ liệu bán hàng, khách hàng)
- JSON files (logs, events)
- API endpoints (mock data)
- Database (PostgreSQL/MySQL)

### 2. **Data Extraction Layer**
- Scripts để đọc từ nhiều nguồn khác nhau
- Xử lý batch và streaming (tùy chọn)
- Error handling và retry logic

### 3. **Data Processing/Transformation**
- Data validation (kiểm tra format, kiểu dữ liệu)
- Data cleaning (loại bỏ duplicates, null values)
- Data enrichment (join, aggregate, calculate)
- Data normalization (chuẩn hóa format)

### 4. **Data Storage**
- **Data Warehouse**: PostgreSQL (cho structured data)
- **Data Lake**: File system hoặc S3-compatible storage (cho raw data)
- **Metadata Store**: Tracking schema, lineage, quality metrics

### 5. **Orchestration**
- Workflow scheduler (Airflow, Prefect, hoặc simple cron)
- Dependency management
- Monitoring và alerting

### 6. **Data Quality & Monitoring**
- Data quality checks
- Logging và monitoring
- Error tracking

### 7. **Analytics Layer**
- SQL queries
- Simple dashboard (optional)
- Reports

## 🛠️ Tech Stack Đề Xuất

- **Language**: Python 3.9+
- **ETL Framework**: Pandas, PySpark (optional)
- **Database**: PostgreSQL hoặc SQLite (cho demo)
- **Orchestration**: Prefect hoặc Apache Airflow (local)
- **Storage**: Local filesystem hoặc MinIO (S3-compatible)

## 📁 Cấu Trúc Project

```
etl/
├── README.md
├── requirements.txt
├── docker-compose.yml          # PostgreSQL, MinIO (optional)
├── config/
│   ├── config.yaml            # Configuration
│   └── database.yaml          # DB connections
├── data/
│   ├── raw/                   # Raw data từ sources
│   ├── processed/             # Data sau khi transform
│   └── sample/                # Sample data để test
├── src/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── csv_extractor.py
│   │   ├── json_extractor.py
│   │   └── api_extractor.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── cleaner.py
│   │   ├── validator.py
│   │   └── transformer.py
│   ├── load/
│   │   ├── __init__.py
│   │   └── loader.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logger.py
│   │   └── database.py
│   └── pipeline.py            # Main pipeline orchestrator
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── scripts/
│   ├── setup_db.py            # Setup database schema
│   └── run_pipeline.py        # Run pipeline
└── notebooks/
    └── analysis.ipynb         # Jupyter notebook cho analysis
```

## 🔄 Use Case Demo: E-commerce Sales Pipeline

### Scenario:
Xử lý dữ liệu bán hàng từ nhiều nguồn:
1. **Orders CSV**: Thông tin đơn hàng
2. **Customers JSON**: Thông tin khách hàng
3. **Products API**: Thông tin sản phẩm

### Pipeline Flow:
1. **Extract**: Đọc dữ liệu từ 3 nguồn
2. **Transform**: 
   - Join orders với customers
   - Enrich với product details
   - Tính toán revenue, profit
   - Validate data quality
3. **Load**: 
   - Lưu raw data vào data lake
   - Lưu processed data vào data warehouse
4. **Analytics**: 
   - Query tổng doanh thu theo ngày
   - Top customers
   - Product performance

## 📋 Implementation Plan

### Phase 1: Setup & Infrastructure
- [ ] Setup Python environment
- [ ] Install dependencies
- [ ] Setup database (PostgreSQL hoặc SQLite)
- [ ] Create project structure
- [ ] Setup logging

### Phase 2: Extract Layer
- [ ] Implement CSV extractor
- [ ] Implement JSON extractor
- [ ] Implement API extractor
- [ ] Add error handling
- [ ] Write tests

### Phase 3: Transform Layer
- [ ] Implement data validator
- [ ] Implement data cleaner
- [ ] Implement transformer (join, aggregate)
- [ ] Add data quality checks
- [ ] Write tests

### Phase 4: Load Layer
- [ ] Implement database loader
- [ ] Implement file loader (data lake)
- [ ] Add schema management
- [ ] Write tests

### Phase 5: Pipeline Orchestration
- [ ] Create main pipeline
- [ ] Add dependency management
- [ ] Add scheduling (optional)
- [ ] Add monitoring

### Phase 6: Sample Data & Testing
- [ ] Generate sample data
- [ ] Create end-to-end test
- [ ] Document usage

### Phase 7: Analytics & Reporting
- [ ] Create sample queries
- [ ] Create simple dashboard (optional)
- [ ] Document insights

## 🎓 Concepts Cần Hiểu

1. **ETL vs ELT**: Extract-Transform-Load vs Extract-Load-Transform
2. **Batch vs Streaming**: Xử lý theo lô vs real-time
3. **Data Modeling**: Star schema, fact/dimension tables
4. **Data Quality**: Validation, profiling, monitoring
5. **Orchestration**: Workflow management, scheduling
6. **Data Lineage**: Tracking data flow
7. **Incremental Loading**: Chỉ load data mới/changed
8. **Idempotency**: Chạy lại pipeline không tạo duplicate

## 🔗 Resources

- Apache Airflow: https://airflow.apache.org/
- dbt: https://www.getdbt.com/
- Prefect: https://www.prefect.io/
- Great Expectations: https://greatexpectations.io/
