"""
Database setup script - Create database schema
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import text
from src.utils.database import db_manager
from src.utils.logger import logger


def create_tables():
    """Create all database tables"""
    engine = db_manager.connect()
    is_sqlite = db_manager.connection_string.startswith("sqlite")
    
    # SQL statements to create tables
    create_dim_customers = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name VARCHAR(255) NOT NULL,
        email VARCHAR(255),
        city VARCHAR(100),
        country VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    create_dim_products = """
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        category VARCHAR(100),
        brand VARCHAR(100),
        price DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    create_dim_date = """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY,
        date DATE NOT NULL UNIQUE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        week INTEGER,
        day_of_week INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Different syntax for SQLite vs PostgreSQL
    if is_sqlite:
        create_fact_sales = """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER,
            product_id INTEGER,
            date_id INTEGER,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL,
            discount DECIMAL(10, 2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );
        """
    else:  # PostgreSQL
        create_fact_sales = """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            date_id INTEGER,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL,
            discount DECIMAL(10, 2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );
        """
    
    # Create indexes for better query performance
    create_indexes = """
    CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_id);
    CREATE INDEX IF NOT EXISTS idx_fact_sales_created ON fact_sales(created_at);
    """
    
    try:
        with engine.connect() as conn:
            logger.info("Creating dimension tables...")
            conn.execute(text(create_dim_customers))
            conn.execute(text(create_dim_products))
            conn.execute(text(create_dim_date))
            
            logger.info("Creating fact table...")
            conn.execute(text(create_fact_sales))
            
            logger.info("Creating indexes...")
            conn.execute(text(create_indexes))
            
            conn.commit()
            logger.info("Database schema created successfully!")
            
            # Verify tables were created
            if db_manager.connection_string.startswith("sqlite"):
                check_tables = "SELECT name FROM sqlite_master WHERE type='table';"
            else:
                check_tables = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public';
                """
            
            result = conn.execute(text(check_tables))
            tables = [row[0] for row in result]
            logger.info(f"Created tables: {', '.join(tables)}")
            
    except Exception as e:
        logger.error(f"Error creating database schema: {e}")
        raise


def drop_tables():
    """Drop all tables (use with caution!)"""
    engine = db_manager.connect()
    
    drop_statements = [
        "DROP TABLE IF EXISTS fact_sales;",
        "DROP TABLE IF EXISTS dim_date;",
        "DROP TABLE IF EXISTS dim_products;",
        "DROP TABLE IF EXISTS dim_customers;",
    ]
    
    try:
        with engine.connect() as conn:
            logger.warning("Dropping all tables...")
            for statement in drop_statements:
                conn.execute(text(statement))
            conn.commit()
            logger.info("All tables dropped successfully!")
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        raise


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup database schema")
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop existing tables before creating (WARNING: This will delete all data!)"
    )
    
    args = parser.parse_args()
    
    # Test connection first
    logger.info("Testing database connection...")
    if not db_manager.test_connection():
        logger.error("Failed to connect to database. Please check your configuration.")
        sys.exit(1)
    
    # Drop tables if requested
    if args.drop:
        drop_tables()
    
    # Create tables
    create_tables()
    
    logger.info("Database setup completed!")


if __name__ == "__main__":
    main()

