"""
Dashboard/BI - View ETL results and analytics
"""
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy import text

from src.utils.database import db_manager
from src.utils.logger import logger


class Dashboard:
    """Dashboard for viewing ETL results and analytics"""
    
    def __init__(self):
        """Initialize dashboard"""
        self.engine = None
    
    def connect(self):
        """Connect to database"""
        if self.engine is None:
            self.engine = db_manager.connect()
        return self.engine
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """
        Get pipeline execution statistics
        
        Returns:
            Dictionary with pipeline statistics
        """
        stats = {
            "database_connected": False,
            "tables": {},
            "total_records": 0,
            "last_update": None
        }
        
        try:
            engine = self.connect()
            if not db_manager.test_connection():
                return stats
            
            stats["database_connected"] = True
            
            # Get table row counts
            with engine.connect() as conn:
                # Get all tables
                tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
                """
                result = conn.execute(text(tables_query))
                tables = [row[0] for row in result]
                
                for table in tables:
                    count_query = f"SELECT COUNT(*) as count FROM {table};"
                    count_result = conn.execute(text(count_query))
                    count = count_result.fetchone()[0]
                    
                    stats["tables"][table] = {
                        "row_count": int(count),
                        "last_updated": None  # Could add timestamp tracking
                    }
                    stats["total_records"] += int(count)
            
            stats["last_update"] = datetime.now().isoformat()
        
        except Exception as e:
            logger.error(f"Error getting pipeline stats: {e}")
        
        return stats
    
    def get_sales_summary(self) -> pd.DataFrame:
        """
        Get sales summary from fact table
        
        Returns:
            DataFrame with sales summary
        """
        try:
            engine = self.connect()
            
            # Check if fact_sales table exists and has data
            check_query = "SELECT COUNT(*) as count FROM fact_sales;"
            with engine.connect() as conn:
                result = conn.execute(text(check_query))
                count = result.fetchone()[0]
                
                if count == 0:
                    # Return empty summary
                    return pd.DataFrame({
                        'total_orders': [0],
                        'total_quantity': [0],
                        'total_revenue': [0.0],
                        'avg_order_value': [0.0],
                        'total_discount': [0.0]
                    })
            
            query = """
            SELECT 
                COUNT(*) as total_orders,
                COALESCE(SUM(quantity), 0) as total_quantity,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                COALESCE(AVG(total_amount), 0) as avg_order_value,
                COALESCE(SUM(discount), 0) as total_discount
            FROM fact_sales;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                return df
        
        except Exception as e:
            logger.error(f"Error getting sales summary: {e}")
            return pd.DataFrame({
                'total_orders': [0],
                'total_quantity': [0],
                'total_revenue': [0.0],
                'avg_order_value': [0.0],
                'total_discount': [0.0]
            })
    
    def get_customer_summary(self) -> pd.DataFrame:
        """
        Get customer summary
        
        Returns:
            DataFrame with customer statistics
        """
        try:
            engine = self.connect()
            
            query = """
            SELECT 
                COUNT(*) as total_customers,
                COUNT(DISTINCT city) as cities,
                COUNT(DISTINCT country) as countries
            FROM dim_customers;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                return df
        
        except Exception as e:
            logger.error(f"Error getting customer summary: {e}")
            return pd.DataFrame()
    
    def get_product_summary(self) -> pd.DataFrame:
        """
        Get product summary
        
        Returns:
            DataFrame with product statistics
        """
        try:
            engine = self.connect()
            
            query = """
            SELECT 
                COUNT(*) as total_products,
                COUNT(DISTINCT category) as categories,
                COUNT(DISTINCT brand) as brands,
                AVG(price) as avg_price
            FROM dim_products;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                return df
        
        except Exception as e:
            logger.error(f"Error getting product summary: {e}")
            return pd.DataFrame()
    
    def get_top_customers(self, limit: int = 10) -> pd.DataFrame:
        """
        Get top customers by revenue
        
        Args:
            limit: Number of top customers to return
        
        Returns:
            DataFrame with top customers
        """
        try:
            engine = self.connect()
            
            query = """
            SELECT 
                c.customer_id,
                c.customer_name,
                c.city,
                c.country,
                COUNT(f.sale_id) as order_count,
                SUM(f.total_amount) as total_revenue
            FROM dim_customers c
            LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
            GROUP BY c.customer_id, c.customer_name, c.city, c.country
            ORDER BY total_revenue DESC NULLS LAST
            LIMIT :limit;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={"limit": limit})
                return df
        
        except Exception as e:
            logger.error(f"Error getting top customers: {e}")
            return pd.DataFrame()
    
    def get_top_products(self, limit: int = 10) -> pd.DataFrame:
        """
        Get top products by sales
        
        Args:
            limit: Number of top products to return
        
        Returns:
            DataFrame with top products
        """
        try:
            engine = self.connect()
            
            query = """
            SELECT 
                p.product_id,
                p.product_name,
                p.category,
                p.brand,
                COUNT(f.sale_id) as sales_count,
                SUM(f.quantity) as total_quantity_sold,
                SUM(f.total_amount) as total_revenue
            FROM dim_products p
            LEFT JOIN fact_sales f ON p.product_id = f.product_id
            GROUP BY p.product_id, p.product_name, p.category, p.brand
            ORDER BY total_revenue DESC NULLS LAST
            LIMIT :limit;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={"limit": limit})
                return df
        
        except Exception as e:
            logger.error(f"Error getting top products: {e}")
            return pd.DataFrame()
    
    def get_daily_sales(self) -> pd.DataFrame:
        """
        Get daily sales trend
        
        Returns:
            DataFrame with daily sales
        """
        try:
            engine = self.connect()
            
            # Try to get from fact_sales if available, otherwise return empty
            query = """
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as order_count,
                SUM(quantity) as total_quantity,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value
            FROM fact_sales
            GROUP BY DATE(created_at)
            ORDER BY DATE(created_at);
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                if not df.empty and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                return df
        
        except Exception as e:
            logger.debug(f"Error getting daily sales (may be no data): {e}")
            return pd.DataFrame()
    
    def get_category_performance(self) -> pd.DataFrame:
        """
        Get sales performance by category
        
        Returns:
            DataFrame with category performance
        """
        try:
            engine = self.connect()
            
            query = """
            SELECT 
                p.category,
                COUNT(f.sale_id) as sales_count,
                SUM(f.quantity) as total_quantity,
                SUM(f.total_amount) as total_revenue,
                AVG(f.total_amount) as avg_order_value
            FROM dim_products p
            LEFT JOIN fact_sales f ON p.product_id = f.product_id
            GROUP BY p.category
            ORDER BY total_revenue DESC NULLS LAST;
            """
            
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                return df
        
        except Exception as e:
            logger.error(f"Error getting category performance: {e}")
            return pd.DataFrame()


def get_dashboard_data() -> Dict[str, Any]:
    """
    Get all dashboard data
    
    Returns:
        Dictionary with all dashboard data
    """
    dashboard = Dashboard()
    
    return {
        "stats": dashboard.get_pipeline_stats(),
        "sales_summary": dashboard.get_sales_summary(),
        "customer_summary": dashboard.get_customer_summary(),
        "product_summary": dashboard.get_product_summary(),
        "top_customers": dashboard.get_top_customers(),
        "top_products": dashboard.get_top_products(),
        "daily_sales": dashboard.get_daily_sales(),
        "category_performance": dashboard.get_category_performance()
    }

