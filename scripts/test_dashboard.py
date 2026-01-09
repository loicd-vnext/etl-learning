"""
Test dashboard functionality
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.dashboard import Dashboard
from src.utils.database import db_manager
from src.utils.logger import logger


def test_dashboard():
    """Test dashboard functionality"""
    logger.info("=" * 60)
    logger.info("TESTING DASHBOARD FUNCTIONALITY")
    logger.info("=" * 60)
    
    # Test database connection
    logger.info("1. Testing database connection...")
    if not db_manager.test_connection():
        logger.error("✗ Database connection failed")
        return False
    logger.info("  ✓ Database connected")
    
    # Initialize dashboard
    logger.info("2. Initializing dashboard...")
    dashboard = Dashboard()
    logger.info("  ✓ Dashboard initialized")
    
    # Test pipeline stats
    logger.info("3. Testing pipeline stats...")
    stats = dashboard.get_pipeline_stats()
    if stats["database_connected"]:
        logger.info(f"  ✓ Pipeline stats: {len(stats['tables'])} tables, {stats['total_records']} total records")
    else:
        logger.warning("  ⚠ Pipeline stats: Database not connected")
    
    # Test customer summary
    logger.info("4. Testing customer summary...")
    customer_summary = dashboard.get_customer_summary()
    if not customer_summary.empty:
        logger.info(f"  ✓ Customer summary: {customer_summary['total_customers'].iloc[0]} customers")
    else:
        logger.warning("  ⚠ No customer data")
    
    # Test product summary
    logger.info("5. Testing product summary...")
    product_summary = dashboard.get_product_summary()
    if not product_summary.empty:
        logger.info(f"  ✓ Product summary: {product_summary['total_products'].iloc[0]} products")
    else:
        logger.warning("  ⚠ No product data")
    
    # Test sales summary
    logger.info("6. Testing sales summary...")
    sales_summary = dashboard.get_sales_summary()
    if not sales_summary.empty:
        logger.info(f"  ✓ Sales summary available")
    else:
        logger.info("  ℹ No sales data (run pipeline to load data)")
    
    # Test top customers
    logger.info("7. Testing top customers query...")
    top_customers = dashboard.get_top_customers(limit=5)
    logger.info(f"  ✓ Top customers query: {len(top_customers)} results")
    
    # Test top products
    logger.info("8. Testing top products query...")
    top_products = dashboard.get_top_products(limit=5)
    logger.info(f"  ✓ Top products query: {len(top_products)} results")
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ Dashboard functionality test completed!")
    logger.info("=" * 60)
    logger.info("")
    logger.info("To start dashboard, run:")
    logger.info("  streamlit run dashboard.py")
    logger.info("")
    
    return True


if __name__ == "__main__":
    test_dashboard()

