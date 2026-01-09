"""
Run ETL Pipeline - Main script to execute the ETL pipeline
"""
import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.pipeline import ETLPipeline, PipelineConfig
from src.utils.logger import logger


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Run ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default configuration
  python scripts/run_pipeline.py
  
  # Run with custom data sources
  python scripts/run_pipeline.py --orders data/my_orders.csv --customers data/my_customers.json
  
  # Run without validation
  python scripts/run_pipeline.py --no-validate
  
  # Run without loading to warehouse
  python scripts/run_pipeline.py --no-load
        """
    )
    
    # Data source arguments
    parser.add_argument(
        '--orders',
        type=str,
        default='data/sample/orders.csv',
        help='Path to orders CSV file (default: data/sample/orders.csv)'
    )
    parser.add_argument(
        '--customers',
        type=str,
        default='data/sample/customers.json',
        help='Path to customers JSON file (default: data/sample/customers.json)'
    )
    parser.add_argument(
        '--products',
        type=str,
        default='data/sample/products.json',
        help='Path to products JSON file (default: data/sample/products.json)'
    )
    
    # Pipeline options
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip data validation step'
    )
    parser.add_argument(
        '--no-clean',
        action='store_true',
        help='Skip data cleaning step'
    )
    parser.add_argument(
        '--no-transform',
        action='store_true',
        help='Skip data transformation step'
    )
    parser.add_argument(
        '--no-lake',
        action='store_true',
        help='Skip saving to data lake'
    )
    parser.add_argument(
        '--no-load',
        action='store_true',
        help='Skip loading to data warehouse'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue pipeline execution even if a step fails'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for database operations (default: 1000)'
    )
    
    args = parser.parse_args()
    
    # Create configuration
    config = PipelineConfig(
        orders_path=args.orders,
        customers_path=args.customers,
        products_path=args.products,
        validate_data=not args.no_validate,
        clean_data=not args.no_clean,
        transform_data=not args.no_transform,
        save_to_lake=not args.no_lake,
        load_to_warehouse=not args.no_load,
        continue_on_error=args.continue_on_error,
        batch_size=args.batch_size
    )
    
    # Run pipeline
    try:
        pipeline = ETLPipeline(config)
        result = pipeline.run()
        
        # Exit with appropriate code
        sys.exit(0 if result.success else 1)
    
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

