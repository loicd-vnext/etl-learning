"""
ETL Pipeline - Main pipeline orchestrator
"""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from src.extract import extract_csv, extract_json, extract_json_nested
from src.transform import DataValidator, DataCleaner, DataTransformer
from src.load import DataLoader, DataLakeStorage
from src.utils.logger import logger
from src.utils.database import db_manager


@dataclass
class PipelineConfig:
    """Pipeline configuration"""
    # Data sources
    orders_path: str = "data/sample/orders.csv"
    customers_path: str = "data/sample/customers.json"
    products_path: str = "data/sample/products.json"
    products_record_path: str = "products"
    
    # Validation
    validate_data: bool = True
    required_columns: List[str] = field(default_factory=lambda: [
        'order_id', 'customer_id', 'product_id', 'quantity', 'unit_price'
    ])
    
    # Cleaning
    clean_data: bool = True
    remove_duplicates: bool = True
    handle_nulls: bool = True
    null_strategy: str = 'fill'
    
    # Transformation
    transform_data: bool = True
    
    # Loading
    save_to_lake: bool = True
    load_to_warehouse: bool = True
    batch_size: int = 1000
    
    # Error handling
    continue_on_error: bool = False
    max_retries: int = 3


@dataclass
class PipelineResult:
    """Pipeline execution result"""
    success: bool = False
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    steps_completed: List[str] = field(default_factory=list)
    steps_failed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize ETL Pipeline
        
        Args:
            config: Pipeline configuration (default: PipelineConfig())
        """
        self.config = config or PipelineConfig()
        self.result = PipelineResult()
        self.storage = DataLakeStorage()
        self.loader = DataLoader(batch_size=self.config.batch_size)
        self.validator = DataValidator()
        self.cleaner = DataCleaner()
        self.transformer = DataTransformer()
    
    def run(self) -> PipelineResult:
        """
        Run the complete ETL pipeline
        
        Returns:
            PipelineResult with execution details
        """
        self.result.start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("ETL PIPELINE STARTED")
        logger.info("=" * 60)
        
        try:
            # Step 1: Extract
            logger.info("")
            logger.info("STEP 1: EXTRACT")
            logger.info("-" * 60)
            orders_df, customers_df, products_df = self._extract()
            self.result.steps_completed.append("extract")
            self.result.statistics["extract"] = {
                "orders_rows": len(orders_df),
                "customers_rows": len(customers_df),
                "products_rows": len(products_df)
            }
            
            # Step 2: Save Raw Data to Data Lake
            if self.config.save_to_lake:
                logger.info("")
                logger.info("STEP 2: SAVE RAW DATA TO DATA LAKE")
                logger.info("-" * 60)
                self._save_raw_data(orders_df, customers_df, products_df)
                self.result.steps_completed.append("save_raw_data")
            
            # Step 3: Validate
            if self.config.validate_data:
                logger.info("")
                logger.info("STEP 3: VALIDATE")
                logger.info("-" * 60)
                self._validate(orders_df)
                self.result.steps_completed.append("validate")
            
            # Step 4: Clean
            if self.config.clean_data:
                logger.info("")
                logger.info("STEP 4: CLEAN")
                logger.info("-" * 60)
                orders_df = self._clean(orders_df)
                self.result.steps_completed.append("clean")
                self.result.statistics["clean"] = {
                    "rows_after_cleaning": len(orders_df)
                }
            
            # Step 5: Transform
            if self.config.transform_data:
                logger.info("")
                logger.info("STEP 5: TRANSFORM")
                logger.info("-" * 60)
                transformed_df = self._transform(orders_df, customers_df, products_df)
                self.result.steps_completed.append("transform")
                self.result.statistics["transform"] = {
                    "rows": len(transformed_df),
                    "columns": len(transformed_df.columns)
                }
            else:
                transformed_df = orders_df
            
            # Step 6: Save Processed Data
            if self.config.save_to_lake:
                logger.info("")
                logger.info("STEP 6: SAVE PROCESSED DATA")
                logger.info("-" * 60)
                self._save_processed_data(transformed_df)
                self.result.steps_completed.append("save_processed_data")
            
            # Step 7: Load to Warehouse
            if self.config.load_to_warehouse:
                logger.info("")
                logger.info("STEP 7: LOAD TO WAREHOUSE")
                logger.info("-" * 60)
                self._load_to_warehouse(customers_df, products_df, transformed_df)
                self.result.steps_completed.append("load_to_warehouse")
            
            # Success
            self.result.success = True
            logger.info("")
            logger.info("=" * 60)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
        
        except Exception as e:
            self.result.success = False
            error_msg = f"Pipeline failed: {str(e)}"
            self.result.errors.append(error_msg)
            logger.error("")
            logger.error("=" * 60)
            logger.error("ETL PIPELINE FAILED")
            logger.error("=" * 60)
            logger.error(error_msg)
            import traceback
            logger.error(traceback.format_exc())
            
            if not self.config.continue_on_error:
                raise
        
        finally:
            self.result.end_time = datetime.now()
            if self.result.start_time:
                self.result.duration_seconds = (
                    self.result.end_time - self.result.start_time
                ).total_seconds()
            
            self._print_summary()
        
        return self.result
    
    def _extract(self):
        """Extract data from sources"""
        logger.info("Extracting orders from CSV...")
        orders_df = extract_csv(self.config.orders_path)
        logger.info(f"  ✓ Extracted {len(orders_df)} orders")
        
        logger.info("Extracting customers from JSON...")
        customers_df = extract_json(self.config.customers_path)
        logger.info(f"  ✓ Extracted {len(customers_df)} customers")
        
        logger.info("Extracting products from nested JSON...")
        products_df = extract_json_nested(
            self.config.products_path,
            record_path=self.config.products_record_path
        )
        logger.info(f"  ✓ Extracted {len(products_df)} products")
        
        return orders_df, customers_df, products_df
    
    def _save_raw_data(self, orders_df, customers_df, products_df):
        """Save raw data to data lake"""
        timestamp = datetime.now()
        
        logger.info("Saving raw orders data...")
        self.storage.save_raw_data(orders_df, 'orders', format='parquet', timestamp=timestamp)
        logger.info("  ✓ Saved orders to data lake")
        
        logger.info("Saving raw customers data...")
        self.storage.save_raw_data(customers_df, 'customers', format='parquet', timestamp=timestamp)
        logger.info("  ✓ Saved customers to data lake")
        
        logger.info("Saving raw products data...")
        self.storage.save_raw_data(products_df, 'products', format='parquet', timestamp=timestamp)
        logger.info("  ✓ Saved products to data lake")
    
    def _validate(self, df):
        """Validate data"""
        logger.info("Running data validation...")
        
        # Business rules
        rules = {
            'quantity': lambda x: x > 0,
            'unit_price': lambda x: x > 0
        }
        
        validation_result = self.validator.validate_all(
            df,
            required_columns=self.config.required_columns,
            business_rules=rules
        )
        
        if validation_result['is_valid']:
            logger.info("  ✓ Data validation passed")
        else:
            error_msg = "Data validation failed"
            logger.error(f"  ✗ {error_msg}")
            if not self.config.continue_on_error:
                raise ValueError(error_msg)
    
    def _clean(self, df):
        """Clean data"""
        logger.info("Cleaning data...")
        cleaned_df = self.cleaner.clean_all(
            df,
            remove_duplicates=self.config.remove_duplicates,
            handle_nulls=self.config.handle_nulls,
            null_strategy=self.config.null_strategy
        )
        logger.info(f"  ✓ Cleaned data: {len(cleaned_df)} rows")
        return cleaned_df
    
    def _transform(self, orders_df, customers_df, products_df):
        """Transform data"""
        logger.info("Transforming sales data...")
        transformed_df = self.transformer.transform_sales_data(
            orders_df,
            customers_df,
            products_df
        )
        logger.info(f"  ✓ Transformed: {len(transformed_df)} rows, {len(transformed_df.columns)} columns")
        return transformed_df
    
    def _save_processed_data(self, df):
        """Save processed data to data lake"""
        logger.info("Saving processed data...")
        self.storage.save_processed_data(
            df,
            'sales_transformed',
            format='parquet',
            timestamp=datetime.now()
        )
        logger.info("  ✓ Saved processed data to data lake")
    
    def _load_to_warehouse(self, customers_df, products_df, transformed_df):
        """Load data to data warehouse"""
        # Test database connection
        if not db_manager.test_connection():
            error_msg = "Database connection failed"
            logger.error(f"  ✗ {error_msg}")
            if not self.config.continue_on_error:
                raise ConnectionError(error_msg)
            return
        
        # Load dimension tables
        logger.info("Loading dimension tables...")
        logger.info("  Loading dim_customers...")
        self.loader.load_dimension_table(
            customers_df,
            'dim_customers',
            key_column='customer_id'
        )
        logger.info("  ✓ Loaded dim_customers")
        
        logger.info("  Loading dim_products...")
        self.loader.load_dimension_table(
            products_df,
            'dim_products',
            key_column='product_id'
        )
        logger.info("  ✓ Loaded dim_products")
        
        # Prepare fact table data
        # Note: In a real scenario, we would need to populate dim_date first
        # For now, we'll skip fact table loading or use a simplified approach
        logger.info("  Note: Fact table loading requires date dimension setup")
        logger.info("  ✓ Dimension tables loaded successfully")
    
    def _print_summary(self):
        """Print pipeline execution summary"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Status: {'✓ SUCCESS' if self.result.success else '✗ FAILED'}")
        logger.info(f"Duration: {self.result.duration_seconds:.2f} seconds")
        logger.info(f"Steps completed: {len(self.result.steps_completed)}")
        logger.info(f"  {', '.join(self.result.steps_completed)}")
        
        if self.result.steps_failed:
            logger.info(f"Steps failed: {len(self.result.steps_failed)}")
            logger.info(f"  {', '.join(self.result.steps_failed)}")
        
        if self.result.errors:
            logger.info(f"Errors: {len(self.result.errors)}")
            for error in self.result.errors:
                logger.info(f"  - {error}")
        
        if self.result.statistics:
            logger.info("Statistics:")
            for key, value in self.result.statistics.items():
                logger.info(f"  {key}: {value}")
        
        logger.info("=" * 60)


def run_pipeline(config: Optional[PipelineConfig] = None) -> PipelineResult:
    """
    Convenience function to run ETL pipeline
    
    Args:
        config: Pipeline configuration
    
    Returns:
        PipelineResult
    """
    pipeline = ETLPipeline(config)
    return pipeline.run()

