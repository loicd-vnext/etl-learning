"""
Data Loader - Load data to data warehouse and data lake
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any
from sqlalchemy import text
from src.utils.database import db_manager
from src.utils.logger import logger


class DataLoader:
    """Load data to data warehouse and data lake"""
    
    def __init__(self, batch_size: int = 1000):
        """
        Initialize data loader
        
        Args:
            batch_size: Number of rows to insert per batch
        """
        self.batch_size = batch_size
        self.load_stats = {}
    
    def load_to_warehouse(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append',
        index: bool = False
    ) -> Dict[str, Any]:
        """
        Load DataFrame to data warehouse table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index as a column
        
        Returns:
            Dictionary with load statistics
        """
        if df.empty:
            logger.warning(f"DataFrame is empty. Nothing to load to {table_name}")
            return {"rows_loaded": 0, "status": "skipped"}
        
        logger.info(f"Loading {len(df)} rows to table '{table_name}'")
        
        try:
            engine = db_manager.connect()
            
            # Load data
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=index,
                method='multi',  # Use multi-row insert for better performance
                chunksize=self.batch_size
            )
            
            rows_loaded = len(df)
            logger.info(f"✓ Successfully loaded {rows_loaded} rows to '{table_name}'")
            
            self.load_stats[table_name] = {
                "rows_loaded": rows_loaded,
                "timestamp": datetime.now().isoformat()
            }
            
            return {
                "rows_loaded": rows_loaded,
                "status": "success",
                "table": table_name
            }
        
        except Exception as e:
            logger.error(f"Error loading data to '{table_name}': {e}")
            raise
    
    def upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Upsert (insert or update) data
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            conflict_columns: Columns to check for conflicts (primary key or unique constraint)
            update_columns: Columns to update if conflict (default: all columns except conflict_columns)
        
        Returns:
            Dictionary with upsert statistics
        """
        if df.empty:
            logger.warning(f"DataFrame is empty. Nothing to upsert to {table_name}")
            return {"rows_upserted": 0, "status": "skipped"}
        
        logger.info(f"Upserting {len(df)} rows to table '{table_name}'")
        
        try:
            engine = db_manager.connect()
            is_sqlite = db_manager.connection_string.startswith("sqlite")
            
            if update_columns is None:
                # Update all columns except conflict columns
                update_columns = [col for col in df.columns if col not in conflict_columns]
            
            rows_upserted = 0
            
            # Process in batches
            for i in range(0, len(df), self.batch_size):
                batch_df = df.iloc[i:i + self.batch_size]
                
                if is_sqlite:
                    # SQLite uses INSERT OR REPLACE
                    rows_upserted += self._upsert_sqlite(batch_df, table_name, conflict_columns, engine)
                else:
                    # PostgreSQL uses ON CONFLICT
                    rows_upserted += self._upsert_postgresql(
                        batch_df, table_name, conflict_columns, update_columns, engine
                    )
            
            logger.info(f"✓ Successfully upserted {rows_upserted} rows to '{table_name}'")
            
            return {
                "rows_upserted": rows_upserted,
                "status": "success",
                "table": table_name
            }
        
        except Exception as e:
            logger.error(f"Error upserting data to '{table_name}': {e}")
            raise
    
    def _upsert_sqlite(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: List[str],
        engine
    ) -> int:
        """Upsert for SQLite"""
        # SQLite uses INSERT OR REPLACE
        # This replaces the entire row if conflict
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        return len(df)
    
    def _upsert_postgresql(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: List[str],
        update_columns: List[str],
        engine
    ) -> int:
        """Upsert for PostgreSQL using ON CONFLICT"""
        # Build ON CONFLICT clause
        conflict_target = ", ".join(conflict_columns)
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        columns = list(df.columns)
        columns_str = ", ".join(columns)
        
        with engine.begin() as conn:
            # Process in smaller batches for better performance
            for idx, row in df.iterrows():
                values = [row[col] for col in columns]
                # Use :param style for SQLAlchemy parameter binding
                params = {f"param_{i}": val for i, val in enumerate(values)}
                placeholders = ", ".join([f":param_{i}" for i in range(len(values))])
                
                # Build INSERT ... ON CONFLICT statement
                insert_stmt = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target})
                DO UPDATE SET {update_set}
                """
                
                # Execute statement with parameter binding
                conn.execute(text(insert_stmt), params)
        
        return len(df)
    
    def incremental_load(
        self,
        df: pd.DataFrame,
        table_name: str,
        timestamp_column: str = 'created_at',
        last_load_timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Incremental load - only load new data
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            timestamp_column: Column name containing timestamp
            last_load_timestamp: Last load timestamp (if None, will query from table)
        
        Returns:
            Dictionary with load statistics
        """
        if df.empty:
            logger.warning(f"DataFrame is empty. Nothing to load incrementally to {table_name}")
            return {"rows_loaded": 0, "status": "skipped"}
        
        # Get last load timestamp if not provided
        if last_load_timestamp is None:
            last_load_timestamp = self._get_last_load_timestamp(table_name, timestamp_column)
        
        # Filter new data
        if timestamp_column in df.columns:
            if last_load_timestamp:
                df_new = df[df[timestamp_column] > last_load_timestamp].copy()
                logger.info(f"Filtered {len(df_new)} new rows (after {last_load_timestamp})")
            else:
                df_new = df.copy()
                logger.info(f"No previous load timestamp, loading all {len(df_new)} rows")
        else:
            logger.warning(f"Timestamp column '{timestamp_column}' not found. Loading all rows.")
            df_new = df.copy()
        
        if df_new.empty:
            logger.info(f"No new data to load to {table_name}")
            return {"rows_loaded": 0, "status": "no_new_data"}
        
        # Load new data
        return self.load_to_warehouse(df_new, table_name, if_exists='append')
    
    def _get_last_load_timestamp(
        self,
        table_name: str,
        timestamp_column: str
    ) -> Optional[datetime]:
        """Get last load timestamp from table"""
        try:
            engine = db_manager.connect()
            
            query = f"SELECT MAX({timestamp_column}) as last_timestamp FROM {table_name}"
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                row = result.fetchone()
                
                if row and row[0]:
                    return row[0]
                return None
        
        except Exception as e:
            logger.warning(f"Could not get last load timestamp: {e}")
            return None
    
    def load_dimension_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_column: str
    ) -> Dict[str, Any]:
        """
        Load dimension table with upsert
        
        Args:
            df: DataFrame to load
            table_name: Dimension table name
            key_column: Primary key column name
        
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Loading dimension table '{table_name}' with key '{key_column}'")
        return self.upsert(df, table_name, conflict_columns=[key_column])
    
    def load_fact_table(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Load fact table (append only)
        
        Args:
            df: DataFrame to load
            table_name: Fact table name
        
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Loading fact table '{table_name}'")
        return self.load_to_warehouse(df, table_name, if_exists='append')


class DataLakeStorage:
    """Store raw data to data lake (filesystem)"""
    
    def __init__(self, base_path: str = "data/raw"):
        """
        Initialize data lake storage
        
        Args:
            base_path: Base path for raw data storage
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save_raw_data(
        self,
        df: pd.DataFrame,
        source_name: str,
        format: str = 'parquet',
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Save raw data to data lake
        
        Args:
            df: DataFrame to save
            source_name: Name of data source
            format: File format ('parquet', 'csv', 'json')
            timestamp: Timestamp for versioning (default: current time)
        
        Returns:
            Path to saved file
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # Create directory for source
        source_dir = self.base_path / source_name
        source_dir.mkdir(parents=True, exist_ok=True)
        
        # Create filename with timestamp
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f"{source_name}_{timestamp_str}.{format}"
        filepath = source_dir / filename
        
        # Save based on format
        if format == 'parquet':
            df.to_parquet(filepath, index=False)
        elif format == 'csv':
            df.to_csv(filepath, index=False)
        elif format == 'json':
            df.to_json(filepath, orient='records', date_format='iso')
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Saved raw data to: {filepath}")
        return str(filepath)
    
    def save_processed_data(
        self,
        df: pd.DataFrame,
        process_name: str,
        format: str = 'parquet',
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Save processed data to data lake
        
        Args:
            df: DataFrame to save
            process_name: Name of processing step
            format: File format
            timestamp: Timestamp for versioning
        
        Returns:
            Path to saved file
        """
        processed_path = Path("data/processed")
        processed_path.mkdir(parents=True, exist_ok=True)
        
        if timestamp is None:
            timestamp = datetime.now()
        
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f"{process_name}_{timestamp_str}.{format}"
        filepath = processed_path / filename
        
        if format == 'parquet':
            df.to_parquet(filepath, index=False)
        elif format == 'csv':
            df.to_csv(filepath, index=False)
        elif format == 'json':
            df.to_json(filepath, orient='records', date_format='iso')
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Saved processed data to: {filepath}")
        return str(filepath)


def load_to_warehouse(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = 'append'
) -> Dict[str, Any]:
    """
    Convenience function to load data to warehouse
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        if_exists: What to do if table exists
    
    Returns:
        Load statistics
    """
    loader = DataLoader()
    return loader.load_to_warehouse(df, table_name, if_exists=if_exists)

