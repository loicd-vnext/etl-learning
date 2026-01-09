"""
Data Cleaner - Clean and standardize data
"""
import pandas as pd
import re
from typing import Optional, Dict, List, Literal, Any
from src.utils.logger import logger


class DataCleaner:
    """Clean and standardize data"""
    
    def __init__(self):
        """Initialize cleaner"""
        self.cleaning_stats = {}
    
    def remove_duplicates(
        self,
        df: pd.DataFrame,
        subset: Optional[List[str]] = None,
        keep: Literal['first', 'last', False] = 'first'
    ) -> pd.DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: DataFrame to clean
            subset: Columns to consider for duplicates (default: all columns)
            keep: Which duplicates to keep ('first', 'last', or False to drop all)
        
        Returns:
            DataFrame with duplicates removed
        """
        original_count = len(df)
        
        df_cleaned = df.drop_duplicates(subset=subset, keep=keep)
        
        removed_count = original_count - len(df_cleaned)
        if removed_count > 0:
            logger.info(f"Removed {removed_count} duplicate rows")
            self.cleaning_stats["duplicates_removed"] = removed_count
        
        return df_cleaned
    
    def handle_nulls(
        self,
        df: pd.DataFrame,
        strategy: Literal['fill', 'drop', 'skip'] = 'fill',
        fill_value: Optional[Any] = None,
        fill_method: Optional[Literal['mean', 'median', 'mode', 'forward', 'backward']] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Handle null values
        
        Args:
            df: DataFrame to clean
            strategy: Strategy to handle nulls ('fill', 'drop', 'skip')
            fill_value: Value to fill (if strategy='fill' and fill_method=None)
            fill_method: Method to fill ('mean', 'median', 'mode', 'forward', 'backward')
            columns: Columns to process (default: all columns)
        
        Returns:
            DataFrame with nulls handled
        """
        df_cleaned = df.copy()
        columns_to_process = columns if columns else df.columns
        
        if strategy == 'drop':
            original_count = len(df_cleaned)
            df_cleaned = df_cleaned.dropna(subset=columns_to_process)
            removed_count = original_count - len(df_cleaned)
            if removed_count > 0:
                logger.info(f"Dropped {removed_count} rows with null values")
                self.cleaning_stats["null_rows_dropped"] = removed_count
        
        elif strategy == 'fill':
            for col in columns_to_process:
                if col not in df_cleaned.columns:
                    continue
                
                null_count = df_cleaned[col].isnull().sum()
                if null_count == 0:
                    continue
                
                if fill_method == 'mean' and pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    fill_val = df_cleaned[col].mean()
                    df_cleaned[col].fillna(fill_val, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with mean: {fill_val:.2f}")
                
                elif fill_method == 'median' and pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    fill_val = df_cleaned[col].median()
                    df_cleaned[col].fillna(fill_val, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with median: {fill_val:.2f}")
                
                elif fill_method == 'mode':
                    fill_val = df_cleaned[col].mode()[0] if not df_cleaned[col].mode().empty else None
                    if fill_val is not None:
                        df_cleaned[col].fillna(fill_val, inplace=True)
                        logger.info(f"Filled {null_count} nulls in '{col}' with mode: {fill_val}")
                
                elif fill_method == 'forward':
                    df_cleaned[col].fillna(method='ffill', inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with forward fill")
                
                elif fill_method == 'backward':
                    df_cleaned[col].fillna(method='bfill', inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with backward fill")
                
                elif fill_value is not None:
                    df_cleaned[col].fillna(fill_value, inplace=True)
                    logger.info(f"Filled {null_count} nulls in '{col}' with value: {fill_value}")
        
        # 'skip' strategy does nothing
        return df_cleaned
    
    def trim_whitespace(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Trim whitespace from string columns
        
        Args:
            df: DataFrame to clean
            columns: Columns to process (default: all string columns)
        
        Returns:
            DataFrame with trimmed strings
        """
        df_cleaned = df.copy()
        columns_to_process = columns if columns else df.select_dtypes(include=['object']).columns
        
        for col in columns_to_process:
            if col in df_cleaned.columns and df_cleaned[col].dtype == 'object':
                df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
                logger.debug(f"Trimmed whitespace in column '{col}'")
        
        return df_cleaned
    
    def standardize_dates(
        self,
        df: pd.DataFrame,
        date_columns: List[str],
        format: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Standardize date formats
        
        Args:
            df: DataFrame to clean
            date_columns: Columns containing dates
            format: Expected date format (if None, pandas will infer)
        
        Returns:
            DataFrame with standardized dates
        """
        df_cleaned = df.copy()
        
        for col in date_columns:
            if col not in df_cleaned.columns:
                continue
            
            try:
                if format:
                    df_cleaned[col] = pd.to_datetime(df_cleaned[col], format=format, errors='coerce')
                else:
                    df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
                
                logger.info(f"Standardized dates in column '{col}'")
            except Exception as e:
                logger.warning(f"Error standardizing dates in '{col}': {e}")
        
        return df_cleaned
    
    def standardize_emails(
        self,
        df: pd.DataFrame,
        email_columns: List[str]
    ) -> pd.DataFrame:
        """
        Standardize email formats (lowercase, trim)
        
        Args:
            df: DataFrame to clean
            email_columns: Columns containing emails
        
        Returns:
            DataFrame with standardized emails
        """
        df_cleaned = df.copy()
        
        for col in email_columns:
            if col not in df_cleaned.columns:
                continue
            
            df_cleaned[col] = df_cleaned[col].astype(str).str.lower().str.strip()
            logger.debug(f"Standardized emails in column '{col}'")
        
        return df_cleaned
    
    def clean_all(
        self,
        df: pd.DataFrame,
        remove_duplicates: bool = True,
        handle_nulls: bool = True,
        null_strategy: Literal['fill', 'drop', 'skip'] = 'fill',
        trim_whitespace: bool = True,
        standardize_dates: Optional[List[str]] = None,
        standardize_emails: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Apply all cleaning operations
        
        Args:
            df: DataFrame to clean
            remove_duplicates: Whether to remove duplicates
            handle_nulls: Whether to handle nulls
            null_strategy: Strategy for handling nulls
            trim_whitespace: Whether to trim whitespace
            standardize_dates: List of date columns to standardize
            standardize_emails: List of email columns to standardize
        
        Returns:
            Cleaned DataFrame
        """
        logger.info(f"Starting data cleaning for DataFrame with {len(df)} rows")
        df_cleaned = df.copy()
        
        if remove_duplicates:
            df_cleaned = self.remove_duplicates(df_cleaned)
        
        if handle_nulls:
            df_cleaned = self.handle_nulls(df_cleaned, strategy=null_strategy)
        
        if trim_whitespace:
            df_cleaned = self.trim_whitespace(df_cleaned)
        
        if standardize_dates:
            df_cleaned = self.standardize_dates(df_cleaned, standardize_dates)
        
        if standardize_emails:
            df_cleaned = self.standardize_emails(df_cleaned, standardize_emails)
        
        logger.info(f"Data cleaning completed. Final row count: {len(df_cleaned)}")
        return df_cleaned


def clean_data(
    df: pd.DataFrame,
    remove_duplicates: bool = True,
    handle_nulls: bool = True,
    null_strategy: Literal['fill', 'drop', 'skip'] = 'fill'
) -> pd.DataFrame:
    """
    Convenience function to clean data
    
    Args:
        df: DataFrame to clean
        remove_duplicates: Whether to remove duplicates
        handle_nulls: Whether to handle nulls
        null_strategy: Strategy for handling nulls
    
    Returns:
        Cleaned DataFrame
    """
    cleaner = DataCleaner()
    return cleaner.clean_all(
        df=df,
        remove_duplicates=remove_duplicates,
        handle_nulls=handle_nulls,
        null_strategy=null_strategy
    )

