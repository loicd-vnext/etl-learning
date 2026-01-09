"""
CSV Extractor - Extract data from CSV files
"""
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from src.utils.logger import logger


def extract_csv(
    file_path: str,
    encoding: str = "utf-8",
    delimiter: str = ",",
    header: int = 0,
    **kwargs
) -> pd.DataFrame:
    """
    Extract data from CSV file
    
    Args:
        file_path: Path to CSV file
        encoding: File encoding (default: utf-8)
        delimiter: CSV delimiter (default: ,)
        header: Row to use as column names (default: 0)
        **kwargs: Additional arguments passed to pd.read_csv
    
    Returns:
        DataFrame containing the CSV data
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or invalid format
        Exception: For other errors
    """
    file_path_obj = Path(file_path)
    
    # Check if file exists
    if not file_path_obj.exists():
        error_msg = f"CSV file not found: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Check if file is readable
    if not file_path_obj.is_file():
        error_msg = f"Path is not a file: {file_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        logger.info(f"Extracting data from CSV: {file_path}")
        
        # Read CSV file
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            delimiter=delimiter,
            header=header,
            **kwargs
        )
        
        # Check if DataFrame is empty
        if df.empty:
            logger.warning(f"CSV file is empty: {file_path}")
            return df
        
        logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
        logger.debug(f"Columns: {list(df.columns)}")
        
        return df
    
    except pd.errors.EmptyDataError:
        error_msg = f"CSV file is empty: {file_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except pd.errors.ParserError as e:
        error_msg = f"Error parsing CSV file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except UnicodeDecodeError as e:
        error_msg = f"Encoding error reading {file_path}. Try different encoding: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except Exception as e:
        error_msg = f"Unexpected error reading CSV file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def extract_csv_with_validation(
    file_path: str,
    required_columns: Optional[list] = None,
    expected_rows: Optional[int] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Extract CSV with validation
    
    Args:
        file_path: Path to CSV file
        required_columns: List of required column names
        expected_rows: Expected number of rows (for validation)
        **kwargs: Additional arguments passed to extract_csv
    
    Returns:
        Dictionary with 'data' (DataFrame) and 'validation' (dict with validation results)
    """
    # Extract data
    df = extract_csv(file_path, **kwargs)
    
    validation = {
        "file_path": file_path,
        "rows_extracted": len(df),
        "columns_found": list(df.columns),
        "is_valid": True,
        "errors": [],
        "warnings": []
    }
    
    # Check required columns
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            validation["is_valid"] = False
            validation["errors"].append(
                f"Missing required columns: {list(missing_columns)}"
            )
            logger.error(f"Missing required columns: {list(missing_columns)}")
    
    # Check expected rows
    if expected_rows is not None:
        if len(df) != expected_rows:
            validation["warnings"].append(
                f"Expected {expected_rows} rows, got {len(df)}"
            )
            logger.warning(f"Row count mismatch: expected {expected_rows}, got {len(df)}")
    
    return {
        "data": df,
        "validation": validation
    }

