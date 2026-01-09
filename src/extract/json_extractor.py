"""
JSON Extractor - Extract data from JSON files
"""
import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from src.utils.logger import logger


def extract_json(
    file_path: str,
    encoding: str = "utf-8",
    orient: str = "records"
) -> pd.DataFrame:
    """
    Extract data from JSON file
    
    Args:
        file_path: Path to JSON file
        encoding: File encoding (default: utf-8)
        orient: JSON orientation for pandas (default: records)
                Options: 'records', 'index', 'values', 'split', 'table'
    
    Returns:
        DataFrame containing the JSON data
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or invalid format
        Exception: For other errors
    """
    file_path_obj = Path(file_path)
    
    # Check if file exists
    if not file_path_obj.exists():
        error_msg = f"JSON file not found: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Check if file is readable
    if not file_path_obj.is_file():
        error_msg = f"Path is not a file: {file_path}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        logger.info(f"Extracting data from JSON: {file_path}")
        
        # Read JSON file
        with open(file_path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, list):
            # List of records
            if len(data) == 0:
                logger.warning(f"JSON file contains empty list: {file_path}")
                return pd.DataFrame()
            
            # Check if it's a list of objects (records)
            if isinstance(data[0], dict):
                df = pd.json_normalize(data)
            else:
                # Simple list, convert to DataFrame
                df = pd.DataFrame(data)
        
        elif isinstance(data, dict):
            # Single object or nested structure
            if orient == "records" and "records" in data:
                # JSON with 'records' key
                df = pd.json_normalize(data["records"])
            elif orient == "index" and all(isinstance(v, dict) for v in data.values()):
                # Dictionary with index keys
                df = pd.DataFrame.from_dict(data, orient="index")
            else:
                # Try to normalize nested structure
                df = pd.json_normalize(data)
        
        else:
            error_msg = f"Unsupported JSON structure in {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Check if DataFrame is empty
        if df.empty:
            logger.warning(f"JSON file resulted in empty DataFrame: {file_path}")
            return df
        
        logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
        logger.debug(f"Columns: {list(df.columns)}")
        
        return df
    
    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON format in {file_path}: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except UnicodeDecodeError as e:
        error_msg = f"Encoding error reading {file_path}. Try different encoding: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    except Exception as e:
        error_msg = f"Unexpected error reading JSON file {file_path}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def extract_json_nested(
    file_path: str,
    record_path: Optional[Union[str, List[str]]] = None,
    meta: Optional[List[str]] = None,
    encoding: str = "utf-8"
) -> pd.DataFrame:
    """
    Extract nested JSON data using json_normalize
    
    Args:
        file_path: Path to JSON file
        record_path: Path to nested records (for json_normalize)
        meta: Metadata fields to include (for json_normalize)
        encoding: File encoding (default: utf-8)
    
    Returns:
        DataFrame with flattened nested JSON data
    """
    file_path_obj = Path(file_path)
    
    if not file_path_obj.exists():
        error_msg = f"JSON file not found: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        logger.info(f"Extracting nested JSON from: {file_path}")
        
        with open(file_path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # Normalize nested JSON
        if record_path:
            df = pd.json_normalize(data, record_path=record_path, meta=meta)
        else:
            df = pd.json_normalize(data)
        
        logger.info(f"Successfully extracted {len(df)} rows from nested JSON: {file_path}")
        logger.debug(f"Columns: {list(df.columns)}")
        
        return df
    
    except Exception as e:
        error_msg = f"Error extracting nested JSON from {file_path}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def extract_json_with_validation(
    file_path: str,
    required_fields: Optional[List[str]] = None,
    expected_count: Optional[int] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Extract JSON with validation
    
    Args:
        file_path: Path to JSON file
        required_fields: List of required field names
        expected_count: Expected number of records
        **kwargs: Additional arguments passed to extract_json
    
    Returns:
        Dictionary with 'data' (DataFrame) and 'validation' (dict with validation results)
    """
    # Extract data
    df = extract_json(file_path, **kwargs)
    
    validation = {
        "file_path": file_path,
        "rows_extracted": len(df),
        "columns_found": list(df.columns),
        "is_valid": True,
        "errors": [],
        "warnings": []
    }
    
    # Check required fields
    if required_fields:
        missing_fields = set(required_fields) - set(df.columns)
        if missing_fields:
            validation["is_valid"] = False
            validation["errors"].append(
                f"Missing required fields: {list(missing_fields)}"
            )
            logger.error(f"Missing required fields: {list(missing_fields)}")
    
    # Check expected count
    if expected_count is not None:
        if len(df) != expected_count:
            validation["warnings"].append(
                f"Expected {expected_count} records, got {len(df)}"
            )
            logger.warning(f"Record count mismatch: expected {expected_count}, got {len(df)}")
    
    return {
        "data": df,
        "validation": validation
    }

