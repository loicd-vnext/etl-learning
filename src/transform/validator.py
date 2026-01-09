"""
Data Validator - Validate data schema, types, and business rules
"""
import pandas as pd
from typing import Dict, List, Any, Optional, Callable
from src.utils.logger import logger


class DataValidator:
    """Validate data against schema and business rules"""
    
    def __init__(self):
        """Initialize validator"""
        self.validation_results = {}
    
    def validate_schema(
        self,
        df: pd.DataFrame,
        required_columns: List[str],
        column_types: Optional[Dict[str, type]] = None
    ) -> Dict[str, Any]:
        """
        Validate DataFrame schema
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            column_types: Dictionary mapping column names to expected types
        
        Returns:
            Dictionary with validation results
        """
        result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "missing_columns": [],
            "type_mismatches": []
        }
        
        # Check required columns
        missing = set(required_columns) - set(df.columns)
        if missing:
            result["is_valid"] = False
            result["missing_columns"] = list(missing)
            result["errors"].append(f"Missing required columns: {list(missing)}")
            logger.error(f"Missing required columns: {list(missing)}")
        
        # Check column types
        if column_types:
            for col, expected_type in column_types.items():
                if col not in df.columns:
                    continue
                
                actual_type = df[col].dtype
                if not self._check_type_compatibility(actual_type, expected_type):
                    result["is_valid"] = False
                    result["type_mismatches"].append({
                        "column": col,
                        "expected": expected_type,
                        "actual": str(actual_type)
                    })
                    result["errors"].append(
                        f"Type mismatch in column '{col}': expected {expected_type}, got {actual_type}"
                    )
                    logger.error(f"Type mismatch in column '{col}': expected {expected_type}, got {actual_type}")
        
        return result
    
    def _check_type_compatibility(self, actual: type, expected: type) -> bool:
        """Check if actual type is compatible with expected type"""
        type_mapping = {
            int: [int, 'int64', 'int32', 'Int64'],
            float: [float, 'float64', 'float32', 'Float64'],
            str: [str, 'object', 'string'],
            bool: [bool, 'bool', 'boolean']
        }
        
        expected_types = type_mapping.get(expected, [expected])
        return str(actual) in [str(t) for t in expected_types] or actual == expected
    
    def validate_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate data types in DataFrame
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Dictionary with type validation results
        """
        result = {
            "is_valid": True,
            "errors": [],
            "type_info": {}
        }
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = df[col].isnull().sum()
            
            result["type_info"][col] = {
                "dtype": dtype,
                "null_count": int(null_count),
                "null_percentage": float(null_count / len(df) * 100) if len(df) > 0 else 0
            }
            
            if null_count > len(df) * 0.5:  # More than 50% nulls
                result["warnings"] = result.get("warnings", [])
                result["warnings"].append(f"Column '{col}' has {null_count} null values ({null_count/len(df)*100:.1f}%)")
                logger.warning(f"Column '{col}' has high null percentage: {null_count/len(df)*100:.1f}%")
        
        return result
    
    def validate_business_rules(
        self,
        df: pd.DataFrame,
        rules: Dict[str, Callable]
    ) -> Dict[str, Any]:
        """
        Validate business rules
        
        Args:
            df: DataFrame to validate
            rules: Dictionary mapping column names to validation functions
                   Function should return True if valid, False otherwise
        
        Returns:
            Dictionary with validation results
        """
        result = {
            "is_valid": True,
            "errors": [],
            "violations": {}
        }
        
        for col, rule_func in rules.items():
            if col not in df.columns:
                result["errors"].append(f"Column '{col}' not found for rule validation")
                continue
            
            # Apply rule to each row
            violations = []
            for idx, value in df[col].items():
                try:
                    if not rule_func(value):
                        violations.append({
                            "row": int(idx),
                            "value": value
                        })
                except Exception as e:
                    logger.warning(f"Error applying rule to column '{col}', row {idx}: {e}")
                    violations.append({
                        "row": int(idx),
                        "value": value,
                        "error": str(e)
                    })
            
            if violations:
                result["is_valid"] = False
                result["violations"][col] = violations
                result["errors"].append(
                    f"Column '{col}' has {len(violations)} rule violations"
                )
                logger.error(f"Column '{col}' has {len(violations)} rule violations")
        
        return result
    
    def validate_completeness(
        self,
        df: pd.DataFrame,
        required_columns: Optional[List[str]] = None,
        min_completeness: float = 0.95
    ) -> Dict[str, Any]:
        """
        Validate data completeness
        
        Args:
            df: DataFrame to validate
            required_columns: Columns to check (default: all columns)
            min_completeness: Minimum completeness percentage (default: 0.95)
        
        Returns:
            Dictionary with completeness validation results
        """
        result = {
            "is_valid": True,
            "errors": [],
            "completeness": {}
        }
        
        columns_to_check = required_columns if required_columns else df.columns
        
        for col in columns_to_check:
            if col not in df.columns:
                continue
            
            null_count = df[col].isnull().sum()
            completeness = 1 - (null_count / len(df)) if len(df) > 0 else 0
            
            result["completeness"][col] = {
                "null_count": int(null_count),
                "completeness": float(completeness),
                "completeness_percentage": float(completeness * 100)
            }
            
            if completeness < min_completeness:
                result["is_valid"] = False
                result["errors"].append(
                    f"Column '{col}' completeness ({completeness*100:.1f}%) "
                    f"below minimum ({min_completeness*100:.1f}%)"
                )
                logger.error(
                    f"Column '{col}' completeness ({completeness*100:.1f}%) "
                    f"below minimum ({min_completeness*100:.1f}%)"
                )
        
        return result
    
    def validate_all(
        self,
        df: pd.DataFrame,
        required_columns: List[str],
        column_types: Optional[Dict[str, type]] = None,
        business_rules: Optional[Dict[str, Callable]] = None,
        min_completeness: float = 0.95
    ) -> Dict[str, Any]:
        """
        Run all validations
        
        Args:
            df: DataFrame to validate
            required_columns: Required column names
            column_types: Expected column types
            business_rules: Business rule functions
            min_completeness: Minimum completeness percentage
        
        Returns:
            Comprehensive validation report
        """
        logger.info(f"Starting comprehensive validation for DataFrame with {len(df)} rows")
        
        report = {
            "is_valid": True,
            "row_count": len(df),
            "column_count": len(df.columns),
            "validations": {}
        }
        
        # Schema validation
        schema_result = self.validate_schema(df, required_columns, column_types)
        report["validations"]["schema"] = schema_result
        if not schema_result["is_valid"]:
            report["is_valid"] = False
        
        # Data type validation
        type_result = self.validate_data_types(df)
        report["validations"]["data_types"] = type_result
        
        # Completeness validation
        completeness_result = self.validate_completeness(df, required_columns, min_completeness)
        report["validations"]["completeness"] = completeness_result
        if not completeness_result["is_valid"]:
            report["is_valid"] = False
        
        # Business rules validation
        if business_rules:
            rules_result = self.validate_business_rules(df, business_rules)
            report["validations"]["business_rules"] = rules_result
            if not rules_result["is_valid"]:
                report["is_valid"] = False
        
        if report["is_valid"]:
            logger.info("✓ All validations passed")
        else:
            logger.warning("⚠ Some validations failed")
        
        return report


def validate_data(
    df: pd.DataFrame,
    required_columns: List[str],
    column_types: Optional[Dict[str, type]] = None,
    business_rules: Optional[Dict[str, Callable]] = None
) -> Dict[str, Any]:
    """
    Convenience function to validate data
    
    Args:
        df: DataFrame to validate
        required_columns: Required column names
        column_types: Expected column types
        business_rules: Business rule functions
    
    Returns:
        Validation report
    """
    validator = DataValidator()
    return validator.validate_all(
        df=df,
        required_columns=required_columns,
        column_types=column_types,
        business_rules=business_rules
    )

