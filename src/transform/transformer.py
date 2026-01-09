"""
Transformer - Transform, join, enrich, and aggregate data
"""
import pandas as pd
from typing import Optional, Dict, List, Any
from src.utils.logger import logger


class DataTransformer:
    """Transform and enrich data"""
    
    def __init__(self):
        """Initialize transformer"""
        self.transformation_stats = {}
    
    def join_data(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_on: str,
        right_on: str,
        how: str = 'inner',
        suffixes: tuple = ('_x', '_y')
    ) -> pd.DataFrame:
        """
        Join two DataFrames
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_on: Column name in left DataFrame
            right_on: Column name in right DataFrame
            how: Type of join ('left', 'right', 'inner', 'outer')
            suffixes: Suffixes for overlapping columns
        
        Returns:
            Joined DataFrame
        """
        logger.info(f"Joining DataFrames: {len(left_df)} rows x {len(right_df)} rows")
        
        try:
            result = pd.merge(
                left_df,
                right_df,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes=suffixes
            )
            
            logger.info(f"Join completed: {len(result)} rows after {how} join")
            self.transformation_stats["join"] = {
                "left_rows": len(left_df),
                "right_rows": len(right_df),
                "result_rows": len(result),
                "join_type": how
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Error joining DataFrames: {e}")
            raise
    
    def enrich_with_lookup(
        self,
        df: pd.DataFrame,
        lookup_df: pd.DataFrame,
        df_key: str,
        lookup_key: str,
        lookup_columns: List[str],
        prefix: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Enrich DataFrame with data from lookup table
        
        Args:
            df: Main DataFrame
            lookup_df: Lookup DataFrame
            df_key: Key column in main DataFrame
            lookup_key: Key column in lookup DataFrame
            lookup_columns: Columns to add from lookup DataFrame
            prefix: Prefix for new columns (optional)
        
        Returns:
            Enriched DataFrame
        """
        logger.info(f"Enriching DataFrame with {len(lookup_columns)} columns from lookup table")
        
        # Select columns to merge
        merge_columns = [lookup_key] + lookup_columns
        lookup_subset = lookup_df[merge_columns].copy()
        
        # Add prefix if specified
        if prefix:
            lookup_subset = lookup_subset.rename(columns={
                col: f"{prefix}_{col}" for col in lookup_columns
            })
            lookup_columns = [f"{prefix}_{col}" for col in lookup_columns]
        
        # Merge
        result = pd.merge(
            df,
            lookup_subset,
            left_on=df_key,
            right_on=lookup_key,
            how='left'
        )
        
        # Drop the lookup key column if it's not the same as df_key
        if df_key != lookup_key:
            result = result.drop(columns=[lookup_key])
        
        logger.info(f"Enrichment completed: added {len(lookup_columns)} columns")
        return result
    
    def calculate_fields(
        self,
        df: pd.DataFrame,
        calculations: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Calculate new fields using expressions
        
        Args:
            df: DataFrame
            calculations: Dictionary mapping new column names to calculation expressions
                         Expressions can use column names and pandas operations
                         Example: {'total': 'quantity * unit_price', 'discount_amount': 'total * discount / 100'}
        
        Returns:
            DataFrame with calculated fields
        """
        logger.info(f"Calculating {len(calculations)} new fields")
        
        df_result = df.copy()
        
        for new_col, expression in calculations.items():
            try:
                # Evaluate expression in the context of the DataFrame
                df_result[new_col] = df_result.eval(expression)
                logger.debug(f"Calculated field '{new_col}': {expression}")
            except Exception as e:
                logger.error(f"Error calculating field '{new_col}' with expression '{expression}': {e}")
                raise
        
        logger.info(f"Successfully calculated {len(calculations)} fields")
        return df_result
    
    def aggregate_data(
        self,
        df: pd.DataFrame,
        group_by: List[str],
        aggregations: Dict[str, List[str]]
    ) -> pd.DataFrame:
        """
        Aggregate data by groups
        
        Args:
            df: DataFrame to aggregate
            group_by: Columns to group by
            aggregations: Dictionary mapping columns to aggregation functions
                        Example: {'quantity': ['sum', 'mean'], 'total_amount': ['sum']}
        
        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Aggregating data by {group_by}")
        
        try:
            # Build aggregation dictionary
            agg_dict = {}
            for col, funcs in aggregations.items():
                if col not in df.columns:
                    logger.warning(f"Column '{col}' not found for aggregation")
                    continue
                
                for func in funcs:
                    new_col_name = f"{col}_{func}"
                    agg_dict[col] = funcs
            
            # Group and aggregate
            result = df.groupby(group_by).agg(aggregations).reset_index()
            
            # Flatten column names if needed
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                                 for col in result.columns.values]
            
            logger.info(f"Aggregation completed: {len(result)} groups")
            return result
        
        except Exception as e:
            logger.error(f"Error aggregating data: {e}")
            raise
    
    def filter_data(
        self,
        df: pd.DataFrame,
        condition: str
    ) -> pd.DataFrame:
        """
        Filter DataFrame using condition
        
        Args:
            df: DataFrame to filter
            condition: Filter condition as string expression
                      Example: 'quantity > 0', 'total_amount >= 100'
        
        Returns:
            Filtered DataFrame
        """
        logger.info(f"Filtering data with condition: {condition}")
        
        try:
            result = df.query(condition)
            logger.info(f"Filtered from {len(df)} to {len(result)} rows")
            return result
        
        except Exception as e:
            logger.error(f"Error filtering data: {e}")
            raise
    
    def transform_sales_data(
        self,
        orders_df: pd.DataFrame,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform sales data: join orders with customers and products, calculate totals
        
        Args:
            orders_df: Orders DataFrame
            customers_df: Customers DataFrame
            products_df: Products DataFrame
        
        Returns:
            Transformed sales DataFrame
        """
        logger.info("Starting sales data transformation")
        
        # Step 1: Join orders with customers
        logger.info("Step 1: Joining orders with customers")
        df = self.join_data(
            orders_df,
            customers_df,
            left_on='customer_id',
            right_on='customer_id',
            how='left'
        )
        
        # Step 2: Join with products
        logger.info("Step 2: Enriching with product details")
        df = self.enrich_with_lookup(
            df,
            products_df,
            df_key='product_id',
            lookup_key='product_id',
            lookup_columns=['product_name', 'category', 'brand', 'price'],
            prefix='product'
        )
        
        # Step 3: Calculate fields
        logger.info("Step 3: Calculating derived fields")
        df = self.calculate_fields(df, {
            'total_amount': 'quantity * unit_price',
            'discount_amount': 'total_amount * discount / 100',
            'final_amount': 'total_amount - discount_amount'
        })
        
        logger.info(f"Sales data transformation completed: {len(df)} rows")
        return df
    
    def aggregate_daily_sales(
        self,
        sales_df: pd.DataFrame,
        date_column: str = 'order_date'
    ) -> pd.DataFrame:
        """
        Aggregate sales by day
        
        Args:
            sales_df: Sales DataFrame
            date_column: Name of date column
        
        Returns:
            Daily aggregated sales DataFrame
        """
        logger.info("Aggregating daily sales")
        
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(sales_df[date_column]):
            sales_df[date_column] = pd.to_datetime(sales_df[date_column])
        
        # Extract date (without time)
        sales_df['date'] = sales_df[date_column].dt.date
        
        # Aggregate
        result = self.aggregate_data(
            sales_df,
            group_by=['date'],
            aggregations={
                'quantity': ['sum'],
                'total_amount': ['sum'],
                'final_amount': ['sum'],
                'order_id': ['count']  # Number of orders
            }
        )
        
        # Rename columns
        result = result.rename(columns={'order_id_count': 'order_count'})
        
        logger.info(f"Daily sales aggregation completed: {len(result)} days")
        return result


def transform_data(
    orders_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Convenience function to transform sales data
    
    Args:
        orders_df: Orders DataFrame
        customers_df: Customers DataFrame
        products_df: Products DataFrame
    
    Returns:
        Transformed sales DataFrame
    """
    transformer = DataTransformer()
    return transformer.transform_sales_data(orders_df, customers_df, products_df)

