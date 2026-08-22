"""
Pure PySpark transformation functions for SCD pipelines.

These functions contain no Lakeflow-specific code and can be:
1. Unit tested locally with standard PySpark
2. Integrated tested with pytest-spark
3. Used in both Lakeflow pipelines and standalone PySpark jobs

Dependencies: Only pyspark.sql (no databricks-specific imports)
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


# ============================================================================
# Bronze Layer: CDC Ingestion Transformations
# ============================================================================

def get_customer_cdc_schema() -> StructType:
    """Define the expected CDC schema for customer data.
    
    Returns:
        StructType with all customer CDC fields
    """
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("sequence_num", LongType(), True),
    ])


def transform_bronze_cdc(df: DataFrame) -> DataFrame:
    """Transform raw CDC data into bronze schema.
    
    Applies type casting and column selection for customer CDC events.
    This function is idempotent and can be tested with batch DataFrames.
    
    Args:
        df: Raw DataFrame from source (streaming or batch)
        
    Returns:
        DataFrame with standardized bronze schema
    """
    return df.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("first_name").cast("string").alias("first_name"),
        F.col("last_name").cast("string").alias("last_name"),
        F.col("email").cast("string").alias("email"),
        F.col("city").cast("string").alias("city"),
        F.col("state").cast("string").alias("state"),
        F.col("operation").cast("string").alias("operation"),
        F.col("sequence_num").cast("long").alias("sequence_num"),
    )


# ============================================================================
# Gold Layer: Consumer-Facing Transformations
# ============================================================================

def select_business_columns(df: DataFrame, columns: List[str] = None) -> DataFrame:
    """Select only business columns (exclude SCD metadata).
    
    Args:
        df: Silver DataFrame (may contain __START_AT, __END_AT, etc.)
        columns: List of business columns to select. 
                 Default: ["customer_id", "first_name", "last_name", "email", "city", "state"]
        
    Returns:
        DataFrame with only business columns
    """
    if columns is None:
        columns = ["customer_id", "first_name", "last_name", "email", "city", "state"]
    
    return df.select(*columns)


def filter_current_records_scd2(df: DataFrame, end_column: str = "__END_AT") -> DataFrame:
    """Filter SCD Type 2 table to get only current (active) records.
    
    Args:
        df: SCD Type 2 DataFrame with temporal columns
        end_column: Name of the end-date column (default: __END_AT)
        
    Returns:
        DataFrame containing only current records (end_column IS NULL)
    """
    return df.filter(F.col(end_column).isNull())


def create_gold_view_scd1(df_silver: DataFrame) -> DataFrame:
    """Create Gold view from SCD Type 1 silver table.
    
    SCD Type 1 contains only current state, so just select business columns.
    
    Args:
        df_silver: Silver SCD Type 1 DataFrame
        
    Returns:
        Gold DataFrame with business columns only
    """
    return select_business_columns(df_silver)


def create_gold_view_scd2(df_silver: DataFrame) -> DataFrame:
    """Create Gold view from SCD Type 2 silver table.
    
    SCD Type 2 contains history, so filter to current records then select business columns.
    
    Args:
        df_silver: Silver SCD Type 2 DataFrame (with __START_AT, __END_AT)
        
    Returns:
        Gold DataFrame with current records and business columns only
    """
    df_current = filter_current_records_scd2(df_silver)
    return select_business_columns(df_current)


# ============================================================================
# Data Quality & Validation Functions
# ============================================================================

def validate_cdc_operations(df: DataFrame, operation_column: str = "operation") -> DataFrame:
    """Validate that CDC operations are in expected set.
    
    Args:
        df: DataFrame with CDC operation column
        operation_column: Name of operation column
        
    Returns:
        DataFrame with only valid operations (INSERT, UPDATE, DELETE)
    """
    valid_operations = ["INSERT", "UPDATE", "DELETE"]
    return df.filter(F.col(operation_column).isin(valid_operations))


def add_data_quality_flags(df: DataFrame) -> DataFrame:
    """Add data quality flags for monitoring.
    
    Args:
        df: Customer CDC DataFrame
        
    Returns:
        DataFrame with quality flags added
    """
    return df.withColumn(
        "is_valid_email",
        F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    ).withColumn(
        "is_complete",
        F.col("customer_id").isNotNull() & 
        F.col("first_name").isNotNull() & 
        F.col("last_name").isNotNull()
    )


# ============================================================================
# Testing Utilities
# ============================================================================

def simulate_scd_type1_manual(df_cdc: DataFrame, key_columns: List[str]) -> DataFrame:
    """Manually simulate SCD Type 1 logic for testing (without Lakeflow AUTO CDC).
    
    This is a simplified implementation for local testing.
    In production, use Lakeflow's create_auto_cdc_flow for proper handling.
    
    Args:
        df_cdc: CDC events DataFrame (must be sorted by sequence_num)
        key_columns: Primary key column(s)
        
    Returns:
        DataFrame with latest state per key (SCD Type 1)
    """
    from pyspark.sql.window import Window
    
    # Get latest non-DELETE record per customer
    window_spec = Window.partitionBy(*key_columns).orderBy(F.desc("sequence_num"))
    
    return (
        df_cdc
        .filter(F.col("operation") != "DELETE")
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank", "operation", "sequence_num")
    )


def simulate_scd_type2_manual(df_cdc: DataFrame, key_columns: List[str]) -> DataFrame:
    """Manually simulate SCD Type 2 logic for testing (without Lakeflow AUTO CDC).
    
    This is a simplified implementation for local testing.
    In production, use Lakeflow's create_auto_cdc_flow for proper handling.
    
    Args:
        df_cdc: CDC events DataFrame (must be sorted by sequence_num)
        key_columns: Primary key column(s)
        
    Returns:
        DataFrame with historical records including __START_AT, __END_AT
    """
    from pyspark.sql.window import Window
    
    # Calculate __START_AT and __END_AT for each version
    window_spec = Window.partitionBy(*key_columns).orderBy("sequence_num")
    
    df_with_end = (
        df_cdc
        .filter(F.col("operation") != "DELETE")
        .withColumn("__START_AT", F.col("sequence_num"))
        .withColumn("__END_AT", F.lead("sequence_num").over(window_spec))
        .drop("operation", "sequence_num")
    )
    
    # Handle DELETEs: set __END_AT for the previous active record
    # (Simplified - full implementation requires more complex logic)
    
    return df_with_end