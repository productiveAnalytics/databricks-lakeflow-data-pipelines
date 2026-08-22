"""
Local Unit Tests for SCD Transformation Functions

These tests can run with standard PySpark locally (no Databricks runtime needed).

To run locally:
1. Install: pip install pyspark pytest pytest-spark
2. Run: pytest test_scd_transformations.py -v

Or run interactively in this notebook for quick validation.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from scd_transformations import (
    transform_bronze_cdc,
    create_gold_view_scd1,
    create_gold_view_scd2,
    filter_current_records_scd2,
    validate_cdc_operations,
    simulate_scd_type1_manual,
)


# ============================================================================
# Test Setup
# ============================================================================

@pytest.fixture(scope="session")
def spark():
    """Create local Spark session for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("SCD_Transformations_Test") \
        .getOrCreate()


@pytest.fixture
def sample_cdc_data(spark):
    """Create sample CDC data for testing."""
    data = [
        ("C001", "Alice", "Nguyen", "alice@example.com", "Chicago", "IL", "INSERT", 1000),
        ("C002", "Bob", "Carter", "bob@example.com", "Austin", "TX", "INSERT", 1001),
        ("C001", "Alice", "Nguyen", "alice@example.com", "Dallas", "TX", "UPDATE", 1003),
        ("C002", "Bob", "Carter", "bob@example.com", None, None, "DELETE", 1010),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("sequence_num", LongType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_scd2_data(spark):
    """Create sample SCD Type 2 silver data for testing."""
    data = [
        ("C001", "Alice", "Nguyen", "alice@example.com", "Chicago", "IL", 1000, 1003),
        ("C001", "Alice", "Nguyen", "alice@example.com", "Dallas", "TX", 1003, None),
        ("C002", "Bob", "Carter", "bob@example.com", "Austin", "TX", 1001, 1010),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("__START_AT", LongType(), True),
        StructField("__END_AT", LongType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ============================================================================
# Bronze Layer Tests
# ============================================================================

def test_transform_bronze_cdc(spark, sample_cdc_data):
    """Test bronze CDC transformation maintains schema and data."""
    result = transform_bronze_cdc(sample_cdc_data)
    
    # Check schema
    assert "customer_id" in result.columns
    assert "operation" in result.columns
    assert "sequence_num" in result.columns
    
    # Check data types
    assert result.schema["customer_id"].dataType.typeName() == "string"
    assert result.schema["sequence_num"].dataType.typeName() == "long"
    
    # Check row count preserved
    assert result.count() == 4
    
    print("✓ Bronze transformation test passed")


def test_validate_cdc_operations(spark, sample_cdc_data):
    """Test CDC operation validation."""
    # Add an invalid operation
    from pyspark.sql import Row
    invalid_row = Row(customer_id="C003", first_name="Carol", last_name="Diaz",
                     email="carol@example.com", city="Denver", state="CO",
                     operation="INVALID", sequence_num=1020)
    df_with_invalid = sample_cdc_data.union(spark.createDataFrame([invalid_row], sample_cdc_data.schema))
    
    result = validate_cdc_operations(df_with_invalid)
    
    # Should filter out the invalid operation
    assert result.count() == 4
    assert result.filter("operation = 'INVALID'").count() == 0
    
    print("✓ CDC operation validation test passed")


# ============================================================================
# Gold Layer Tests
# ============================================================================

def test_create_gold_view_scd1(spark, sample_cdc_data):
    """Test SCD Type 1 gold view creation."""
    # Simulate SCD Type 1 (latest state only)
    df_silver = simulate_scd_type1_manual(sample_cdc_data, ["customer_id"])
    
    result = create_gold_view_scd1(df_silver)
    
    # Should have only business columns
    assert "customer_id" in result.columns
    assert "first_name" in result.columns
    assert "operation" not in result.columns
    assert "sequence_num" not in result.columns
    
    # Should have only current records (C002 was deleted)
    assert result.count() == 1
    assert result.filter("customer_id = 'C001'").count() == 1
    assert result.filter("customer_id = 'C002'").count() == 0  # Deleted
    
    print("✓ SCD Type 1 gold view test passed")


def test_filter_current_records_scd2(spark, sample_scd2_data):
    """Test filtering current records from SCD Type 2 table."""
    result = filter_current_records_scd2(sample_scd2_data)
    
    # Should have only records where __END_AT IS NULL
    assert result.count() == 1
    assert result.filter("customer_id = 'C001'").count() == 1
    assert result.filter("city = 'Dallas'").count() == 1  # Latest version
    
    print("✓ SCD Type 2 current records filter test passed")


def test_create_gold_view_scd2(spark, sample_scd2_data):
    """Test SCD Type 2 gold view creation."""
    result = create_gold_view_scd2(sample_scd2_data)
    
    # Should have only business columns
    assert "customer_id" in result.columns
    assert "__START_AT" not in result.columns
    assert "__END_AT" not in result.columns
    
    # Should have only current records
    assert result.count() == 1
    assert result.filter("customer_id = 'C001'").count() == 1
    
    # Verify it's the latest version
    row = result.filter("customer_id = 'C001'").collect()[0]
    assert row["city"] == "Dallas"  # Latest update
    
    print("✓ SCD Type 2 gold view test passed")


# ============================================================================
# Integration Tests
# ============================================================================

def test_end_to_end_scd1_pipeline(spark, sample_cdc_data):
    """Test complete SCD Type 1 pipeline flow."""
    # Bronze transformation
    df_bronze = transform_bronze_cdc(sample_cdc_data)
    assert df_bronze.count() == 4
    
    # Silver (manual simulation for testing)
    df_silver = simulate_scd_type1_manual(df_bronze, ["customer_id"])
    assert df_silver.count() == 1  # C001 current, C002 deleted
    
    # Gold view
    df_gold = create_gold_view_scd1(df_silver)
    assert df_gold.count() == 1
    assert "operation" not in df_gold.columns
    
    print("✓ End-to-end SCD Type 1 pipeline test passed")


def test_end_to_end_scd2_pipeline(spark, sample_scd2_data):
    """Test complete SCD Type 2 pipeline flow."""
    # Verify silver has history
    assert sample_scd2_data.count() == 3  # 2 versions of C001 + 1 of C002
    
    # Gold view (current only)
    df_gold = create_gold_view_scd2(sample_scd2_data)
    assert df_gold.count() == 1  # Only C001 current
    assert "__START_AT" not in df_gold.columns
    
    print("✓ End-to-end SCD Type 2 pipeline test passed")


# ============================================================================
# Run Tests Interactively (for Databricks notebooks)
# ============================================================================

if __name__ == "__main__":
    print("Running SCD Transformation Tests...\n")
    
    # Create local Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("SCD_Test") \
        .getOrCreate()
    
    # Create test data
    cdc_data = [
        ("C001", "Alice", "Nguyen", "alice@example.com", "Chicago", "IL", "INSERT", 1000),
        ("C002", "Bob", "Carter", "bob@example.com", "Austin", "TX", "INSERT", 1001),
        ("C001", "Alice", "Nguyen", "alice@example.com", "Dallas", "TX", "UPDATE", 1003),
        ("C002", "Bob", "Carter", "bob@example.com", None, None, "DELETE", 1010),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("sequence_num", LongType(), True),
    ])
    df_cdc = spark.createDataFrame(cdc_data, schema)
    
    # Run tests
    print("Test 1: Bronze Transformation")
    df_bronze = transform_bronze_cdc(df_cdc)
    print(f"  Input: {df_cdc.count()} rows")
    print(f"  Output: {df_bronze.count()} rows")
    df_bronze.show()
    
    print("\nTest 2: SCD Type 1 Manual Simulation")
    df_scd1 = simulate_scd_type1_manual(df_bronze, ["customer_id"])
    print(f"  Current state: {df_scd1.count()} customers")
    df_scd1.show()
    
    print("\nTest 3: Gold View (SCD Type 1)")
    df_gold1 = create_gold_view_scd1(df_scd1)
    df_gold1.show()
    
    print("\n✅ All tests completed successfully!")
    print("\nTo run with pytest:")
    print("  pip install pytest pytest-spark")
    print("  pytest test_scd_transformations.py -v")