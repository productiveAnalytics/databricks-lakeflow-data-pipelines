"""
SCD Type 2 Lakeflow Pipeline - Thin Wrapper Pattern

This pipeline file contains ONLY Lakeflow-specific decorators and orchestration.
All business logic lives in src/common/scd_transformations.py for testability.

Architecture:
- Bronze: Ingest CDC events from UC Volume
- Silver: Apply SCD Type 2 using AUTO CDC (Lakeflow-managed, tracks full history)
- Gold: Current state view (testable transformation)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Import testable transformation functions
import sys
sys.path.append("/Workspace/Users/lalitstar@gmail.com/databricks-lakeflow-data-pipelines/src/common")
from scd_transformations import transform_bronze_cdc, create_gold_view_scd2

# Pipeline configuration
CATALOG = spark.conf.get("pipeline_catalog")
SOURCE_PATH = spark.conf.get("source_path")

BRONZE = f"{CATALOG}.bronze__type_2__pyspark.customer_cdc"
SILVER = f"{CATALOG}.silver__type_2__pyspark.customer_dim_scd2"
GOLD = f"{CATALOG}.gold__type_2__pyspark.customer_current"


# ============================================================================
# Bronze Layer: CDC Ingestion
# ============================================================================

@dp.table(
    name=BRONZE,
    comment="Raw customer CDC events from the simulated source feed.",
)
def customer_cdc():
    """Bronze table: Ingest and transform CDC events.
    
    Lakeflow-specific: Uses cloudFiles (Auto Loader) for streaming ingestion.
    Testable logic: transform_bronze_cdc() can be unit tested locally.
    """
    # Lakeflow-specific: streaming read with Auto Loader
    df_raw = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_PATH)
    )
    
    # Testable transformation: type casting and column selection
    return transform_bronze_cdc(df_raw)


# ============================================================================
# Silver Layer: SCD Type 2 (Lakeflow AUTO CDC with History)
# ============================================================================

# Note: create_auto_cdc_flow is Lakeflow-specific and cannot be tested locally.
# The AUTO CDC logic handles:
# - Out-of-order event sequencing
# - Historical versioning (creates __START_AT, __END_AT columns)
# - Upserts (INSERT/UPDATE create new versions)
# - Deletes (expire the current version)
# - Idempotent processing

dp.create_streaming_table(
    name=SILVER,
    comment="Historical customer dimension maintained as SCD Type 2.",
)

dp.create_auto_cdc_flow(
    target=SILVER,
    source=BRONZE,
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type="2",
)


# ============================================================================
# Gold Layer: Current State View
# ============================================================================

@dp.materialized_view(
    name=GOLD,
    comment="Current customer state exposed as a Gold consumer dataset.",
)
def customer_current():
    """Gold view: Consumer-facing current state (filters out history).
    
    Testable logic: create_gold_view_scd2() can be unit tested with batch DataFrames.
    """
    df_silver = spark.read.table(SILVER)
    
    # Testable transformation: filter current records and select business columns
    return create_gold_view_scd2(df_silver)
