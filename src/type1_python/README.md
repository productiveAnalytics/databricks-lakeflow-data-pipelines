# SCD Type 1 + PySpark

This implementation uses Lakeflow's Python pipeline interface (`pyspark.pipelines`) with normal PySpark DataFrame APIs for Bronze ingestion and Gold transformations.

## Run

```bash
databricks bundle run -t dev bootstrap_sample_data
databricks bundle run -t dev scd1_python_pipeline
databricks bundle run -t dev scd1_python_dqx
```

## Data flow

- Bronze: Spark Structured Streaming + Auto Loader from the Unity Catalog Volume.
- Silver: `dp.create_auto_cdc_flow(..., stored_as_scd_type="1")`.
- Gold: materialized view selecting the current Silver state.

The Python implementation keeps the same business semantics as the SQL pipeline so the two implementations can be compared directly.
