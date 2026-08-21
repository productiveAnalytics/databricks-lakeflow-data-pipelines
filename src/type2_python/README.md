# SCD Type 2 + PySpark

This implementation uses the Lakeflow Python pipeline interface plus standard PySpark transformations and `dp.create_auto_cdc_flow(..., stored_as_scd_type="2")`.

## Run

```bash
databricks bundle run -t dev bootstrap_sample_data
databricks bundle run -t dev scd2_python_pipeline
databricks bundle run -t dev scd2_python_dqx
```

The Silver target retains the full customer history. The Gold materialized view filters `__END_AT IS NULL` so downstream users receive current-state customers without losing the Silver history.
