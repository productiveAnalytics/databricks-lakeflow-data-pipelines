# SCD Type 2 + SQL

## Flow

```text
UC Volume JSON CDC
       |
       v
bronze__type_2__sql.customer_cdc
       |
       | AUTO CDC, key=customer_id, sequence=sequence_num
       v
silver__type_2__sql.customer_dim_scd2
       |
       | __START_AT / __END_AT
       v
 gold__type_2__sql.customer_current
```

## Run

```bash
databricks bundle run -t dev bootstrap_sample_data
databricks bundle run -t dev scd2_sql_pipeline
databricks bundle run -t dev scd2_sql_dqx
```

SCD2 preserves historical versions. Lakeflow creates `__START_AT` and `__END_AT`; the Gold dataset filters on `__END_AT IS NULL` to expose only the current version.
