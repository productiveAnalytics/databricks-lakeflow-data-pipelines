# SCD Type 1 + SQL

## Flow

```text
UC Volume JSON CDC
       |
       v
bronze__type_1__sql.customer_cdc
       |
       | AUTO CDC, key=customer_id, sequence=sequence_num
       v
silver__type_1__sql.customer_dim_scd1
       |
       v
 gold__type_1__sql.customer_current
```

## Run

```bash
databricks bundle run -t dev bootstrap_sample_data
databricks bundle run -t dev scd1_sql_pipeline
databricks bundle run -t dev scd1_sql_dqx
```

## Key Lakeflow SQL

The Silver table uses:

```sql
CREATE FLOW customer_cdc_flow_scd1
AS AUTO CDC INTO silver__type_1__sql.customer_dim_scd1
FROM STREAM(bronze__type_1__sql.customer_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;
```

SCD1 keeps the latest state only. The repository intentionally includes out-of-order sequence numbers so the demo exercises Lakeflow's sequencing behavior rather than simple file-arrival order.
