-- Lakeflow SQL: SCD Type 2 + SQL
-- Source: CDC JSON files from the UC Volume.
-- Bronze preserves the source CDC events. Silver is maintained by AUTO CDC.
-- Gold exposes a consumer-friendly current-state view.

CREATE OR REFRESH STREAMING TABLE bronze__type_2__sql.customer_cdc
COMMENT "Raw customer CDC events from the simulated source feed."
AS
SELECT
  CAST(customer_id AS STRING) AS customer_id,
  CAST(first_name AS STRING) AS first_name,
  CAST(last_name AS STRING) AS last_name,
  CAST(email AS STRING) AS email,
  CAST(city AS STRING) AS city,
  CAST(state AS STRING) AS state,
  CAST(operation AS STRING) AS operation,
  CAST(sequence_num AS BIGINT) AS sequence_num
FROM STREAM read_files(
  :source_path,
  format => "json",
  inferColumnTypes => true
);

CREATE OR REFRESH STREAMING TABLE silver__type_2__sql.customer_dim_scd2
COMMENT "Historical customer dimension maintained as SCD Type 2.";

CREATE FLOW customer_cdc_flow_scd2
AS AUTO CDC INTO silver__type_2__sql.customer_dim_scd2
FROM STREAM(bronze__type_2__sql.customer_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 2;

-- Ignore volatile operational attributes if you add them to the source by using TRACK HISTORY ON * EXCEPT (...).

CREATE OR REFRESH MATERIALIZED VIEW gold__type_2__sql.customer_current
COMMENT "Current customer state exposed as a Gold consumer dataset."
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  state
FROM silver__type_2__sql.customer_dim_scd2
WHERE __END_AT IS NULL;
