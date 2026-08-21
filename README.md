# Databricks Lakeflow SCD Type 1 / Type 2 Reference Repository

A GitHub-ready reference implementation of modern Databricks Lakeflow pipelines for CDC-driven Slowly Changing Dimensions (SCD), with:

- Lakeflow `AUTO CDC` for SCD Type 1 and SCD Type 2
- Four independent pipelines:
  - SCD1 + SQL
  - SCD1 + PySpark
  - SCD2 + SQL
  - SCD2 + PySpark
- Bronze / Silver / Gold medallion architecture
- Unity Catalog schemas named `<layer>__type_<1|2>__<sql|pyspark>`
- A deterministic sample CDC stream containing INSERT, UPDATE, DELETE and intentionally out-of-order update records
- DQX declarative YAML checks at Bronze, Silver and Gold
- DQX quarantine outputs and summary metrics tables
- Databricks Declarative Automation Bundle (DAB) deployment
- Unit tests for the sample CDC expectations and repository configuration

## Current Databricks APIs used

This repository intentionally uses the current Lakeflow naming:

- Python: `from pyspark import pipelines as dp`
- CDC: `dp.create_auto_cdc_flow(...)`
- SQL: `CREATE FLOW ... AS AUTO CDC ...`
- SCD: `stored_as_scd_type="1"` / `STORED AS SCD TYPE 1` and `"2"`
- Deployment: Declarative Automation Bundles (formerly Databricks Asset Bundles)
- DQX: `databricks-labs-dqx==0.15.0`

## Repository layout

```text
.
├── databricks.yml
├── resources/
│   ├── schemas.yml
│   ├── volumes.yml
│   ├── pipelines.yml
│   └── jobs.yml
├── src/
│   ├── data/customer_cdc.jsonl
│   ├── common/bootstrap_sample_data.py
│   ├── type1_sql/pipeline.sql
│   ├── type1_python/pipeline.py
│   ├── type2_sql/pipeline.sql
│   └── type2_python/pipeline.py
├── dqx/
│   ├── checks/<implementation>/<layer>.yml
│   └── scripts/run_dqx.py
├── docs/
│   ├── architecture.md
│   └── expected-results.md
└── tests/
    └── test_repository.py
```

## Architecture

Each implementation publishes into three Unity Catalog schemas. For example:

```text
catalog
├── bronze__type_1__sql.customer_cdc
├── silver__type_1__sql.customer_dim_scd1
└── gold__type_1__sql.customer_current
```

The SCD2 SQL implementation is analogous:

```text
catalog
├── bronze__type_2__sql.customer_cdc
├── silver__type_2__sql.customer_dim_scd2
└── gold__type_2__sql.customer_current
```

The four implementations are intentionally isolated so that you can run, compare and benchmark them independently.

## Prerequisites

1. A Databricks workspace with Unity Catalog enabled.
2. Lakeflow pipeline support with `AUTO CDC`; Databricks documents `AUTO CDC` as requiring the Pro or Advanced pipeline edition.
3. Databricks CLI with Declarative Automation Bundles support.
4. A catalog where you can create schemas and volumes.
5. Permissions to create Lakeflow pipelines and the output tables.

The bundle creates the schemas and a managed Unity Catalog volume. Existing schemas with the same names can be used instead by removing the schema resources and retaining the configured catalog.

## Configure the bundle

Edit `databricks.yml` and replace the example catalog value:

```yaml
variables:
  catalog:
    default: main
```

The default target is `dev` and deploys using serverless pipeline compute.

### Why the `dev` target does not use `mode: development`

`mode: development` prefixes every resource name with `dev_<short_name>_`, and that
prefix is applied to Unity Catalog schemas — which also relocates the managed volume to
`/Volumes/<catalog>/dev_<user>_demo__input/...`. The pipeline sources address their
Bronze and Silver schemas by literal name, so under the prefix the pipelines would write
Bronze and Silver into schemas the bundle never created while publishing Gold into the
prefixed one, and every DQX job would then fail on a missing table.
(`presets.name_prefix: ""` does not suppress it — an empty string is treated as unset.)

The `dev` target therefore sets `presets.pipelines_development: true` explicitly instead,
which keeps fast pipeline startup without renaming anything. To isolate two developers,
give them different catalogs rather than different prefixes:

```bash
databricks bundle deploy -t dev --var catalog=my_sandbox
```

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The bundle provisions:

- 12 implementation Unity Catalog schemas plus 1 shared input schema
- 1 managed volume for the sample CDC files
- 4 Lakeflow pipelines
- 1 bootstrap job that loads the sample CDC files
- 4 DQX validation jobs, one per implementation

## Run an implementation

Example for SCD Type 1 + SQL:

```bash
databricks bundle run -t dev bootstrap_sample_data
```

Then start:

```bash
databricks bundle run -t dev scd1_sql_pipeline
```

After the pipeline completes, run the DQX validation job:

```bash
databricks bundle run -t dev scd1_sql_dqx
```

The same pattern applies to `scd1_python`, `scd2_sql`, and `scd2_python`.

## Implementation-level READMEs

- [SCD Type 1 + SQL](src/type1_sql/README.md)
- [SCD Type 1 + PySpark](src/type1_python/README.md)
- [SCD Type 2 + SQL](src/type2_sql/README.md)
- [SCD Type 2 + PySpark](src/type2_python/README.md)
- [DQX](dqx/README.md)

## What to inspect after a run

For SCD1, the Silver table contains only the latest state per customer. For SCD2, the Silver table contains the historical versions with `__START_AT` and `__END_AT`; `NULL` `__END_AT` identifies the current version.

The DQX jobs create three outputs per layer:

```text
<table>__dq_output
<table>__dq_quarantine
<table>__dq_metrics
```

The metrics table contains row-level counts and per-check quality metrics. Quarantined rows preserve the DQX error/warning metadata for troubleshooting.

## Resetting the demo

Because Lakeflow checkpoints preserve progress, use a full pipeline reset when you want to replay the sample source from the beginning. Delete/recreate the demo input files using the bootstrap job, then reset and rerun the desired pipeline from the Databricks UI or CLI.

## Why AUTO CDC instead of hand-written MERGE

This repository is deliberately designed around `AUTO CDC`. The source provides a business key, change operation and sequence column. Lakeflow handles out-of-order CDC sequencing, SCD versioning and delete semantics without manually building a `foreachBatch` + `MERGE` state machine.

## License

MIT
