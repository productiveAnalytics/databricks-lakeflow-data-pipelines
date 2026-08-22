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
├── setup/
│   └── create_catalog.sql                   # 🔧 Unity Catalog creation script
├── resources/
│   ├── schemas.yml
│   ├── volumes.yml
│   ├── pipelines.yml
│   └── jobs.yml
├── src/
│   ├── data/customer_cdc.jsonl
│   ├── common/
│   │   ├── bootstrap_sample_data.py
│   │   ├── scd_transformations.py           # ✅ Testable PySpark functions
│   │   ├── test_scd_transformations.py      # ✅ Unit tests (pytest)
│   │   └── README_LOCAL_TESTING.md          # Local testing guide
│   ├── type1_sql/pipeline.sql
│   ├── type1_python/pipeline.py             # ⚙️ Thin Lakeflow wrapper
│   ├── type2_sql/pipeline.sql
│   └── type2_python/pipeline.py             # ⚙️ Thin Lakeflow wrapper
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
4. A Unity Catalog where you can create schemas and volumes.
5. Permissions to create Lakeflow pipelines and the output tables.

### Setup: Create the Unity Catalog

Before deploying the bundle, create the target Unity Catalog and grant necessary permissions:

```bash
# Option 1: Run the setup script in Databricks SQL Editor or notebook
# Copy and paste the contents of setup/create_catalog.sql

# Option 2: Run via Databricks CLI
databricks sql statements execute \
  --warehouse-id <your-warehouse-id> \
  --sql-file setup/create_catalog.sql
```

**Important**: Edit `setup/create_catalog.sql` to replace `lalitstar@gmail.com` with your username or grant permissions to a group.

The script creates:
- Catalog `lakeflow_scd_demo` (if not exists)
- Grants: `USE CATALOG`, `CREATE SCHEMA`, `USE SCHEMA`

The bundle then creates the schemas and a managed Unity Catalog volume within this catalog. Existing schemas with the same names can be used instead by removing the schema resources and retaining the configured catalog.

## Configure the bundle

Edit `databricks.yml` and set the catalog name (must match the catalog created in the setup step):

```yaml
variables:
  catalog:
    default: lakeflow_scd_demo  # Or use your existing catalog name
```

**Note**: The catalog must already exist with appropriate permissions before deployment. If you used a different catalog name in the setup script, update this value accordingly.

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

**Prerequisites**: Ensure you've created the Unity Catalog using the setup script (see Prerequisites section above).

```bash
# Validate the bundle configuration
databricks bundle validate -t dev

# Deploy all resources
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

## Architecture: Testable Logic + Lakeflow Orchestration

### ⚠️ Important: AUTO CDC is Lakeflow-Specific

**`AUTO CDC` and related Lakeflow APIs (`dp.create_auto_cdc_flow()`, `CREATE FLOW ... AS AUTO CDC`) are Databricks proprietary features that require the Databricks Lakeflow runtime.** These capabilities are NOT available in:

- Local PySpark environments
- Standard Apache Spark
- Non-Databricks platforms (AWS EMR, Azure Synapse, Google Dataproc, etc.)

The AUTO CDC engine provides advanced features that cannot be replicated outside Databricks:
- Out-of-order CDC event sequencing
- Automatic SCD Type 1 and Type 2 versioning
- Idempotent processing with state management
- Optimized late-arriving data handling

### Refactored for Local Development

Following [Databricks best practices for local development](https://docs.databricks.com/aws/en/ldp/develop-locally), the **Python pipelines** have been refactored to separate testable logic from Lakeflow-specific orchestration:

```
┌──────────────────────────────────────────────────────────┐
│  src/common/scd_transformations.py                       │
│  ✅ Pure PySpark functions (testable locally)            │
│  • transform_bronze_cdc()                                │
│  • create_gold_view_scd1()                               │
│  • create_gold_view_scd2()                               │
│  • filter_current_records_scd2()                         │
│  • validate_cdc_operations()                             │
└──────────────────────────────────────────────────────────┘
                      ▲
                      │ imports
                      │
┌──────────────────────────────────────────────────────────┐
│  src/type1_python/pipeline.py                            │
│  src/type2_python/pipeline.py                            │
│  ⚙️  Thin Lakeflow wrappers                              │
│  • @dp.table decorators                                  │
│  • dp.create_auto_cdc_flow() ⚠️ Lakeflow-only            │
│  • @dp.materialized_view                                 │
└──────────────────────────────────────────────────────────┘
```

### What Can Be Tested Locally?

| Component | Testable Locally? | Location |
|-----------|-------------------|----------|
| Bronze transformation (type casting, validation) | ✅ Yes | `scd_transformations.py` |
| Gold view creation (filtering, projection) | ✅ Yes | `scd_transformations.py` |
| Data quality checks | ✅ Yes | `scd_transformations.py` |
| **AUTO CDC logic** | ❌ No - Lakeflow-only | `pipeline.py` (wrapper) |
| **Auto Loader (cloudFiles)** | ❌ No - Databricks service | `pipeline.py` (wrapper) |
| **Streaming decorators** | ❌ No - Lakeflow runtime | `pipeline.py` (wrapper) |

### Local Testing Guide

The Python pipelines include:

1. **[scd_transformations.py](src/common/scd_transformations.py)** - Testable PySpark functions
2. **[test_scd_transformations.py](src/common/test_scd_transformations.py)** - Unit tests (pytest)
3. **[README_LOCAL_TESTING.md](src/common/README_LOCAL_TESTING.md)** - Complete testing guide

#### Quick Local Setup & Test

```bash
# Install dependencies with uv (recommended)
curl -LsSf https://astral.sh/uv/install.sh | sh  # Install uv if needed
uv pip install -e ".[local]"  # Install with PySpark 4.1+, Delta Spark, pytest-spark

# Run tests
pytest src/common/test_scd_transformations.py -v

# Or using pip
pip install -e ".[local]"
pytest src/common/test_scd_transformations.py -v
```

**⚠️ Important:** Declarative Lakeflow pipelines require **Apache Spark 4.1+** for the `pyspark.pipelines` module. This allows you to test pipeline wrapper files locally. However, Databricks-specific extensions (`dp.create_auto_cdc_flow`, `cloudFiles`, DQX expectations) remain Databricks-only. See [README_LOCAL_TESTING.md](src/common/README_LOCAL_TESTING.md) for details.

```python
# Inside Databricks notebook
import sys
sys.path.append("/Workspace/Users/<your-email>/databricks-lakeflow-data-pipelines/src/common")
from scd_transformations import transform_bronze_cdc

# Test with sample data
result = transform_bronze_cdc(test_df)
result.show()
```

### Benefits of This Architecture

✅ **Fast Iteration** - Test transformations locally without deploying (seconds vs minutes)  
✅ **Lower Cost** - No pipeline compute charges during development  
✅ **CI/CD Ready** - Tests run in GitHub Actions / Jenkins without Databricks  
✅ **Better Design** - Clear separation between business logic and orchestration  
✅ **Portable** - Transformation logic can work outside Databricks (with manual CDC implementation)  

### Platform Compatibility Summary

| Feature | Databricks Lakeflow | Local PySpark | Other Spark Platforms |
|---------|---------------------|---------------|----------------------|
| Transformation functions | ✅ | ✅ | ✅ |
| Unit tests | ✅ | ✅ | ✅ |
| AUTO CDC | ✅ | ❌ | ❌ |
| Auto Loader | ✅ | ❌ | ❌ |
| SCD Type 1/2 (manual) | ✅ | ✅ (simulation) | ✅ (custom ETL) |

**For non-Databricks deployments:** Use the testable transformation functions as a foundation and implement manual CDC/SCD logic using standard Spark operations (window functions, MERGE statements, Delta Lake operations, or equivalent).

### SQL Pipelines

The SQL pipelines (`src/type1_sql/pipeline.sql`, `src/type2_sql/pipeline.sql`) are **not refactored** because:
- SQL is inherently declarative and does not have the same local testing workflow as Python
- The entire SQL pipeline uses Lakeflow-specific syntax (`CREATE FLOW`, `AS AUTO CDC`)
- SQL transformations can be tested via `executeCode` in Databricks notebooks

For SQL pipeline development, use Databricks notebooks with serverless compute for rapid iteration.
