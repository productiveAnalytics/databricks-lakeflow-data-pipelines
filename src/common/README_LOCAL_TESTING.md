# Local Development Setup

This guide explains how to set up and test the SCD transformation logic locally using PySpark.

## Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager (recommended) or pip



## Compatibility & Version Notes

### ✅ Apache Spark 4.1+ Required for Declarative Pipelines

According to the [Apache Spark Declarative Pipelines documentation](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html):

> **Apache Spark includes declarative pipelines beginning in Spark 4.1**, available through the pyspark.pipelines module.

**This project now uses PySpark 4.1+ for local testing**, which enables:
- ✅ Importing pipeline wrapper files (`src/type*_python/pipeline.py`)
- ✅ Using `from pyspark import pipelines as dp`
- ✅ Testing decorators: `@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`
- ✅ Full standard DataFrame API compatibility

**What remains Databricks-only** (not part of Apache Spark):
- ❌ `dp.create_auto_cdc_flow()` - Databricks extension for CDC/SCD
- ❌ `dp.create_auto_cdc_from_snapshot_flow()` - Databricks extension
- ❌ `@dp.expect(...)` - Databricks DQX expectations
- ❌ `cloudFiles` / Auto Loader - Databricks managed ingestion service

### Why PySpark 4.1+?

| Requirement | Version | Reason |
|-------------|---------|--------|
| **pyspark.pipelines module** | 4.1+ | Core requirement for declarative pipelines |
| **Delta Lake compatibility** | 3.2+ | Supports Spark 4.x |
| **Databricks Runtime match** | N/A | DBR backports pipelines to Spark 3.5.x, but local needs 4.1+ |

### What CAN vs. CANNOT Be Tested Locally

| Component | File Location | Local PySpark 4.1+ | Databricks |
|-----------|---------------|---------------------|------------|
| **Transformation functions** | `src/common/scd_transformations.py` | ✅ **Yes** | ✅ Yes |
| **Pipeline wrapper files** | `src/type*_python/pipeline.py` | ✅ **Yes** (import works) | ✅ Yes |
| Standard DataFrame operations | Both | ✅ **Yes** | ✅ Yes |
| Window functions, aggregations | Both | ✅ **Yes** | ✅ Yes |
| Delta Lake read/write | Both | ✅ **Yes** | ✅ Yes |
| `from pyspark import pipelines as dp` | `pipeline.py` | ✅ **Yes** (Spark 4.1+) | ✅ Yes |
| `@dp.table`, `@dp.materialized_view` | `pipeline.py` | ✅ **Yes** (Spark 4.1+) | ✅ Yes |
| `@dp.temporary_view` | `pipeline.py` | ✅ **Yes** (Spark 4.1+) | ✅ Yes |
| `dp.create_auto_cdc_flow()` | `pipeline.py` | ❌ **No** (Databricks-only) | ✅ Yes |
| `cloudFiles` (Auto Loader) | `pipeline.py` | ❌ **No** (service) | ✅ Yes |
| `@dp.expect(...)` expectations | `pipeline.py` | ❌ **No** (Databricks DQX) | ✅ Yes |
| DQX validation jobs | `dqx/` directory | ❌ **No** | ✅ Yes |

### Practical Testing Strategy

With Spark 4.1+, you can now:

1. **Import and syntax-check pipeline files** - Verify decorators and structure
2. **Test transformation logic** - Run scd_transformations.py functions with real data
3. **Mock AUTO CDC behavior** - Write unit tests that simulate CDC flow logic
4. **Validate schema contracts** - Ensure Bronze → Silver → Gold schemas are correct

You **cannot** run end-to-end pipeline execution with AUTO CDC locally - that remains Databricks-only.

### ### DQX (Data Quality) Note
- `databricks-labs-dqx==0.15.0` is used only on Databricks
- DQX jobs run AFTER pipeline execution on the Databricks platform
- Local tests focus on transformation logic, not DQX validation rules
- DQX validation is tested during Databricks deployment

## Installation

### Option 1: Using uv (Recommended)

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Navigate to the project root
cd databricks-lakeflow-data-pipelines

# Install the project with local development dependencies
uv pip install -e ".[local]"
```

### Option 2: Using pip

```bash
# Navigate to the project root
cd databricks-lakeflow-data-pipelines

# Install the project with local development dependencies
pip install -e ".[local]"
```

## What Gets Installed

The `[local]` extra includes:
- **PySpark 3.5.x** - For local Spark development and testing
- **Delta Spark 3.x** - For Delta Lake operations locally
- **pytest-spark** - PySpark fixtures and helpers for pytest

These dependencies are **NOT** included when running on Databricks, where Spark is provided by the runtime.

## Running Tests Locally

### Run all tests
```bash
pytest
```

### Run only unit tests (fast, no Spark required)
```bash
pytest -m unit
```

### Run integration tests (requires Spark)
```bash
pytest -m integration
```

### Run specific test file
```bash
pytest src/common/test_scd_transformations.py -v
```

### Run with coverage
```bash
pytest --cov=src/common --cov-report=html
```

## Testing Specific Transformations

You can test individual transformation functions interactively:

```python
# Start Python REPL
python

# Import the transformations
from pyspark.sql import SparkSession
from src.common.scd_transformations import (
    transform_bronze_cdc,
    create_gold_view_scd1,
    create_gold_view_scd2,
    get_customer_cdc_schema
)

# Create a local Spark session
spark = SparkSession.builder \
    .appName("SCD Local Test") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Create test data
test_data = [
    ("C001", "Alice", "alice@example.com", "I", 1, "2024-01-01T10:00:00Z"),
    ("C001", "Alice Smith", "alice@example.com", "U", 2, "2024-01-02T11:00:00Z"),
]

df = spark.createDataFrame(test_data, get_customer_cdc_schema())

# Test bronze transformation
result = transform_bronze_cdc(df)
result.show()

# Clean up
spark.stop()
```

## Project Structure for Testing

```
src/common/
├── scd_transformations.py      # ✅ Pure PySpark functions (testable)
├── test_scd_transformations.py # ✅ Unit/integration tests
└── README_LOCAL_TESTING.md     # This file

src/type1_python/pipeline.py    # ⚙️ Lakeflow wrapper (uses scd_transformations)
src/type2_python/pipeline.py    # ⚙️ Lakeflow wrapper (uses scd_transformations)
```

## Limitations

When testing locally, you **cannot** test:
- ❌ Lakeflow AUTO CDC APIs (`dp.create_auto_cdc_flow()`)
- ❌ Auto Loader (`spark.readStream.format("cloudFiles")`)
- ❌ Lakeflow streaming decorators (`@dp.table`, `@dp.materialized_view`)

These are Databricks-proprietary features that only work on the Databricks platform.

## CI/CD Integration

The local dependencies enable CI/CD testing without Databricks:

```yaml
# Example GitHub Actions workflow
name: Test
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install uv
          uv pip install -e ".[local]"
      - name: Run tests
        run: pytest -v
```

## Troubleshooting

### Java version issues
PySpark requires Java 8, 11, or 17. Check your version:
```bash
java -version
```

### Memory issues
If tests fail with OOM errors, increase Spark driver memory:
```bash
export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"
pytest
```

### Delta Lake configuration
If Delta operations fail, ensure Spark is configured correctly:
```python
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```
