# Declarative DQX validation

This directory contains declarative YAML quality rules plus one reusable Spark Python runner.

## Design

For each implementation (`type1_sql`, `type1_python`, `type2_sql`, `type2_python`) there are three rule files:

```text
bronze.yml
silver.yml
gold.yml
```

The runner loads the YAML with `DQEngine.validate_checks`, applies the rules with `apply_checks_by_metadata_and_split`, and persists:

- valid records: `<table>__dq_output`
- invalid/quarantined records: `<table>__dq_quarantine`
- summary metrics: `<table>__dq_metrics`

DQX is deliberately external to Lakeflow expectations. This keeps the validation framework reusable across both the SQL and PySpark transformation implementations and makes the checks version-controlled as metadata.

The demo pins `databricks-labs-dqx==0.15.0`, which is the current tagged release used by this repository.
