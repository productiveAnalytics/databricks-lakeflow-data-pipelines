# Deployment guide

## 1. Configure the Databricks CLI

Authenticate the Databricks CLI to the target workspace using your preferred supported authentication method.

## 2. Update the workspace target

Edit `databricks.yml`:

```yaml
targets:
  dev:
    workspace:
      host: https://<your-databricks-workspace>
```

Set the `catalog` variable to a catalog where you have create privileges.

The `dev` target intentionally does not set `mode: development`. That mode prefixes
Unity Catalog schema names with `dev_<short_name>_`, which would move the schemas and
the volume out from under the literal schema names used in `src/` and the DQX job
arguments. Isolate developers by catalog instead:

```bash
databricks bundle deploy -t dev --var catalog=my_sandbox
```

## 3. Validate the bundle

```bash
databricks bundle validate -t dev
```

## 4. Deploy

```bash
databricks bundle deploy -t dev
```

## 5. Seed the sample source

```bash
databricks bundle run -t dev bootstrap_sample_data
```

## 6. Run one pipeline

Example:

```bash
databricks bundle run -t dev scd2_python_pipeline
```

## 7. Run DQX

```bash
databricks bundle run -t dev scd2_python_dqx
```

Repeat for the other three implementations.

## Reset / replay

Lakeflow checkpoints are intentionally persistent. To replay the demo source, rerun the bootstrap job and execute a full pipeline reset from the Databricks pipeline UI/CLI. Do not remove checkpoints in production workloads merely to recover from a normal transient failure.

## Local validation performed for this repository

The repository includes static tests for the sample CDC data, bundle resource declarations, Unity Catalog schema naming, and presence of all DQX layer rules. Run:

```bash
python -m pytest -q
```

The Databricks CLI must be available and authenticated before `databricks bundle validate` can validate workspace-specific resource semantics.
