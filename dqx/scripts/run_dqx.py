import argparse
from pathlib import Path

import yaml
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--implementation", choices=["type1_sql", "type1_python", "type2_sql", "type2_python"], required=True)
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    check_file = repo_root / "dqx" / "checks" / args.implementation / f"{args.layer}.yml"
    checks = yaml.safe_load(check_file.read_text(encoding="utf-8"))

    status = DQEngine.validate_checks(checks)
    if status.has_errors:
        raise ValueError(f"Invalid DQX rules in {check_file}: {status.errors}")

    observer = DQMetricsObserver(name=f"{args.implementation}_{args.layer}")
    engine = DQEngine(WorkspaceClient(), observer=observer)

    input_df = spark.table(args.table)
    valid_df, quarantine_df, observation = engine.apply_checks_by_metadata_and_split(input_df, checks)

    output_table = f"{args.table}__dq_output"
    quarantine_table = f"{args.table}__dq_quarantine"
    metrics_table = f"{args.table}__dq_metrics"

    engine.save_results_in_table(
        output_df=valid_df,
        quarantine_df=quarantine_df,
        observation=observation,
        output_config=OutputConfig(location=output_table, mode="overwrite"),
        quarantine_config=OutputConfig(location=quarantine_table, mode="overwrite"),
        metrics_config=OutputConfig(location=metrics_table, mode="append"),
    )

    print(f"DQX validation completed: {args.table}")
    print(f"  valid      = {output_table}")
    print(f"  quarantine = {quarantine_table}")
    print(f"  metrics    = {metrics_table}")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    main()
