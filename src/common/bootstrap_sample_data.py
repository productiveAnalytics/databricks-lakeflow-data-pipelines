import argparse
from pathlib import Path

from databricks.sdk.runtime import dbutils


def main() -> None:
    parser = argparse.ArgumentParser()
    # The bundle passes the volume path it actually provisioned, so this script
    # never has to reconstruct /Volumes/<catalog>/<schema>/<volume> itself.
    parser.add_argument("--destination", required=True)
    args = parser.parse_args()

    # Use workspace-synced path instead of __file__ (not available in job context)
    workspace_root = Path("/Workspace/Users/lalitstar@gmail.com/databricks-lakeflow-data-pipelines")
    source_file = workspace_root / "src" / "data" / "customer_cdc.jsonl"
    destination = args.destination.rstrip("/")

    dbutils.fs.rm(destination, True)
    dbutils.fs.mkdirs(destination)
    dbutils.fs.put(f"{destination}/customer_cdc.jsonl", source_file.read_text(encoding="utf-8"), True)
    print(f"Loaded sample CDC feed into {destination}")


if __name__ == "__main__":
    main()
