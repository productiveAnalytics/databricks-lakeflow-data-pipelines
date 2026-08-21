import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_sample_cdc_has_insert_update_delete_and_out_of_order_sequence():
    rows = [json.loads(line) for line in (ROOT / "src/data/customer_cdc.jsonl").read_text().splitlines()]
    operations = {row["operation"] for row in rows}
    assert operations == {"INSERT", "UPDATE", "DELETE"}

    c001_updates = [r["sequence_num"] for r in rows if r["customer_id"] == "C001" and r["operation"] == "UPDATE"]
    assert c001_updates == [1005, 1003]
    assert c001_updates != sorted(c001_updates)


def test_all_implementations_have_three_dqx_layers():
    for implementation in ["type1_sql", "type1_python", "type2_sql", "type2_python"]:
        for layer in ["bronze", "silver", "gold"]:
            check_file = ROOT / "dqx/checks" / implementation / f"{layer}.yml"
            assert check_file.exists()
            checks = yaml.safe_load(check_file.read_text())
            assert checks
            assert all(rule["criticality"] in {"error", "warn"} for rule in checks)


def test_bundle_declares_all_implementations_and_uc_schemas():
    import yaml

    root = ROOT
    bundle = yaml.safe_load((root / "databricks.yml").read_text())
    schemas = yaml.safe_load((root / "resources/schemas.yml").read_text())
    pipelines = yaml.safe_load((root / "resources/pipelines.yml").read_text())

    assert bundle["bundle"]["name"] == "databricks-lakeflow-scd-demo"
    schema_names = {v["name"] for v in schemas["resources"]["schemas"].values()}
    expected = {
        f"{layer}__type_{scd}__{lang}"
        for scd in (1, 2)
        for lang in ("sql", "pyspark")
        for layer in ("bronze", "silver", "gold")
    }
    assert expected.issubset(schema_names)

    pipeline_names = set(pipelines["resources"]["pipelines"])
    assert pipeline_names == {
        "scd1_sql_pipeline", "scd1_python_pipeline",
        "scd2_sql_pipeline", "scd2_python_pipeline",
    }


def test_each_layer_has_dqx_metadata():
    implementations = ("type1_sql", "type1_python", "type2_sql", "type2_python")
    layers = ("bronze", "silver", "gold")
    for implementation in implementations:
        for layer in layers:
            path = ROOT / "dqx" / "checks" / implementation / f"{layer}.yml"
            assert path.exists()
            text = path.read_text()
            assert "- name:" in text
            assert "check:" in text


# ---------------------------------------------------------------------------
# Schema-name wiring
#
# The pipeline sources address their Bronze/Silver schemas by literal name, so
# anything that rewrites deployed schema names (notably `mode: development`,
# which prefixes UC schemas with `dev_<user>_`) silently splits the pipelines:
# Bronze/Silver land in schemas the bundle never created, and every DQX job
# then fails on a missing table. These tests pin that wiring shut.
# ---------------------------------------------------------------------------

SCHEMA_NAME_RE = re.compile(r"\b((?:bronze|silver|gold)__type_[12]__(?:sql|pyspark))\b")


def _declared_schema_names():
    schemas = yaml.safe_load((ROOT / "resources/schemas.yml").read_text())
    return {v["name"] for v in schemas["resources"]["schemas"].values()}


def _schema_resource_keys():
    schemas = yaml.safe_load((ROOT / "resources/schemas.yml").read_text())
    return set(schemas["resources"]["schemas"])


def test_every_schema_used_in_pipeline_sources_is_declared_in_the_bundle():
    declared = _declared_schema_names()
    seen = set()
    for source in sorted(ROOT.glob("src/type*/pipeline.*")):
        used = set(SCHEMA_NAME_RE.findall(source.read_text()))
        assert used, f"{source} references no medallion schema"
        undeclared = used - declared
        assert not undeclared, f"{source} uses schemas not declared in the bundle: {sorted(undeclared)}"
        seen |= used
    assert seen == declared - {"demo__input"}, "declared schemas and schemas used by src/ have drifted apart"


def test_dqx_jobs_reference_schemas_through_bundle_resources():
    jobs = yaml.safe_load((ROOT / "resources/jobs.yml").read_text())["resources"]["jobs"]
    keys = _schema_resource_keys()
    tables = []
    for job_name, job in jobs.items():
        if not job_name.endswith("_dqx"):
            continue
        for task in job["tasks"]:
            params = task["spark_python_task"]["parameters"]
            table = params[params.index("--table") + 1]
            tables.append(table)
            # Literal schema names here would go stale the moment the deployed
            # schema name changes; go through the schema resource instead.
            match = re.fullmatch(
                r"\$\{var\.catalog\}\.\$\{resources\.schemas\.(\w+)\.name\}\.\w+", table
            )
            assert match, f"{job_name}/{task['task_key']} hardcodes its schema: {table}"
            assert match.group(1) in keys, f"unknown schema resource in {table}"
    assert len(tables) == 12, "expected one DQX task per layer for all four implementations"


def test_dev_target_does_not_rewrite_schema_names():
    dev = yaml.safe_load((ROOT / "databricks.yml").read_text())["targets"]["dev"]
    assert dev.get("mode") != "development", (
        "mode: development prefixes UC schema names with dev_<user>_, which breaks the "
        "literal schema names in src/ and the DQX job table arguments"
    )
    assert dev["presets"]["pipelines_development"] is True, (
        "dev pipelines should still run in development mode for fast startup"
    )


def test_source_path_and_bootstrap_destination_come_from_the_volume_resource():
    volume_ref = "${resources.volumes.demo_input_volume.volume_path}/customer_cdc"

    pipelines = yaml.safe_load((ROOT / "resources/pipelines.yml").read_text())["resources"]["pipelines"]
    for name, pipeline in pipelines.items():
        settings = pipeline.get("parameters") or pipeline.get("configuration")
        assert settings["source_path"] == volume_ref, f"{name} does not read from the bundle's volume"

    jobs = yaml.safe_load((ROOT / "resources/jobs.yml").read_text())["resources"]["jobs"]
    params = jobs["bootstrap_sample_data"]["tasks"][0]["spark_python_task"]["parameters"]
    assert params == ["--destination", volume_ref], (
        "bootstrap must be handed the provisioned volume path rather than rebuilding it"
    )


def test_auto_cdc_clause_order_and_scd_type_per_implementation():
    # Lakeflow's SQL parser requires APPLY AS DELETE before SEQUENCE BY.
    for scd_type in (1, 2):
        sql = (ROOT / f"src/type{scd_type}_sql/pipeline.sql").read_text()
        assert sql.index("APPLY AS DELETE WHEN") < sql.index("SEQUENCE BY")
        assert f"STORED AS SCD TYPE {scd_type}" in sql

        py = (ROOT / f"src/type{scd_type}_python/pipeline.py").read_text()
        assert f'stored_as_scd_type="{scd_type}"' in py
        assert "apply_as_deletes=" in py

    # SCD2 Gold must publish current rows only.
    assert "__END_AT IS NULL" in (ROOT / "src/type2_sql/pipeline.sql").read_text()
    assert '__END_AT"' in (ROOT / "src/type2_python/pipeline.py").read_text()


def test_pipeline_sources_use_current_lakeflow_apis_not_legacy_dlt():
    banned = ["import dlt", "@dlt.", "dlt.apply_changes", "LIVE.", "CREATE LIVE TABLE", "dp.read("]
    for source in sorted(ROOT.glob("src/type*/pipeline.*")):
        text = source.read_text()
        for token in banned:
            assert token not in text, f"{source} uses legacy DLT syntax: {token}"
