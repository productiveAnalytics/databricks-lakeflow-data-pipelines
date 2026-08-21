from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("pipeline_catalog")
SOURCE_PATH = spark.conf.get("source_path")

BRONZE = f"{CATALOG}.bronze__type_2__pyspark.customer_cdc"
SILVER = f"{CATALOG}.silver__type_2__pyspark.customer_dim_scd2"
GOLD = f"{CATALOG}.gold__type_2__pyspark.customer_current"


@dp.table(
    name=BRONZE,
    comment="Raw customer CDC events from the simulated source feed.",
)
def customer_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_PATH)
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
            F.col("email").cast("string").alias("email"),
            F.col("city").cast("string").alias("city"),
            F.col("state").cast("string").alias("state"),
            F.col("operation").cast("string").alias("operation"),
            F.col("sequence_num").cast("long").alias("sequence_num"),
        )
    )


dp.create_streaming_table(
    name=SILVER,
    comment="Historical customer dimension maintained as SCD Type 2.",
)

dp.create_auto_cdc_flow(
    target=SILVER,
    source=BRONZE,
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type="2",
)


@dp.materialized_view(
    name=GOLD,
    comment="Current customer state exposed as a Gold consumer dataset.",
)
def customer_current():
    df = spark.read.table(SILVER)
    df = df.filter(F.col("__END_AT").isNull())
    return df.select(
        "customer_id", "first_name", "last_name", "email", "city", "state"
    )
