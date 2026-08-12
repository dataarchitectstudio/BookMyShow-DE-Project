from pyspark import pipelines as dp

GOLD_SCHEMA = spark.conf.get("gold_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_dim_theatre",
    comment="Theatre dimension (SCD Type 1).",
    cluster_by=["theatre_id"],
)
def gold_dim_theatre():
    return spark.read.table(f"{SILVER_SCHEMA}.silver_theatres").select(
        "theatre_id", "name", "city", "state", "screen_type", "seat_capacity"
    )
