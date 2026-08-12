from pyspark import pipelines as dp

GOLD_SCHEMA = spark.conf.get("gold_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_dim_show",
    comment="Show dimension: showtime/screen_type/base_price with theatre_id/movie_id FKs.",
    cluster_by=["show_id"],
)
def gold_dim_show():
    return spark.read.table(f"{SILVER_SCHEMA}.silver_shows").select(
        "show_id", "movie_id", "theatre_id", "showtime", "screen_type", "base_price"
    )
