from pyspark import pipelines as dp

GOLD_SCHEMA = spark.conf.get("gold_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_dim_movie",
    comment="Movie dimension (SCD Type 1).",
    cluster_by=["movie_id"],
)
def gold_dim_movie():
    return spark.read.table(f"{SILVER_SCHEMA}.silver_movies").select(
        "movie_id", "title", "genre", "duration_minutes", "release_date"
    )
