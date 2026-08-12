from pyspark import pipelines as dp

SILVER_SCHEMA = spark.conf.get("silver_schema")


@dp.table(
    name=f"{SILVER_SCHEMA}.silver_movies",
    comment="Cleaned movie dimension: structurally invalid rows dropped.",
)
@dp.expect_or_drop("valid_movie_id", "movie_id IS NOT NULL")
@dp.expect_or_drop("valid_title", "title IS NOT NULL")
@dp.expect_or_drop("valid_duration", "duration_minutes > 0")
@dp.expect_or_drop("valid_genre", "genre IS NOT NULL")
def silver_movies():
    return spark.readStream.table("bronze_movies").select(
        "movie_id", "title", "genre", "duration_minutes", "release_date"
    )
