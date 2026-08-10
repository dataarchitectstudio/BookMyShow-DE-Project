from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.silver.movies",
    comment="Deduplicated, conformed movie dimension source.",
)
@dp.expect_or_drop("valid_duration", "duration_minutes > 0")
@dp.expect_or_drop("valid_genre", "genre IS NOT NULL")
def movies():
    bronze = spark.read.table(f"{CATALOG}.bronze.movies")
    latest_per_movie = Window.partitionBy("movie_id").orderBy(F.col("_ingested_at").desc())

    return (
        bronze.withColumn("_row_rank", F.row_number().over(latest_per_movie))
        .filter("_row_rank = 1")
        .select(
            F.col("movie_id"),
            F.trim(F.col("title")).alias("title"),
            F.col("genre"),
            F.col("language"),
            F.col("censor_rating"),
            F.col("release_date").cast("date").alias("release_date"),
            F.col("duration_minutes").cast("int").alias("duration_minutes"),
            F.col("average_rating").cast("double").alias("average_rating"),
            F.col("is_blockbuster").cast("boolean").alias("is_blockbuster"),
        )
    )
