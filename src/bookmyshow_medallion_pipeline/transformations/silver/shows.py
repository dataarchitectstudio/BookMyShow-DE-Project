from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.silver.shows",
    comment="Deduplicated, conformed showtime source with movie/theatre referential integrity enforced.",
)
@dp.expect_or_drop("valid_seat_capacity", "total_seats > 0")
@dp.expect_or_drop("valid_price", "base_ticket_price > 0")
def shows():
    bronze = spark.read.table(f"{CATALOG}.bronze.shows")
    movies = spark.read.table(f"{CATALOG}.silver.movies").select("movie_id")
    theatres = spark.read.table(f"{CATALOG}.silver.theatres").select("theatre_id")
    latest_per_show = Window.partitionBy("show_id").orderBy(F.col("_ingested_at").desc())

    return (
        bronze.withColumn("_row_rank", F.row_number().over(latest_per_show))
        .filter("_row_rank = 1")
        .join(movies, "movie_id", "left_semi")  # drop shows referencing an unknown movie
        .join(theatres, "theatre_id", "left_semi")  # drop shows referencing an unknown theatre
        .select(
            F.col("show_id"),
            F.col("movie_id"),
            F.col("theatre_id"),
            F.col("screen_number").cast("int").alias("screen_number"),
            F.col("screen_type"),
            F.col("show_date").cast("date").alias("show_date"),
            F.col("show_time"),
            F.col("base_ticket_price").cast("double").alias("base_ticket_price"),
            F.col("total_seats").cast("int").alias("total_seats"),
        )
    )
