from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_theatre_occupancy",
    comment="Seat occupancy per theatre and screen type, from scheduled capacity vs. successful bookings.",
)
def agg_theatre_occupancy():
    shows = spark.read.table(f"{CATALOG}.silver.shows")
    bookings = spark.read.table(f"{CATALOG}.silver.bookings").filter("payment_status = 'SUCCESS'")
    theatres = spark.read.table(f"{CATALOG}.silver.theatres").select("theatre_id", "theatre_name", "city")

    seats_booked_per_show = bookings.groupBy("show_id").agg(F.sum("seats_booked").alias("seats_booked"))

    show_occupancy = shows.join(seats_booked_per_show, "show_id", "left").withColumn(
        "seats_booked", F.coalesce(F.col("seats_booked"), F.lit(0))
    )

    return (
        show_occupancy.join(theatres, "theatre_id")
        .groupBy("theatre_id", "theatre_name", "city", "screen_type")
        .agg(
            F.count("show_id").alias("total_shows"),
            F.sum("total_seats").alias("total_seats_available"),
            F.sum("seats_booked").alias("total_seats_booked"),
        )
        .withColumn(
            "occupancy_rate",
            F.round(F.least(F.col("total_seats_booked") / F.col("total_seats_available"), F.lit(1.0)), 4),
        )
    )
