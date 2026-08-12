from pyspark import pipelines as dp
from pyspark.sql import functions as F

GOLD_SCHEMA = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_fact_show_occupancy",
    comment=(
        "Show occupancy fact -- grain: one row per show. Kept separate from "
        "gold_fact_bookings to avoid double-counting seat capacity."
    ),
    cluster_by=["theatre_id"],
)
def gold_fact_show_occupancy():
    shows = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_show")
    theatres = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_theatre").select(
        "theatre_id", "city", "seat_capacity"
    )
    # Only realized (successful, non-cancelled) bookings occupy a seat.
    seats_sold = (
        spark.read.table(f"{GOLD_SCHEMA}.gold_fact_bookings")
        .filter((F.col("payment_status") == "success") & (~F.col("is_cancelled")))
        .groupBy("show_id")
        .agg(F.sum("seats_booked").alias("seats_sold"))
    )
    return (
        shows.join(theatres, "theatre_id")
        .join(seats_sold, "show_id", "left")
        .withColumn("seats_sold", F.coalesce(F.col("seats_sold"), F.lit(0)))
        .withColumn("show_date", F.to_date("showtime"))
        .withColumn("occupancy_rate", (F.col("seats_sold") / F.col("seat_capacity")).cast("double"))
        .select(
            "show_id",
            "theatre_id",
            "movie_id",
            "city",
            "screen_type",
            "show_date",
            "showtime",
            "seat_capacity",
            "seats_sold",
            "occupancy_rate",
        )
    )
