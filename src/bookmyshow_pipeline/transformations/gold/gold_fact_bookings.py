from pyspark import pipelines as dp
from pyspark.sql import functions as F

GOLD_SCHEMA = spark.conf.get("gold_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_fact_bookings",
    comment=(
        "Booking fact -- grain: one row per booking. Primary fact behind the revenue/"
        "booking-count/payment-failure/channel/cancellation KPIs."
    ),
    cluster_by=["date_key", "theatre_id"],
)
def gold_fact_bookings():
    bookings = spark.read.table(f"{SILVER_SCHEMA}.silver_bookings")
    shows = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_show").select("show_id", "theatre_id", "movie_id")
    return (
        bookings.join(shows, "show_id")
        .withColumn("net_revenue", F.col("ticket_amount") + F.col("fees") - F.col("discount"))
        .withColumn("date_key", F.date_format("booking_date", "yyyyMMdd").cast("int"))
        .select(
            "booking_id",
            "show_id",
            "theatre_id",
            "movie_id",
            "customer_id",
            "date_key",
            "booking_date",
            "seats_booked",
            "ticket_amount",
            "fees",
            "discount",
            "net_revenue",
            "payment_status",
            "payment_gateway",
            "booking_channel",
            "is_cancelled",
        )
    )
