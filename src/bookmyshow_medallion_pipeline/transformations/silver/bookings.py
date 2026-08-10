from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("bookmyshow.catalog")
VALID_PAYMENT_STATUSES = ("SUCCESS", "FAILED", "REFUNDED")


@dp.materialized_view(
    name=f"{CATALOG}.silver.bookings",
    comment="Deduplicated, conformed booking transactions with show/customer referential integrity enforced.",
)
@dp.expect_or_drop("valid_seats_booked", "seats_booked BETWEEN 1 AND 10")
@dp.expect_or_drop("non_negative_net_amount", "net_amount >= 0")
@dp.expect_or_drop("valid_payment_status", f"payment_status IN {VALID_PAYMENT_STATUSES}")
def bookings():
    bronze = spark.read.table(f"{CATALOG}.bronze.bookings")
    shows = spark.read.table(f"{CATALOG}.silver.shows").select("show_id")
    customers = spark.read.table(f"{CATALOG}.silver.customers").select("customer_id")
    latest_per_booking = Window.partitionBy("booking_id").orderBy(F.col("_ingested_at").desc())

    return (
        bronze.withColumn("_row_rank", F.row_number().over(latest_per_booking))
        .filter("_row_rank = 1")
        .join(shows, "show_id", "left_semi")  # drop bookings referencing an unknown show
        .join(customers, "customer_id", "left_semi")  # drop bookings referencing an unknown customer
        .select(
            F.col("booking_id"),
            F.col("show_id"),
            F.col("customer_id"),
            F.col("booking_timestamp").cast("timestamp").alias("booking_timestamp"),
            F.col("booking_date").cast("date").alias("booking_date"),
            F.col("seats_booked").cast("int").alias("seats_booked"),
            F.col("ticket_amount").cast("double").alias("ticket_amount"),
            F.col("convenience_fee").cast("double").alias("convenience_fee"),
            F.col("discount_amount").cast("double").alias("discount_amount"),
            F.col("net_amount").cast("double").alias("net_amount"),
            F.col("payment_gateway"),
            F.col("payment_status"),
            F.col("booking_channel"),
            F.col("is_cancelled").cast("boolean").alias("is_cancelled"),
        )
    )
