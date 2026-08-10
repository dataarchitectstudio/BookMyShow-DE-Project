from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_payment_gateway_health",
    comment=(
        "Transaction volume and failure rate per payment gateway, day, and city -- "
        "the table that pinpoints a gateway outage incident to a specific date and region."
    ),
)
def agg_payment_gateway_health():
    fact = spark.read.table(f"{CATALOG}.gold.fact_bookings")

    return fact.groupBy("payment_gateway", "booking_date", "city").agg(
        F.count("booking_id").alias("total_transactions"),
        F.sum(F.when(F.col("payment_status") == "FAILED", 1).otherwise(0)).alias("failed_transactions"),
        F.round(
            F.sum(F.when(F.col("payment_status") == "FAILED", 1).otherwise(0)) / F.count("booking_id"), 4
        ).alias("failure_rate"),
    )
