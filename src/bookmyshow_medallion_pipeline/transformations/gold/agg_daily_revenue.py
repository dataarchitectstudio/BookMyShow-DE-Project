from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_daily_revenue",
    comment="Daily booking volume, revenue, and payment-failure rate -- the headline KPI feed for the dashboard.",
)
def agg_daily_revenue():
    fact = spark.read.table(f"{CATALOG}.gold.fact_bookings")

    return fact.groupBy("booking_date").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum(F.when(F.col("payment_status") == "SUCCESS", 1).otherwise(0)).alias("successful_bookings"),
        F.sum(F.when(F.col("payment_status") == "FAILED", 1).otherwise(0)).alias("failed_bookings"),
        F.sum(F.when(F.col("payment_status") == "REFUNDED", 1).otherwise(0)).alias("refunded_bookings"),
        F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)).alias("cancelled_bookings"),
        F.round(
            F.sum(F.when(F.col("payment_status") == "SUCCESS", F.col("net_amount")).otherwise(0.0)), 2
        ).alias("total_revenue"),
        F.round(
            F.sum(F.when(F.col("payment_status") == "FAILED", 1).otherwise(0)) / F.count("booking_id"), 4
        ).alias("payment_failure_rate"),
    )
