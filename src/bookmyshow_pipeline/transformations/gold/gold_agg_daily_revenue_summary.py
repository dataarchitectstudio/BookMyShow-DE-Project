from pyspark import pipelines as dp
from pyspark.sql import functions as F

GOLD_SCHEMA = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_agg_daily_revenue_summary",
    comment=(
        "Daily revenue summary -- grain: date x city x payment_gateway. Direct rollup "
        "of gold_fact_bookings (the 'Daily Batch Job -> revenue summary' table from the "
        "architecture diagram), so it can never disagree with figures computed elsewhere. "
        "This is also the grain the Trends & Reach dashboard's payment-failure-rate "
        "heatmap needs to surface a gateway/city/date spike."
    ),
    cluster_by=["date_key"],
)
def gold_agg_daily_revenue_summary():
    bookings = spark.read.table(f"{GOLD_SCHEMA}.gold_fact_bookings")
    theatres = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_theatre").select("theatre_id", "city")
    realized = (F.col("payment_status") == "success") & (~F.col("is_cancelled"))
    return (
        bookings.join(theatres, "theatre_id")
        .groupBy("date_key", "booking_date", "city", "payment_gateway")
        .agg(
            F.sum(F.when(realized, F.col("net_revenue")).otherwise(F.lit(0))).alias("total_revenue"),
            F.count("booking_id").alias("total_bookings"),
            F.sum(F.when(F.col("payment_status") == "success", F.lit(1)).otherwise(F.lit(0))).alias(
                "successful_bookings"
            ),
            F.sum(F.when(F.col("payment_status") == "failed", F.lit(1)).otherwise(F.lit(0))).alias(
                "failed_bookings"
            ),
            F.sum(F.when(F.col("payment_status") == "refunded", F.lit(1)).otherwise(F.lit(0))).alias(
                "refunded_bookings"
            ),
            F.sum(F.when(F.col("is_cancelled"), F.lit(1)).otherwise(F.lit(0))).alias("cancelled_bookings"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("payment_failure_rate", (F.col("failed_bookings") / F.col("total_bookings")).cast("double"))
        .withColumn("cancellation_rate", (F.col("cancelled_bookings") / F.col("total_bookings")).cast("double"))
    )
