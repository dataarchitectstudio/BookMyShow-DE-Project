from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_SCHEMA = spark.conf.get("gold_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")

# Quartiles over realized spend, richest first -- ntile 1 -> Platinum ... ntile 4 -> Bronze.
LOYALTY_SEGMENTS = ["Platinum", "Gold", "Silver", "Bronze"]


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_dim_customer",
    comment=(
        "Customer dimension with lifetime_spend and loyalty_segment (Platinum/Gold/"
        "Silver/Bronze, quartile-bucketed on realized spend) -- the single place "
        "customer segmentation logic lives."
    ),
    cluster_by=["customer_id"],
)
def gold_dim_customer():
    customers = spark.read.table(f"{SILVER_SCHEMA}.silver_customers").select(
        "customer_id", "name", "email", "phone", "city", "signup_date"
    )
    # Only realized (successful, non-cancelled) bookings count toward spend/segment.
    spend = (
        spark.read.table(f"{GOLD_SCHEMA}.gold_fact_bookings")
        .filter((F.col("payment_status") == "success") & (~F.col("is_cancelled")))
        .groupBy("customer_id")
        .agg(
            F.sum("net_revenue").alias("lifetime_spend"),
            F.count("booking_id").alias("lifetime_bookings"),
        )
    )
    enriched = (
        customers.join(spend, "customer_id", "left")
        .withColumn("lifetime_spend", F.coalesce(F.col("lifetime_spend"), F.lit(0)))
        .withColumn("lifetime_bookings", F.coalesce(F.col("lifetime_bookings"), F.lit(0)))
    )
    segment_ntile = F.ntile(4).over(Window.orderBy(F.desc("lifetime_spend")))
    return enriched.withColumn(
        "loyalty_segment",
        F.element_at(F.array(*[F.lit(s) for s in LOYALTY_SEGMENTS]), segment_ntile),
    )
