from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")

# Mirrors bookmyshow_analytics.business_rules.LOYALTY_TIER_THRESHOLDS / DEFAULT_LOYALTY_TIER.
# Duplicated here (rather than imported) because the pipeline's managed Python
# environment doesn't resolve the project's own editable package; keep in sync
# with tests/unit/test_business_rules.py, which is the source of truth.
LOYALTY_TIER_THRESHOLDS = (
    ("Platinum", 50_000.0),
    ("Gold", 20_000.0),
    ("Silver", 5_000.0),
)
DEFAULT_LOYALTY_TIER = "Bronze"


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_customers",
    comment="Customer dimension enriched with lifetime booking activity and a computed loyalty tier.",
)
def dim_customers():
    customers = spark.read.table(f"{CATALOG}.silver.customers")
    bookings = spark.read.table(f"{CATALOG}.silver.bookings")

    activity = (
        bookings.filter("payment_status = 'SUCCESS'")
        .groupBy("customer_id")
        .agg(
            F.round(F.sum("net_amount"), 2).alias("lifetime_net_spend"),
            F.count("booking_id").alias("total_bookings"),
            F.min("booking_date").alias("first_booking_date"),
            F.max("booking_date").alias("last_booking_date"),
        )
    )

    # Same thresholds as business_rules.classify_loyalty_tier, expressed as a Spark
    # when-chain (checked highest threshold first) instead of a row-wise UDF.
    tier_expr = F.lit(DEFAULT_LOYALTY_TIER)
    for tier, threshold in reversed(LOYALTY_TIER_THRESHOLDS):
        tier_expr = F.when(F.coalesce(F.col("lifetime_net_spend"), F.lit(0.0)) >= threshold, F.lit(tier)).otherwise(
            tier_expr
        )

    return (
        customers.join(activity, "customer_id", "left")
        .withColumn("lifetime_net_spend", F.coalesce(F.col("lifetime_net_spend"), F.lit(0.0)))
        .withColumn("total_bookings", F.coalesce(F.col("total_bookings"), F.lit(0)))
        .withColumn("loyalty_tier", tier_expr)
        .select(
            "customer_id", "full_name", "email", "phone_number", "city", "state",
            "signup_date", "preferred_genre", "preferred_channel", "marketing_opt_in",
            "lifetime_net_spend", "total_bookings", "first_booking_date", "last_booking_date", "loyalty_tier",
        )
    )
