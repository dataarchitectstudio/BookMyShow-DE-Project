from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_city_revenue",
    comment="Daily revenue and customer reach per city.",
)
def agg_city_revenue():
    fact = spark.read.table(f"{CATALOG}.gold.fact_bookings").filter("payment_status = 'SUCCESS'")

    return fact.groupBy("city", "state", "booking_date").agg(
        F.count("booking_id").alias("total_bookings"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.sum("net_amount"), 2).alias("total_revenue"),
    )
