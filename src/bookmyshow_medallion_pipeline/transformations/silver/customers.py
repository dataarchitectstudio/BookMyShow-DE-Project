from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.silver.customers",
    comment="Deduplicated, conformed customer dimension source.",
)
@dp.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
def customers():
    bronze = spark.read.table(f"{CATALOG}.bronze.customers")
    latest_per_customer = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())

    return (
        bronze.withColumn("_row_rank", F.row_number().over(latest_per_customer))
        .filter("_row_rank = 1")
        .select(
            F.col("customer_id"),
            F.trim(F.col("full_name")).alias("full_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.col("phone_number"),
            F.initcap(F.trim(F.col("city"))).alias("city"),
            F.initcap(F.trim(F.col("state"))).alias("state"),
            F.col("signup_date").cast("date").alias("signup_date"),
            F.col("preferred_genre"),
            F.col("preferred_channel"),
            F.col("marketing_opt_in").cast("boolean").alias("marketing_opt_in"),
        )
    )
