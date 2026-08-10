from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")
RAW_PATH = f"/Volumes/{CATALOG}/raw/landing_zone/bookings"


@dp.table(
    name=f"{CATALOG}.bronze.bookings",
    comment="Raw booking transactions ingested from the landing volume via Auto Loader.",
)
@dp.expect_or_drop("valid_booking_id", "booking_id IS NOT NULL")
@dp.expect_or_drop("valid_show_reference", "show_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_reference", "customer_id IS NOT NULL")
def bookings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
