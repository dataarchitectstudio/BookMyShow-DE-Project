from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")
RAW_PATH = f"/Volumes/{CATALOG}/raw/landing_zone/customers"


@dp.table(
    name=f"{CATALOG}.bronze.customers",
    comment="Raw customer records ingested from the landing volume via Auto Loader.",
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
