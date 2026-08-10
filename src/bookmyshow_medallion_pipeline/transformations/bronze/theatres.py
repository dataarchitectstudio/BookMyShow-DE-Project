from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")
RAW_PATH = f"/Volumes/{CATALOG}/raw/landing_zone/theatres"


@dp.table(
    name=f"{CATALOG}.bronze.theatres",
    comment="Raw theatre records ingested from the landing volume via Auto Loader.",
)
@dp.expect_or_drop("valid_theatre_id", "theatre_id IS NOT NULL")
def theatres():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
