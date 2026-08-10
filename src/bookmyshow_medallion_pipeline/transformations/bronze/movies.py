from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")
RAW_PATH = f"/Volumes/{CATALOG}/raw/landing_zone/movies"


@dp.table(
    name=f"{CATALOG}.bronze.movies",
    comment="Raw movie catalog ingested from the landing volume via Auto Loader.",
)
@dp.expect_or_drop("valid_movie_id", "movie_id IS NOT NULL")
def movies():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
