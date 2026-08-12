from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME_PATH = spark.conf.get("raw_volume_path")


@dp.table(
    name="bronze_movies",
    comment="Raw movie records ingested via Auto Loader from the synthetic data raw_landing volume.",
)
def bronze_movies():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .load(f"{RAW_VOLUME_PATH}/movies")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
