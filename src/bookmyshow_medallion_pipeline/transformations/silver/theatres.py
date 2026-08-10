from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.silver.theatres",
    comment="Deduplicated, conformed theatre dimension source.",
)
@dp.expect_or_drop("valid_total_screens", "total_screens > 0")
@dp.expect_or_drop("valid_city", "city IS NOT NULL")
def theatres():
    bronze = spark.read.table(f"{CATALOG}.bronze.theatres")
    latest_per_theatre = Window.partitionBy("theatre_id").orderBy(F.col("_ingested_at").desc())

    return (
        bronze.withColumn("_row_rank", F.row_number().over(latest_per_theatre))
        .filter("_row_rank = 1")
        .select(
            F.col("theatre_id"),
            F.trim(F.col("theatre_name")).alias("theatre_name"),
            F.initcap(F.trim(F.col("city"))).alias("city"),
            F.initcap(F.trim(F.col("state"))).alias("state"),
            F.col("total_screens").cast("int").alias("total_screens"),
            F.col("opened_date").cast("date").alias("opened_date"),
        )
    )
