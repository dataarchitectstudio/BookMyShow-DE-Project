from pyspark import pipelines as dp

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_theatres",
    comment="Theatre dimension curated for BI consumption.",
)
def dim_theatres():
    return spark.read.table(f"{CATALOG}.silver.theatres")
