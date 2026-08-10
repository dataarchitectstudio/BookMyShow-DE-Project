from pyspark import pipelines as dp

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_movies",
    comment="Movie dimension curated for BI consumption.",
)
def dim_movies():
    return spark.read.table(f"{CATALOG}.silver.movies")
