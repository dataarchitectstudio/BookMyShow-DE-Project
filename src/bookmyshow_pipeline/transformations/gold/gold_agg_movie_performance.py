from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_SCHEMA = spark.conf.get("gold_schema")

# Revenue-percentile cutoffs for the hit/steady/underperforming classification: top 20%
# of movies by realized revenue are "hit", next 40% "steady", bottom 40% "underperforming".
HIT_PERCENTILE = 0.8
STEADY_PERCENTILE = 0.4


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_agg_movie_performance",
    comment=(
        "Movie revenue rollup -- grain: one row per movie, classified into "
        "hit/steady/underperforming via revenue percentile thresholds, computed once."
    ),
    cluster_by=["movie_id"],
)
def gold_agg_movie_performance():
    movies = spark.read.table(f"{GOLD_SCHEMA}.gold_dim_movie")
    # Only realized (successful, non-cancelled) bookings count as performance.
    revenue = (
        spark.read.table(f"{GOLD_SCHEMA}.gold_fact_bookings")
        .filter((F.col("payment_status") == "success") & (~F.col("is_cancelled")))
        .groupBy("movie_id")
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.count("booking_id").alias("total_bookings"),
            F.sum("seats_booked").alias("total_seats_sold"),
        )
    )
    enriched = (
        movies.join(revenue, "movie_id", "left")
        .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0)))
        .withColumn("total_bookings", F.coalesce(F.col("total_bookings"), F.lit(0)))
        .withColumn("total_seats_sold", F.coalesce(F.col("total_seats_sold"), F.lit(0)))
    )
    revenue_percentile = F.percent_rank().over(Window.orderBy("total_revenue"))
    return enriched.withColumn(
        "performance_tier",
        F.when(revenue_percentile >= HIT_PERCENTILE, F.lit("hit"))
        .when(revenue_percentile >= STEADY_PERCENTILE, F.lit("steady"))
        .otherwise(F.lit("underperforming")),
    )
