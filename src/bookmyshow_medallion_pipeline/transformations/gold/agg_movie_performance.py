from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bookmyshow.catalog")

# Mirrors bookmyshow_analytics.business_rules.MOVIE_PERFORMANCE_LABELS.
# Duplicated here (rather than imported) because the pipeline's managed Python
# environment doesn't resolve the project's own editable package; keep in sync
# with tests/unit/test_business_rules.py, which is the source of truth.
MOVIE_PERFORMANCE_LABELS = {
    "high": "Blockbuster Hit",
    "mid": "Steady Performer",
    "low": "Underperformer",
}


@dp.materialized_view(
    name=f"{CATALOG}.gold.agg_movie_performance",
    comment="Revenue and booking volume per movie, labeled relative to the catalog-wide revenue distribution.",
)
def agg_movie_performance():
    fact = spark.read.table(f"{CATALOG}.gold.fact_bookings").filter("payment_status = 'SUCCESS'")
    movies = spark.read.table(f"{CATALOG}.silver.movies").select("movie_id", "title")

    per_movie = fact.groupBy("movie_id", "genre", "is_blockbuster").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("seats_booked").alias("total_tickets_sold"),
        F.round(F.sum("net_amount"), 2).alias("total_revenue"),
        F.round(F.avg("ticket_amount"), 2).alias("avg_ticket_price"),
    )

    # Thresholds are the catalog's own revenue quartiles -- computed once per
    # refresh from this (small, per-movie) aggregate, not the raw booking rows.
    thresholds = per_movie.selectExpr(
        "percentile_approx(total_revenue, 0.75) AS high_threshold",
        "percentile_approx(total_revenue, 0.25) AS low_threshold",
    ).collect()[0]

    performance_expr = (
        F.when(F.col("total_revenue") >= F.lit(thresholds["high_threshold"]), F.lit(MOVIE_PERFORMANCE_LABELS["high"]))
        .when(F.col("total_revenue") <= F.lit(thresholds["low_threshold"]), F.lit(MOVIE_PERFORMANCE_LABELS["low"]))
        .otherwise(F.lit(MOVIE_PERFORMANCE_LABELS["mid"]))
    )

    return (
        per_movie.join(movies, "movie_id")
        .withColumn("performance_label", performance_expr)
        .select(
            "movie_id", "title", "genre", "is_blockbuster",
            "total_bookings", "total_tickets_sold", "total_revenue", "avg_ticket_price", "performance_label",
        )
    )
