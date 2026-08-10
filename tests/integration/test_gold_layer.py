"""Integration tests for the deployed gold layer -- require a live Databricks
Connect session against a catalog the medallion pipeline has already refreshed.

Run with: uv run pytest tests/integration -v
Target a different catalog with: BOOKMYSHOW_CATALOG=bookmyshow_analytics_prod uv run pytest tests/integration
"""

import pytest


def test_fact_bookings_has_rows(spark, catalog):
    count = spark.table(f"{catalog}.gold.fact_bookings").count()
    assert count > 0, "fact_bookings should be populated after a pipeline refresh"


def test_fact_bookings_booking_id_is_unique(spark, catalog):
    fact = spark.table(f"{catalog}.gold.fact_bookings")
    total = fact.count()
    distinct = fact.select("booking_id").distinct().count()
    assert total == distinct, "booking_id should be a unique key in the fact table"


def test_fact_bookings_has_no_orphan_customers(spark, catalog):
    fact = spark.table(f"{catalog}.gold.fact_bookings")
    customers = spark.table(f"{catalog}.gold.dim_customers").select("customer_id")
    orphans = fact.join(customers, "customer_id", "left_anti").count()
    assert orphans == 0, "every booking should reference a known customer"


def test_fact_bookings_net_amount_is_never_negative(spark, catalog):
    negative_rows = spark.table(f"{catalog}.gold.fact_bookings").filter("net_amount < 0").count()
    assert negative_rows == 0


def test_dim_customers_loyalty_tier_is_a_known_value(spark, catalog):
    valid_tiers = {"Bronze", "Silver", "Gold", "Platinum"}
    tiers = {row.loyalty_tier for row in spark.table(f"{catalog}.gold.dim_customers").select("loyalty_tier").distinct().collect()}
    assert tiers.issubset(valid_tiers), f"unexpected loyalty tiers: {tiers - valid_tiers}"


def test_agg_daily_revenue_reconciles_with_fact_bookings(spark, catalog):
    fact_total = (
        spark.table(f"{catalog}.gold.fact_bookings")
        .filter("payment_status = 'SUCCESS'")
        .selectExpr("SUM(net_amount) AS total")
        .collect()[0]["total"]
    )
    agg_total = (
        spark.table(f"{catalog}.gold.agg_daily_revenue")
        .selectExpr("SUM(total_revenue) AS total")
        .collect()[0]["total"]
    )
    assert fact_total == pytest.approx(agg_total, rel=1e-6)


def test_agg_theatre_occupancy_rate_is_bounded(spark, catalog):
    out_of_range = spark.table(f"{catalog}.gold.agg_theatre_occupancy").filter(
        "occupancy_rate < 0 OR occupancy_rate > 1"
    ).count()
    assert out_of_range == 0


def test_outage_story_is_visible_in_gateway_health(spark, catalog):
    """Regression test for the synthetic data's payment-gateway-outage story: on
    its worst single day, PayFast in Mumbai/Delhi should show a sharply elevated
    failure rate relative to the gateway's baseline across all other cities and
    days. The outage is a single day out of ~165, so it must be isolated by
    MAX(failure_rate) rather than blended across the whole date range."""
    health = spark.table(f"{catalog}.gold.agg_payment_gateway_health")

    baseline_rate = (
        health.filter("payment_gateway = 'PayFast' AND city NOT IN ('Mumbai', 'Delhi')")
        .selectExpr("SUM(failed_transactions) / SUM(total_transactions) AS rate")
        .collect()[0]["rate"]
    )
    worst_day_rate = (
        health.filter("payment_gateway = 'PayFast' AND city IN ('Mumbai', 'Delhi')")
        .selectExpr("MAX(failure_rate) AS rate")
        .collect()[0]["rate"]
    )

    assert worst_day_rate > baseline_rate * 3, (
        f"expected the outage day to stand out (baseline={baseline_rate:.4f}, "
        f"worst mumbai/delhi day={worst_day_rate:.4f})"
    )


def test_gold_schema_has_all_expected_tables(spark, catalog):
    expected = {
        "dim_customers", "dim_movies", "dim_theatres", "dim_shows", "fact_bookings",
        "agg_daily_revenue", "agg_movie_performance", "agg_theatre_occupancy",
        "agg_payment_gateway_health", "agg_city_revenue",
    }
    actual = {row.tableName for row in spark.sql(f"SHOW TABLES IN {catalog}.gold").collect()}
    missing = expected - actual
    assert not missing, f"missing gold tables: {missing}"
