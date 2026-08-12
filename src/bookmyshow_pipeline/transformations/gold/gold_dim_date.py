from pyspark import pipelines as dp

GOLD_SCHEMA = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_dim_date",
    comment="Calendar dimension: 3 years back to 2 years forward of current_date(), materialized once (not streaming).",
    cluster_by=["date_key"],
)
def gold_dim_date():
    return spark.sql(
        """
        SELECT
            CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
            calendar_date AS date,
            YEAR(calendar_date) AS year,
            QUARTER(calendar_date) AS quarter,
            MONTH(calendar_date) AS month,
            DATE_FORMAT(calendar_date, 'MMMM') AS month_name,
            DAY(calendar_date) AS day_of_month,
            DATE_FORMAT(calendar_date, 'EEEE') AS day_name,
            DAYOFWEEK(calendar_date) IN (1, 7) AS is_weekend
        FROM (
            SELECT explode(sequence(
                date_sub(current_date(), 1095),
                date_add(current_date(), 730)
            )) AS calendar_date
        )
        """
    )
