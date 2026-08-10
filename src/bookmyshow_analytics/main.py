"""Post-refresh reporting task: prints yesterday's headline KPIs from the gold layer.

Runs as the python_wheel_task step of bookmyshow_daily_batch_job, after the
medallion pipeline has refreshed. Gives a quick sanity signal in the job run
log without needing to open the dashboard.
"""

import argparse

from databricks.sdk.runtime import spark


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print the latest daily revenue summary from the BookMyShow gold layer",
    )
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql("USE SCHEMA gold")

    latest_day = spark.sql(
        """
        SELECT
            booking_date,
            total_bookings,
            successful_bookings,
            failed_bookings,
            total_revenue,
            payment_failure_rate
        FROM agg_daily_revenue
        ORDER BY booking_date DESC
        LIMIT 1
        """
    )
    print("BookMyShow daily revenue summary (most recent day):")
    latest_day.show(truncate=False)


if __name__ == "__main__":
    main()
