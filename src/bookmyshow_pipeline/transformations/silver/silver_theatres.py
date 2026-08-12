from pyspark import pipelines as dp

SILVER_SCHEMA = spark.conf.get("silver_schema")

VALID_SCREEN_TYPES_SQL = "screen_type IN ('STANDARD', 'PREMIUM', 'IMAX', 'RECLINER')"


@dp.table(
    name=f"{SILVER_SCHEMA}.silver_theatres",
    comment="Cleaned theatre dimension: structurally invalid rows dropped.",
)
@dp.expect_or_drop("valid_theatre_id", "theatre_id IS NOT NULL")
@dp.expect_or_drop("valid_location", "city IS NOT NULL AND state IS NOT NULL")
@dp.expect_or_drop("valid_screen_type", VALID_SCREEN_TYPES_SQL)
@dp.expect_or_drop("valid_seat_capacity", "seat_capacity > 0")
def silver_theatres():
    return spark.readStream.table("bronze_theatres").select(
        "theatre_id", "name", "city", "state", "screen_type", "seat_capacity"
    )
