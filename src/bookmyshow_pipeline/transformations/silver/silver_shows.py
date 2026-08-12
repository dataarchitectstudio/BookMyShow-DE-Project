from pyspark import pipelines as dp

SILVER_SCHEMA = spark.conf.get("silver_schema")


@dp.table(
    name=f"{SILVER_SCHEMA}.silver_shows",
    comment=(
        "Cleaned show dimension: structurally invalid rows dropped, and rows whose "
        "movie_id/theatre_id has no match in the silver layer filtered out via join."
    ),
)
@dp.expect_or_drop("valid_show_id", "show_id IS NOT NULL")
@dp.expect_or_drop("valid_base_price", "base_price >= 0")
def silver_shows():
    shows = spark.readStream.table("bronze_shows").select(
        "show_id", "movie_id", "theatre_id", "showtime", "screen_type", "base_price"
    )
    movies = spark.read.table(f"{SILVER_SCHEMA}.silver_movies").select("movie_id")
    theatres = spark.read.table(f"{SILVER_SCHEMA}.silver_theatres").select("theatre_id")
    # Referential integrity can't be an `@dp.expect*` predicate (no subqueries allowed
    # in expectations) -- an inner join against the cleaned dimensions is the mechanism
    # that drops orphan movie_id/theatre_id references.
    return shows.join(movies, "movie_id").join(theatres, "theatre_id")
