from pyspark import pipelines as dp

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_shows",
    comment="Showtime dimension denormalized with movie and theatre descriptors for one-stop browsing.",
)
def dim_shows():
    shows = spark.read.table(f"{CATALOG}.silver.shows")
    movies = spark.read.table(f"{CATALOG}.silver.movies").select(
        "movie_id", "title", "genre", "language", "is_blockbuster"
    )
    theatres = spark.read.table(f"{CATALOG}.silver.theatres").select("theatre_id", "theatre_name", "city", "state")

    return (
        shows.join(movies, "movie_id")
        .join(theatres, "theatre_id")
        .select(
            "show_id", "movie_id", "title", "genre", "language", "is_blockbuster",
            "theatre_id", "theatre_name", "city", "state",
            "screen_number", "screen_type", "show_date", "show_time",
            "base_ticket_price", "total_seats",
        )
    )
