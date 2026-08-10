from pyspark import pipelines as dp

CATALOG = spark.conf.get("bookmyshow.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_bookings",
    comment=(
        "Booking-grain fact table denormalized with the show/movie/theatre attributes analysts "
        "filter by most often (city, screen type, genre, blockbuster flag), so dashboards and the "
        "bookings_metrics semantic model can slice without extra joins."
    ),
)
def fact_bookings():
    bookings = spark.read.table(f"{CATALOG}.silver.bookings")
    shows = spark.read.table(f"{CATALOG}.silver.shows").select(
        "show_id", "movie_id", "theatre_id", "screen_type", "show_date"
    )
    movies = spark.read.table(f"{CATALOG}.silver.movies").select("movie_id", "genre", "is_blockbuster")
    theatres = spark.read.table(f"{CATALOG}.silver.theatres").select("theatre_id", "city", "state")

    return (
        bookings.join(shows, "show_id")
        .join(movies, "movie_id")
        .join(theatres, "theatre_id")
        .select(
            "booking_id", "show_id", "customer_id", "movie_id", "theatre_id",
            "booking_timestamp", "booking_date", "show_date",
            "city", "state", "screen_type", "genre", "is_blockbuster",
            "seats_booked", "ticket_amount", "convenience_fee", "discount_amount", "net_amount",
            "payment_gateway", "payment_status", "booking_channel", "is_cancelled",
        )
    )
