import argparse
from datetime import date, datetime, timedelta, timezone

from databricks.sdk.runtime import spark

from bookmyshow import generator, schemas


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).date()


def _run_full(args: argparse.Namespace, volume_root: str) -> None:
    as_of = args.run_date

    theatres_df = generator.generate_theatres(spark, n=args.n_theatres, seed=args.seed)
    movies_df = generator.generate_movies(spark, n=args.n_movies, seed=args.seed, as_of=as_of)
    customers_df = generator.generate_customers(
        spark, n=args.n_customers, seed=args.seed, signup_end=as_of
    )
    history_start = as_of - timedelta(days=args.history_days)
    shows_df = generator.generate_shows(
        spark,
        movies_df,
        theatres_df,
        n=args.n_shows,
        seed=args.seed,
        window_start=history_start,
        window_end=as_of,
    )
    bookings_df = generator.generate_bookings(
        spark,
        shows_df,
        theatres_df,
        num_shows=args.n_shows,
        num_customers=args.n_customers,
        n=args.n_bookings,
        seed=args.seed,
        booking_window_start=history_start,
        booking_window_end=as_of,
    )
    bookings_df = generator.inject_bad_records(bookings_df, seed=args.seed)

    generator.write_entity(theatres_df, volume_root, "theatres", "full_load", mode="overwrite")
    generator.write_entity(movies_df, volume_root, "movies", "full_load", mode="overwrite")
    generator.write_entity(customers_df, volume_root, "customers", "full_load", mode="overwrite")
    generator.write_entity(shows_df, volume_root, "shows", "full_load", mode="overwrite")
    generator.write_entity(bookings_df, volume_root, "bookings", "full_load", mode="overwrite")

    print(
        "Full generation complete: "
        f"{args.n_theatres} theatres, {args.n_movies} movies, {args.n_customers} customers, "
        f"{args.n_shows} shows, {args.n_bookings} bookings (+ injected bad records)."
    )


def _run_incremental(args: argparse.Namespace, volume_root: str) -> None:
    run_date = args.run_date

    movies_path = f"{volume_root}/movies/full_load"
    theatres_path = f"{volume_root}/theatres/full_load"
    try:
        movies_df = spark.read.parquet(movies_path)
        theatres_df = spark.read.parquet(theatres_path)
    except Exception as exc:
        raise RuntimeError(
            "No existing theatres/movies found under the raw landing volume. "
            "Run with --mode full at least once before running --mode incremental."
        ) from exc

    existing_customers = generator.existing_row_count(spark, f"{volume_root}/customers")
    existing_shows = generator.existing_row_count(spark, f"{volume_root}/shows")
    existing_bookings = generator.existing_row_count(spark, f"{volume_root}/bookings")

    new_customers_df = generator.generate_customers(
        spark,
        n=args.daily_new_customers,
        seed=args.seed,
        start_id=existing_customers,
        signup_start=run_date,
        signup_end=run_date,
    )
    new_shows_df = generator.generate_shows(
        spark,
        movies_df,
        theatres_df,
        n=args.daily_new_shows,
        seed=args.seed,
        start_id=existing_shows,
        window_start=run_date,
        window_end=run_date,
    )

    run_date_str = run_date.isoformat()
    generator.write_entity(new_customers_df, volume_root, "customers", run_date_str)
    generator.write_entity(new_shows_df, volume_root, "shows", run_date_str)

    # Bookings can reference any show landed so far (not just today's), so read the
    # full shows history back for the FK join.
    all_shows_df = spark.read.option("recursiveFileLookup", "true").parquet(f"{volume_root}/shows")

    bookings_df = generator.generate_bookings(
        spark,
        all_shows_df,
        theatres_df,
        num_shows=existing_shows + args.daily_new_shows,
        num_customers=existing_customers + args.daily_new_customers,
        n=args.daily_bookings,
        seed=args.seed,
        start_id=existing_bookings,
        booking_window_start=run_date,
        booking_window_end=run_date,
    )
    bookings_df = generator.inject_bad_records(bookings_df, seed=args.seed)
    generator.write_entity(bookings_df, volume_root, "bookings", run_date_str)

    print(
        f"Incremental generation for {run_date_str} complete: "
        f"{args.daily_new_customers} new customers, {args.daily_new_shows} new shows, "
        f"{args.daily_bookings} new bookings (+ injected bad records)."
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic BookMyShow booking-ecosystem data into the raw landing volume.",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument("--run-date", type=_parse_date, default=generator.today())
    parser.add_argument("--seed", type=int, default=schemas.DEFAULT_SEED)
    parser.add_argument("--volume-path", default=None, help="Override the raw_landing volume path")

    parser.add_argument("--n-theatres", type=int, default=schemas.DEFAULT_N_THEATRES)
    parser.add_argument("--n-movies", type=int, default=schemas.DEFAULT_N_MOVIES)
    parser.add_argument("--n-customers", type=int, default=schemas.DEFAULT_N_CUSTOMERS)
    parser.add_argument("--n-shows", type=int, default=schemas.DEFAULT_N_SHOWS)
    parser.add_argument("--n-bookings", type=int, default=schemas.DEFAULT_N_BOOKINGS)
    parser.add_argument("--history-days", type=int, default=schemas.DEFAULT_HISTORY_DAYS)

    parser.add_argument("--daily-new-customers", type=int, default=schemas.DEFAULT_DAILY_NEW_CUSTOMERS)
    parser.add_argument("--daily-new-shows", type=int, default=schemas.DEFAULT_DAILY_NEW_SHOWS)
    parser.add_argument("--daily-bookings", type=int, default=schemas.DEFAULT_DAILY_BOOKINGS)

    args = parser.parse_args()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.bronze_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {args.catalog}.{args.bronze_schema}.raw_landing")
    volume_root = args.volume_path or f"/Volumes/{args.catalog}/{args.bronze_schema}/raw_landing"

    if args.mode == "full":
        _run_full(args, volume_root)
    else:
        _run_incremental(args, volume_root)


if __name__ == "__main__":
    main()
