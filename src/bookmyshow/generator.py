"""Synthetic data generators for the BookMyShow booking ecosystem: theatres, movies,
customers, shows, and bookings.

Each `generate_*` function returns a Spark DataFrame matching the entity's landing
schema. Functions take an explicit `spark` session (rather than importing a
module-level global) so the same code path works from a job's `python_wheel_task`
(`databricks.sdk.runtime.spark`) and from interactive testing.

Story baked into the data (see schemas.py): a payment gateway silently fails for a
full day in two major cities (INCIDENT_GATEWAY / INCIDENT_CITIES / INCIDENT_DATE) —
this is what the payment-health dashboard (built in a later step) must surface
immediately. Movie popularity is power-law distributed so a small minority of movies
become box-office hits, and customer selection is skewed so a minority of customers
account for a disproportionate share of bookings (loyalty segmentation target).
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.utils import AnalysisException

from bookmyshow import schemas

# ---------------------------------------------------------------------------
# Pure helpers (unit-testable without a Spark session)
# ---------------------------------------------------------------------------


def today() -> date:
    return datetime.now(tz=timezone.utc).date()


def movie_popularity_weight(movie_id: str) -> float:
    """Deterministic pseudo-random popularity in (0, 1], power-law skewed so a
    small minority of movies are runaway hits and most are middling-to-low
    performers. Drives how many shows get scheduled for a movie, which in turn
    drives booking volume/revenue concentration.
    """
    digest = hashlib.sha256(movie_id.encode("utf-8")).hexdigest()
    uniform = int(digest[:8], 16) / 0xFFFFFFFF  # deterministic 0..1 given movie_id
    return uniform**4


def _cumulative_thresholds(weights: list[float]) -> list[float]:
    total = sum(weights)
    cum = 0.0
    out = []
    for w in weights:
        cum += w
        out.append(cum / total)
    return out


def weighted_index_column(weights: list[float], rand_col: Column) -> Column:
    """Spark Column selecting an integer index 0..len(weights)-1 from `rand_col`
    (expected uniform in [0, 1)) according to `weights`.
    """
    thresholds = _cumulative_thresholds(weights)
    col = F.when(rand_col < F.lit(thresholds[0]), F.lit(0))
    for i, thr in enumerate(thresholds[1:-1], start=1):
        col = col.when(rand_col < F.lit(thr), F.lit(i))
    return col.otherwise(F.lit(len(weights) - 1))


def index_to_value(values: list[str], idx_col: Column) -> Column:
    """Map an integer index column to one of a small set of literal strings."""
    return F.element_at(F.array(*[F.lit(v) for v in values]), idx_col + F.lit(1))


def rand_col(seed: int, offset: int) -> Column:
    """`F.rand()` seeded deterministically but distinctly per logical field, so
    e.g. city and screen_type aren't sampled from the same random stream.
    """
    return F.rand(seed=seed + offset)


def existing_row_count(spark: SparkSession, path: str) -> int:
    """Row count of already-landed data at `path` (recursing into date
    subfolders), or 0 if nothing has landed there yet.
    """
    try:
        return (
            spark.read.option("recursiveFileLookup", "true").parquet(path).count()
        )
    except AnalysisException:
        return 0


# ---------------------------------------------------------------------------
# Lookup / reference DataFrames
# ---------------------------------------------------------------------------


def _cities_df(spark: SparkSession) -> DataFrame:
    rows = [(i, city, state) for i, (city, state) in enumerate(schemas.CITIES)]
    return spark.createDataFrame(rows, ["city_idx", "city", "state"])


def _screen_types_df(spark: SparkSession) -> DataFrame:
    rows = []
    for i, screen_type in enumerate(schemas.SCREEN_TYPES):
        cap_lo, cap_hi = schemas.SCREEN_TYPE_CAPACITY_RANGE[screen_type]
        price_lo, price_hi = schemas.SCREEN_TYPE_BASE_PRICE_RANGE[screen_type]
        rows.append((i, screen_type, cap_lo, cap_hi, price_lo, price_hi))
    return spark.createDataFrame(
        rows,
        ["screen_type_idx", "screen_type", "cap_lo", "cap_hi", "price_lo", "price_hi"],
    )


# ---------------------------------------------------------------------------
# Faker-backed pandas UDF
# ---------------------------------------------------------------------------


def _faker_udf(field: str):
    @F.pandas_udf(StringType())
    def _udf(seeds: pd.Series) -> pd.Series:
        from faker import Faker  # installed via pyproject.toml; import inside the UDF

        fake = Faker()
        values = []
        for seed_val in seeds:
            fake.seed_instance(int(seed_val))
            if field == "theatre_name":
                values.append(f"{fake.city()} {fake.company_suffix()} Cinemas")
            elif field == "movie_title":
                values.append(fake.catch_phrase().title())
            elif field == "person_name":
                values.append(fake.name())
            elif field == "email":
                # Built manually (not fake.unique.email()) so uniqueness holds
                # globally across Spark partitions, not just within one Faker
                # instance's local tracking.
                values.append(f"{fake.user_name()}{int(seed_val)}@{fake.free_email_domain()}")
            elif field == "phone":
                values.append(fake.msisdn()[:10])
            else:
                raise ValueError(f"Unknown faker field: {field}")
        return pd.Series(values)

    return _udf


# ---------------------------------------------------------------------------
# Entity generators
# ---------------------------------------------------------------------------


def generate_theatres(
    spark: SparkSession,
    n: int = schemas.DEFAULT_N_THEATRES,
    seed: int = schemas.DEFAULT_SEED,
) -> DataFrame:
    base = (
        spark.range(0, n, numPartitions=8)
        .withColumnRenamed("id", "row_id")
        .withColumn("city_rand", rand_col(seed, 1))
        .withColumn("screen_rand", rand_col(seed, 2))
        .withColumn("capacity_rand", rand_col(seed, 3))
        .withColumn("city_idx", weighted_index_column(schemas.CITY_WEIGHTS, F.col("city_rand")))
        .withColumn(
            "screen_type_idx",
            weighted_index_column(schemas.SCREEN_TYPE_WEIGHTS, F.col("screen_rand")),
        )
    )
    return (
        base.join(_cities_df(spark), on="city_idx")
        .join(_screen_types_df(spark), on="screen_type_idx")
        .withColumn("theatre_id", F.format_string("THR-%05d", (F.col("row_id") + 1).cast("int")))
        .withColumn("name", _faker_udf("theatre_name")(F.col("row_id")))
        .withColumn(
            "seat_capacity",
            (F.col("cap_lo") + F.col("capacity_rand") * (F.col("cap_hi") - F.col("cap_lo"))).cast(
                "int"
            ),
        )
        .select("theatre_id", "name", "city", "state", "screen_type", "seat_capacity")
    )


def generate_movies(
    spark: SparkSession,
    n: int = schemas.DEFAULT_N_MOVIES,
    seed: int = schemas.DEFAULT_SEED,
    as_of: date | None = None,
) -> DataFrame:
    as_of = as_of or today()
    genres_df = spark.createDataFrame(list(enumerate(schemas.GENRES)), ["genre_idx", "genre"])
    base = (
        spark.range(0, n, numPartitions=8)
        .withColumnRenamed("id", "row_id")
        .withColumn("genre_rand", rand_col(seed, 10))
        .withColumn("duration_rand", F.randn(seed=seed + 11))
        .withColumn("recency_rand", rand_col(seed, 12))
        .withColumn("genre_idx", weighted_index_column(schemas.GENRE_WEIGHTS, F.col("genre_rand")))
    )
    return (
        base.join(genres_df, on="genre_idx")
        .withColumn("movie_id", F.format_string("MOV-%05d", (F.col("row_id") + 1).cast("int")))
        .withColumn("title", _faker_udf("movie_title")(F.col("row_id")))
        .withColumn(
            "duration_minutes",
            F.greatest(
                F.lit(80),
                F.least(F.lit(220), (F.lit(130) + F.col("duration_rand") * 20).cast("int")),
            ),
        )
        # recency_rand**2 skews release dates toward "recent" (small offset from as_of).
        .withColumn(
            "release_date",
            F.date_sub(
                F.lit(as_of), (F.pow(F.col("recency_rand"), F.lit(2)) * F.lit(3 * 365)).cast("int")
            ),
        )
        .select("movie_id", "title", "genre", "duration_minutes", "release_date")
    )


def generate_customers(
    spark: SparkSession,
    n: int = schemas.DEFAULT_N_CUSTOMERS,
    seed: int = schemas.DEFAULT_SEED,
    start_id: int = 0,
    signup_start: date | None = None,
    signup_end: date | None = None,
) -> DataFrame:
    signup_end = signup_end or today()
    signup_start = signup_start or (signup_end - timedelta(days=3 * 365))
    span_days = max((signup_end - signup_start).days, 0) + 1

    base = (
        spark.range(0, n, numPartitions=32)
        .withColumnRenamed("id", "row_id")
        .withColumn("global_id", F.col("row_id") + F.lit(start_id))
        .withColumn("city_rand", rand_col(seed, 20))
        .withColumn("signup_rand", rand_col(seed, 21))
        .withColumn("city_idx", weighted_index_column(schemas.CITY_WEIGHTS, F.col("city_rand")))
    )
    return (
        base.join(_cities_df(spark), on="city_idx")
        .withColumn(
            "customer_id", F.format_string("CUST-%07d", (F.col("global_id") + 1).cast("int"))
        )
        .withColumn("name", _faker_udf("person_name")(F.col("global_id")))
        .withColumn("email", _faker_udf("email")(F.col("global_id")))
        .withColumn("phone", _faker_udf("phone")(F.col("global_id")))
        .withColumn(
            "signup_date",
            F.date_add(F.lit(signup_start), (F.col("signup_rand") * span_days).cast("int")),
        )
        .select("customer_id", "name", "email", "phone", "city", "signup_date")
    )


def generate_shows(
    spark: SparkSession,
    movies_df: DataFrame,
    theatres_df: DataFrame,
    n: int = schemas.DEFAULT_N_SHOWS,
    seed: int = schemas.DEFAULT_SEED,
    start_id: int = 0,
    window_start: date | None = None,
    window_end: date | None = None,
) -> DataFrame:
    window_end = window_end or today()
    window_start = window_start or window_end
    span_days = max((window_end - window_start).days, 0) + 1

    # Movie popularity -> probability buckets. movies_df is a small dimension table
    # (hundreds of rows), so collecting it to the driver once to build a static
    # weighted-sampling expression is cheap and standard practice (unlike collecting
    # the large fact tables generated below).
    popularity_udf = F.udf(movie_popularity_weight, DoubleType())
    movie_rows = (
        movies_df.select("movie_id")
        .withColumn("weight", popularity_udf(F.col("movie_id")))
        .collect()
    )
    total_weight = sum(r["weight"] for r in movie_rows) or 1.0
    cum = 0.0
    buckets = []
    for r in movie_rows:
        lo = cum
        cum += r["weight"] / total_weight
        buckets.append((r["movie_id"], lo, cum))
    if buckets:
        movie_id, lo, _ = buckets[-1]
        buckets[-1] = (movie_id, lo, 1.0000001)
    movie_buckets = spark.createDataFrame(buckets, ["movie_id", "lo", "hi"])

    theatre_ids = [r["theatre_id"] for r in theatres_df.select("theatre_id").collect()]
    theatre_id_array = F.array(*[F.lit(t) for t in theatre_ids])
    theatre_idx = (rand_col(seed, 31) * F.lit(len(theatre_ids))).cast("int")

    base = (
        spark.range(0, n, numPartitions=16)
        .withColumnRenamed("id", "row_id")
        .withColumn("global_id", F.col("row_id") + F.lit(start_id))
        .withColumn("movie_rand", rand_col(seed, 30))
        .withColumn("day_rand", rand_col(seed, 32))
        .withColumn("hour_rand", rand_col(seed, 33))
        .withColumn("price_rand", rand_col(seed, 34))
        .withColumn("theatre_id", F.element_at(theatre_id_array, theatre_idx + F.lit(1)))
    )

    return (
        base.join(
            movie_buckets, (F.col("movie_rand") >= F.col("lo")) & (F.col("movie_rand") < F.col("hi"))
        )
        .join(theatres_df.select("theatre_id", "screen_type"), on="theatre_id")
        .join(
            F.broadcast(_screen_types_df(spark).select("screen_type", "price_lo", "price_hi")),
            on="screen_type",
        )
        .withColumn("show_id", F.format_string("SHOW-%07d", (F.col("global_id") + 1).cast("int")))
        .withColumn(
            "show_date", F.date_add(F.lit(window_start), (F.col("day_rand") * span_days).cast("int"))
        )
        .withColumn("show_hour", (F.lit(9) + F.col("hour_rand") * 14).cast("int"))  # 9am - 11pm
        .withColumn(
            "showtime",
            F.to_timestamp(
                F.concat_ws(" ", F.col("show_date"), F.format_string("%02d:00:00", F.col("show_hour")))
            ),
        )
        .withColumn(
            "base_price",
            (F.col("price_lo") + F.col("price_rand") * (F.col("price_hi") - F.col("price_lo"))).cast(
                "int"
            ),
        )
        .select("show_id", "movie_id", "theatre_id", "showtime", "screen_type", "base_price")
    )


def generate_bookings(
    spark: SparkSession,
    shows_df: DataFrame,
    theatres_df: DataFrame,
    num_shows: int,
    num_customers: int,
    n: int = schemas.DEFAULT_N_BOOKINGS,
    seed: int = schemas.DEFAULT_SEED,
    start_id: int = 0,
    booking_window_start: date | None = None,
    booking_window_end: date | None = None,
) -> DataFrame:
    booking_window_end = booking_window_end or today()
    booking_window_start = booking_window_start or booking_window_end
    span_days = max((booking_window_end - booking_window_start).days, 0) + 1

    show_idx = (rand_col(seed, 40) * F.lit(num_shows)).cast("int")
    # customer_idx skewed toward low indices (pow > 1) -> a minority of customers
    # generate a disproportionate share of bookings (loyalty-segmentation target).
    customer_idx = (F.pow(rand_col(seed, 41), F.lit(3)) * F.lit(num_customers)).cast("int")

    base = (
        spark.range(0, n, numPartitions=64)
        .withColumnRenamed("id", "row_id")
        .withColumn("global_id", F.col("row_id") + F.lit(start_id))
        .withColumn("show_id", F.format_string("SHOW-%07d", show_idx + F.lit(1)))
        .withColumn("customer_id", F.format_string("CUST-%07d", customer_idx + F.lit(1)))
        .withColumn("channel_rand", rand_col(seed, 42))
        .withColumn("gateway_rand", rand_col(seed, 43))
        .withColumn("status_rand", rand_col(seed, 44))
        .withColumn("cancel_rand", rand_col(seed, 45))
        .withColumn("seats_rand", rand_col(seed, 46))
        .withColumn("discount_rand", rand_col(seed, 47))
        .withColumn("amount_noise", rand_col(seed, 48))
        .withColumn(
            "booking_day",
            F.date_add(F.lit(booking_window_start), (rand_col(seed, 49) * span_days).cast("int")),
        )
        .withColumn(
            "booking_channel",
            index_to_value(
                schemas.BOOKING_CHANNELS,
                weighted_index_column(schemas.BOOKING_CHANNEL_WEIGHTS, F.col("channel_rand")),
            ),
        )
        .withColumn(
            "payment_gateway",
            index_to_value(
                schemas.PAYMENT_GATEWAYS,
                weighted_index_column(schemas.PAYMENT_GATEWAY_WEIGHTS, F.col("gateway_rand")),
            ),
        )
    )

    df = base.join(
        shows_df.select("show_id", "movie_id", "theatre_id", "base_price"), on="show_id"
    ).join(F.broadcast(theatres_df.select("theatre_id", "city")), on="theatre_id")

    baseline_status = index_to_value(
        schemas.PAYMENT_STATUSES,
        weighted_index_column(schemas.BASELINE_PAYMENT_STATUS_WEIGHTS, F.col("status_rand")),
    )
    is_incident = (
        (F.col("payment_gateway") == F.lit(schemas.INCIDENT_GATEWAY))
        & F.col("city").isin(schemas.INCIDENT_CITIES)
        & (F.col("booking_day") == F.lit(schemas.INCIDENT_DATE))
    )
    incident_status = F.when(
        F.col("status_rand") < F.lit(schemas.INCIDENT_FAILURE_RATE), F.lit("failed")
    ).otherwise(F.lit("success"))

    df = (
        df.withColumn(
            "seats_booked", (F.lit(1) + F.floor(F.pow(F.col("seats_rand"), F.lit(2)) * 5)).cast("int")
        )
        .withColumn(
            "ticket_amount",
            (
                F.col("seats_booked")
                * F.col("base_price")
                * (F.lit(0.95) + F.col("amount_noise") * 0.1)
            ).cast("int"),
        )
        .withColumn("fees", (F.col("ticket_amount") * (F.lit(0.02) + F.col("discount_rand") * 0.03)).cast("int"))
        .withColumn(
            "discount",
            F.when(
                F.col("discount_rand") < 0.2,
                (F.col("ticket_amount") * (F.lit(0.05) + F.col("discount_rand") * 0.15)).cast("int"),
            ).otherwise(F.lit(0)),
        )
        .withColumn("payment_status", F.when(is_incident, incident_status).otherwise(baseline_status))
        .withColumn(
            "is_cancelled",
            (F.col("payment_status") == F.lit("refunded"))
            | ((F.col("payment_status") == F.lit("success")) & (F.col("cancel_rand") < 0.015)),
        )
        .withColumn("booking_id", F.format_string("BKG-%08d", (F.col("global_id") + 1).cast("int")))
        .withColumnRenamed("booking_day", "booking_date")
        .select(
            "booking_id",
            "show_id",
            "customer_id",
            "seats_booked",
            "ticket_amount",
            "fees",
            "discount",
            "payment_gateway",
            "payment_status",
            "booking_channel",
            "is_cancelled",
            "booking_date",
        )
    )
    return df


def inject_bad_records(
    df: DataFrame, fraction: float = schemas.BAD_RECORD_FRACTION, seed: int = schemas.DEFAULT_SEED
) -> DataFrame:
    """Corrupt a small slice of bookings so the silver-layer data-quality
    expectations (built in a later step) have real violations to catch: an orphan
    customer FK, a negative ticket amount, a null booking_id, and a handful of
    duplicate rows for AUTO CDC dedup to exercise.
    """
    is_bad = F.rand(seed=seed + 90) < F.lit(fraction)
    kind = F.rand(seed=seed + 91)

    corrupted = (
        df.withColumn(
            "customer_id",
            F.when(is_bad & (kind < 1 / 3), F.lit("CUST-9999999")).otherwise(F.col("customer_id")),
        )
        .withColumn(
            "ticket_amount",
            F.when(is_bad & (kind >= 1 / 3) & (kind < 2 / 3), F.col("ticket_amount") * -1).otherwise(
                F.col("ticket_amount")
            ),
        )
        .withColumn(
            "booking_id",
            F.when(is_bad & (kind >= 2 / 3), F.lit(None).cast("string")).otherwise(F.col("booking_id")),
        )
    )
    duplicates = corrupted.sample(fraction=fraction / 3, seed=seed + 92)
    return corrupted.unionByName(duplicates)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def write_entity(
    df: DataFrame, volume_root: str, entity: str, partition_value: str, mode: str = "append"
) -> str:
    path = f"{volume_root}/{entity}/{partition_value}"
    df.write.mode(mode).parquet(path)
    return path
