# Databricks notebook source
# MAGIC %md
# MAGIC # BookMyShow synthetic data generator
# MAGIC
# MAGIC Generates a realistic, story-driven movie-ticketing dataset into the `raw`
# MAGIC landing volume, ready for the `bookmyshow_medallion_pipeline` to ingest.
# MAGIC
# MAGIC **The story:** a blockbuster release drives a booking surge across the
# MAGIC country. Partway through, a payment gateway outage hits two major cities for
# MAGIC a single day, spiking payment failures and denting revenue in a way that's
# MAGIC traceable back to a specific gateway, date, and pair of cities -- exactly the
# MAGIC kind of incident the gold layer and dashboard are built to surface.
# MAGIC
# MAGIC Tables written (all under `/Volumes/{catalog}/raw/landing_zone/`):
# MAGIC theatres, movies, customers, shows, bookings.
# MAGIC
# MAGIC **Implementation note:** rows are generated locally with NumPy/pandas/Faker
# MAGIC (vectorized, no Spark UDFs) and handed to Spark only for the final
# MAGIC `createDataFrame(...).write.parquet(...)` -- this workspace's serverless
# MAGIC Python-UDF sandbox is currently unavailable, and generation at this scale is
# MAGIC comfortably fast locally regardless.

# COMMAND ----------

import os

import numpy as np
import pandas as pd
from faker import Faker

from bookmyshow_analytics.business_rules import (
    calculate_discount_amount,
    calculate_net_amount,
    is_within_outage_window,
    pick_weighted_category,
    resolve_payment_status,
)

try:
    _ = spark  # probes whether the notebook/job runtime already provided one
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless(True).getOrCreate()


def get_param(name: str, default: str | None = None) -> str:
    """Read a job/notebook widget when running on Databricks, else an env var (for local runs)."""
    try:
        return dbutils.widgets.get(name)
    except Exception:  # noqa: BLE001 -- dbutils raises different types for "undefined" vs "unset"
        value = os.environ.get(name.upper(), default)
        if value is None:
            raise ValueError(f"Missing required parameter: {name}")
        return value


# COMMAND ----------

# MAGIC %md ## Parameters & story configuration

# COMMAND ----------

CATALOG = get_param("catalog")
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/landing_zone"
SEED = int(get_param("seed", "42"))

N_THEATRES = int(get_param("n_theatres", "200"))
N_MOVIES = int(get_param("n_movies", "600"))
N_CUSTOMERS = int(get_param("n_customers", "150000"))
N_SHOWS = int(get_param("n_shows", "120000"))
N_BOOKINGS = int(get_param("n_bookings", "2000000"))

BLOCKBUSTER_SHARE = 0.05          # ~5% of movies are blockbusters
BLOCKBUSTER_SHOW_REPLICATION = 8  # blockbusters get ~8x more scheduled shows
FREQUENT_CUSTOMER_SHARE = 0.12    # ~12% of customers are frequent bookers
FREQUENT_CUSTOMER_REPLICATION = 6

OUTAGE_GATEWAY = "PayFast"
OUTAGE_CITIES = ["Mumbai", "Delhi"]
OUTAGE_DAYS_AGO = 45  # relative to generation date -> a fixed, traceable past incident

CITY_WEIGHTS = [
    ("Mumbai", 18), ("Delhi", 15), ("Bengaluru", 14), ("Hyderabad", 10),
    ("Chennai", 10), ("Pune", 8), ("Kolkata", 8), ("Ahmedabad", 6),
    ("Jaipur", 5), ("Lucknow", 6),
]
CITY_STATE = {
    "Mumbai": "Maharashtra", "Pune": "Maharashtra", "Delhi": "Delhi",
    "Bengaluru": "Karnataka", "Hyderabad": "Telangana", "Chennai": "Tamil Nadu",
    "Kolkata": "West Bengal", "Ahmedabad": "Gujarat", "Jaipur": "Rajasthan",
    "Lucknow": "Uttar Pradesh",
}
GENRE_WEIGHTS = [
    ("Action", 25), ("Drama", 20), ("Comedy", 15), ("Thriller", 15),
    ("Romance", 10), ("Sci-Fi", 8), ("Horror", 7),
]
LANGUAGE_WEIGHTS = [
    ("Hindi", 40), ("English", 25), ("Tamil", 12), ("Telugu", 12),
    ("Kannada", 6), ("Malayalam", 5),
]
CENSOR_WEIGHTS = [("U", 20), ("UA", 45), ("A", 35)]
SCREEN_TYPE_WEIGHTS = [("Standard", 55), ("3D", 30), ("IMAX", 15)]
SHOW_TIME_WEIGHTS = [("10:00", 15), ("13:00", 20), ("16:00", 22), ("19:00", 28), ("22:00", 15)]
GATEWAY_WEIGHTS = [("Razorpay", 40), ("Paytm", 25), ("PayFast", 20), ("CCAvenue", 10), ("Stripe", 5)]
CHANNEL_WEIGHTS = [("App", 65), ("Web", 35)]
DISCOUNT_TIER_WEIGHTS = [("Bronze", 50), ("Silver", 30), ("Gold", 15), ("Platinum", 5)]
SEATS_BOOKED_WEIGHTS = [(1, 15), (2, 40), (3, 20), (4, 15), (5, 6), (6, 4)]

THEATRE_BRANDS = ["PVR", "INOX", "Cinepolis", "Miraj Cinemas", "Carnival Cinemas"]
TITLE_ADJECTIVES = [
    "Silent", "Crimson", "Broken", "Eternal", "Hidden", "Golden", "Last", "Midnight",
    "Wild", "Sacred", "Rising", "Forgotten", "Dark", "Electric", "Final", "Lost",
    "Restless", "Burning", "Endless", "Fearless", "Shattered", "Radiant", "Savage",
    "Distant", "Vanishing", "Blazing", "Frozen", "Roaring", "Untold", "Immortal",
]
TITLE_NOUNS = [
    "Storm", "Legacy", "Journey", "Warriors", "Dreams", "Shadows", "Kingdom", "Horizon",
    "Revenge", "Echoes", "Empire", "Fire", "Destiny", "Rebels", "Symphony", "Odyssey",
    "Redemption", "Vendetta", "Chronicles", "Uprising", "Skyline", "Labyrinth", "Mirage",
    "Sanctuary", "Wilderness", "Inferno", "Serenade", "Conquest", "Paradox", "Reckoning",
]

rng = np.random.default_rng(SEED)
fake = Faker("en_IN")
Faker.seed(SEED)


def weighted_labels(options: list[tuple], size: int, draws: np.ndarray) -> np.ndarray:
    """Vectorized `pick_weighted_category` over an array of uniform(0,1) draws."""
    return np.array([pick_weighted_category(r, options) for r in draws])


def build_pool(values: np.ndarray, boost_mask: np.ndarray, replication: int) -> np.ndarray:
    """Replicate `boost_mask`-selected values `replication`x so uniform sampling over
    the pool naturally over-represents them (used for blockbuster shows / repeat customers)."""
    counts = np.where(boost_mask, replication, 1)
    return np.repeat(values, counts)


# COMMAND ----------

# MAGIC %md ## Infrastructure

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing_zone")

# COMMAND ----------

# MAGIC %md ## Theatres (master table)

# COMMAND ----------

theatre_cities = weighted_labels(CITY_WEIGHTS, N_THEATRES, rng.random(N_THEATRES))
theatres_pdf = pd.DataFrame(
    {
        "theatre_id": [f"THR-{i + 1:05d}" for i in range(N_THEATRES)],
        "theatre_name": [
            f"{rng.choice(THEATRE_BRANDS)} {city} - {fake.street_name()}" for city in theatre_cities
        ],
        "city": theatre_cities,
        "total_screens": rng.integers(2, 13, size=N_THEATRES),
        "opened_date": [
            pd.Timestamp.today().normalize() - pd.Timedelta(days=int(d))
            for d in rng.integers(0, 3650, size=N_THEATRES)
        ],
    }
)
theatres_pdf["state"] = theatres_pdf["city"].map(CITY_STATE)
theatres_pdf = theatres_pdf[["theatre_id", "theatre_name", "city", "state", "total_screens", "opened_date"]]

spark.createDataFrame(theatres_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/theatres")
print(f"theatres written: {len(theatres_pdf)}")

# COMMAND ----------

# MAGIC %md ## Movies (master table)

# COMMAND ----------


def make_title(index: int, used_titles: set) -> str:
    local_rng = np.random.default_rng(SEED + index)
    title = f"{local_rng.choice(TITLE_ADJECTIVES)} {local_rng.choice(TITLE_NOUNS)}"
    suffix = 2
    base_title = title
    while title in used_titles:
        title = f"{base_title} Part {suffix}"
        suffix += 1
    used_titles.add(title)
    return title


_used_titles: set = set()
movie_titles = [make_title(i, _used_titles) for i in range(N_MOVIES)]
is_blockbuster = rng.random(N_MOVIES) < BLOCKBUSTER_SHARE

movies_pdf = pd.DataFrame(
    {
        "movie_id": [f"MOV-{i + 1:05d}" for i in range(N_MOVIES)],
        "title": movie_titles,
        "genre": weighted_labels(GENRE_WEIGHTS, N_MOVIES, rng.random(N_MOVIES)),
        "language": weighted_labels(LANGUAGE_WEIGHTS, N_MOVIES, rng.random(N_MOVIES)),
        "censor_rating": weighted_labels(CENSOR_WEIGHTS, N_MOVIES, rng.random(N_MOVIES)),
        "release_date": [
            pd.Timestamp.today().normalize() - pd.Timedelta(days=int(d))
            for d in rng.integers(0, 730, size=N_MOVIES)
        ],
        "duration_minutes": np.clip(rng.normal(130, 25, size=N_MOVIES), 80, 220).round().astype(int),
        "average_rating": np.clip(rng.normal(3.6, 0.6, size=N_MOVIES), 1.0, 5.0).round(1),
        "is_blockbuster": is_blockbuster,
    }
)

spark.createDataFrame(movies_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/movies")
print(f"movies written: {len(movies_pdf)} ({int(is_blockbuster.sum())} blockbusters)")

# COMMAND ----------

# MAGIC %md ## Customers (master table)

# COMMAND ----------

customer_cities = weighted_labels(CITY_WEIGHTS, N_CUSTOMERS, rng.random(N_CUSTOMERS))
is_frequent_flyer = rng.random(N_CUSTOMERS) < FREQUENT_CUSTOMER_SHARE

customers_pdf = pd.DataFrame(
    {
        "customer_id": [f"CUST-{i + 1:07d}" for i in range(N_CUSTOMERS)],
        "full_name": [fake.name() for _ in range(N_CUSTOMERS)],
        "email": [fake.unique.free_email() for _ in range(N_CUSTOMERS)],
        "phone_number": [fake.msisdn()[-10:] for _ in range(N_CUSTOMERS)],
        "city": customer_cities,
        "signup_date": [
            pd.Timestamp.today().normalize() - pd.Timedelta(days=int(d))
            for d in (rng.random(N_CUSTOMERS) ** 2 * 1095).astype(int)  # skewed toward recent signups
        ],
        "preferred_genre": weighted_labels(GENRE_WEIGHTS, N_CUSTOMERS, rng.random(N_CUSTOMERS)),
        "preferred_channel": weighted_labels(CHANNEL_WEIGHTS, N_CUSTOMERS, rng.random(N_CUSTOMERS)),
        "marketing_opt_in": rng.random(N_CUSTOMERS) < 0.6,
    }
)
customers_pdf["state"] = customers_pdf["city"].map(CITY_STATE)
customers_pdf = customers_pdf[
    [
        "customer_id", "full_name", "email", "phone_number", "city", "state",
        "signup_date", "preferred_genre", "preferred_channel", "marketing_opt_in",
    ]
]

spark.createDataFrame(customers_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/customers")
print(f"customers written: {len(customers_pdf)} ({int(is_frequent_flyer.sum())} frequent bookers)")

# COMMAND ----------

# MAGIC %md ## Shows (blockbusters get disproportionately more shows)

# COMMAND ----------

movie_pool = build_pool(movies_pdf["movie_id"].to_numpy(), is_blockbuster, BLOCKBUSTER_SHOW_REPLICATION)
show_movie_ids = rng.choice(movie_pool, size=N_SHOWS)
show_theatre_idx = rng.integers(0, N_THEATRES, size=N_SHOWS)
show_screen_types = weighted_labels(SCREEN_TYPE_WEIGHTS, N_SHOWS, rng.random(N_SHOWS))

theatre_total_screens = theatres_pdf["total_screens"].to_numpy()[show_theatre_idx]
screen_numbers = (rng.random(N_SHOWS) * theatre_total_screens).astype(int) + 1

base_price = np.select(
    [show_screen_types == "IMAX", show_screen_types == "3D"],
    [np.round(300 + rng.random(N_SHOWS) * 250, -1), np.round(220 + rng.random(N_SHOWS) * 130, -1)],
    default=np.round(150 + rng.random(N_SHOWS) * 100, -1),
)
total_seats = np.select(
    [show_screen_types == "IMAX", show_screen_types == "3D"],
    [(rng.random(N_SHOWS) * 100).astype(int) + 200, (rng.random(N_SHOWS) * 50).astype(int) + 100],
    default=(rng.random(N_SHOWS) * 60).astype(int) + 120,
)
# Show dates span 150 days back to 14 days ahead (past + upcoming scheduled shows).
show_date_offsets = (rng.random(N_SHOWS) * 164).astype(int) - 14

shows_pdf = pd.DataFrame(
    {
        "show_id": [f"SHW-{i + 1:06d}" for i in range(N_SHOWS)],
        "movie_id": show_movie_ids,
        "theatre_id": theatres_pdf["theatre_id"].to_numpy()[show_theatre_idx],
        "screen_number": screen_numbers,
        "screen_type": show_screen_types,
        "show_date": [
            pd.Timestamp.today().normalize() + pd.Timedelta(days=int(-d)) for d in show_date_offsets
        ],
        "show_time": weighted_labels(SHOW_TIME_WEIGHTS, N_SHOWS, rng.random(N_SHOWS)),
        "base_ticket_price": base_price,
        "total_seats": total_seats,
    }
)

spark.createDataFrame(shows_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/shows")
print(f"shows written: {len(shows_pdf)}")

# COMMAND ----------

# MAGIC %md ## Bookings (main fact table, ~2M rows carrying the outage story)

# COMMAND ----------

OUTAGE_DATE = (pd.Timestamp.today().normalize() - pd.Timedelta(days=OUTAGE_DAYS_AGO)).date()

# Frequent customers are replicated in the pool -- creates the repeat-customer /
# 80-20 revenue concentration the "Repeat Customer Rate" KPI is built to surface.
customer_pool = build_pool(
    customers_pdf["customer_id"].to_numpy(), is_frequent_flyer, FREQUENT_CUSTOMER_REPLICATION
)
booking_customer_ids = rng.choice(customer_pool, size=N_BOOKINGS)

# Shows are sampled uniformly -- blockbuster demand skew is already baked in
# because blockbuster movies have ~8x more scheduled shows (see above).
show_lookup = shows_pdf.set_index("show_id")
show_theatre = shows_pdf["theatre_id"].to_numpy()
theatre_city_map = theatres_pdf.set_index("theatre_id")["city"].to_dict()

booking_show_idx = rng.integers(0, N_SHOWS, size=N_BOOKINGS)
booking_show_ids = shows_pdf["show_id"].to_numpy()[booking_show_idx]
booking_show_price = shows_pdf["base_ticket_price"].to_numpy()[booking_show_idx]
booking_show_date = shows_pdf["show_date"].to_numpy()[booking_show_idx]
booking_show_time = shows_pdf["show_time"].to_numpy()[booking_show_idx]
booking_theatre_id = show_theatre[booking_show_idx]
booking_city = np.array([theatre_city_map[t] for t in booking_theatre_id])

seats_booked = weighted_labels(SEATS_BOOKED_WEIGHTS, N_BOOKINGS, rng.random(N_BOOKINGS)).astype(int)
payment_gateways = weighted_labels(GATEWAY_WEIGHTS, N_BOOKINGS, rng.random(N_BOOKINGS))
booking_channels = weighted_labels(CHANNEL_WEIGHTS, N_BOOKINGS, rng.random(N_BOOKINGS))
discount_tiers = weighted_labels(DISCOUNT_TIER_WEIGHTS, N_BOOKINGS, rng.random(N_BOOKINGS))

lead_hours = rng.random(N_BOOKINGS) * 72 + 1
show_datetime = pd.to_datetime(
    pd.Series(booking_show_date).dt.strftime("%Y-%m-%d") + " " + pd.Series(booking_show_time)
)
booking_timestamp = show_datetime - pd.to_timedelta(lead_hours, unit="h")
booking_date = booking_timestamp.dt.date

ticket_amount = np.round(seats_booked * booking_show_price, 2)
convenience_fee = np.round(ticket_amount * (0.03 + rng.random(N_BOOKINGS) * 0.02) + 15, 2)
discount_amount = np.array(
    [calculate_discount_amount(amt, tier) for amt, tier in zip(ticket_amount, discount_tiers)]
)
net_amount = np.array(
    [
        calculate_net_amount(t, f, d)
        for t, f, d in zip(ticket_amount, convenience_fee, discount_amount)
    ]
)

failure_rolls = rng.random(N_BOOKINGS)
refund_rolls = rng.random(N_BOOKINGS)
outage_date_str = str(OUTAGE_DATE)
payment_status = np.array(
    [
        resolve_payment_status(
            is_within_outage_window(gw, city, OUTAGE_GATEWAY, OUTAGE_CITIES, str(bdate), outage_date_str),
            froll,
            rroll,
        )
        for gw, city, bdate, froll, rroll in zip(
            payment_gateways, booking_city, booking_date, failure_rolls, refund_rolls
        )
    ]
)
cancel_rolls = rng.random(N_BOOKINGS)
is_cancelled = (payment_status == "SUCCESS") & (cancel_rolls < 0.04)

bookings_pdf = pd.DataFrame(
    {
        "booking_id": [f"BKG-{i + 1:08d}" for i in range(N_BOOKINGS)],
        "show_id": booking_show_ids,
        "customer_id": booking_customer_ids,
        "booking_timestamp": booking_timestamp,
        "booking_date": booking_date,
        "seats_booked": seats_booked,
        "ticket_amount": ticket_amount,
        "convenience_fee": convenience_fee,
        "discount_amount": discount_amount,
        "net_amount": net_amount,
        "payment_gateway": payment_gateways,
        "payment_status": payment_status,
        "booking_channel": booking_channels,
        "is_cancelled": is_cancelled,
    }
)

# Write in chunks to keep local memory / Arrow transfer bounded at 2M rows.
CHUNK_SIZE = 250_000
first_chunk = True
for start in range(0, len(bookings_pdf), CHUNK_SIZE):
    chunk = bookings_pdf.iloc[start : start + CHUNK_SIZE]
    mode = "overwrite" if first_chunk else "append"
    spark.createDataFrame(chunk).write.mode(mode).parquet(f"{VOLUME_PATH}/bookings")
    first_chunk = False
    print(f"  bookings chunk written: rows {start}-{start + len(chunk)}")

print(f"bookings written: {len(bookings_pdf)}")

# COMMAND ----------

# MAGIC %md ## Sanity checks

# COMMAND ----------

print(f"Outage window: gateway={OUTAGE_GATEWAY}, cities={OUTAGE_CITIES}, date={OUTAGE_DATE}")

baseline_mask = bookings_pdf["booking_date"] != OUTAGE_DATE
outage_mask = (
    (bookings_pdf["booking_date"] == OUTAGE_DATE)
    & (bookings_pdf["payment_gateway"] == OUTAGE_GATEWAY)
    & (np.isin(booking_city, OUTAGE_CITIES))
)

baseline_failure_rate = (bookings_pdf.loc[baseline_mask, "payment_status"] == "FAILED").mean()
outage_failure_rate = (bookings_pdf.loc[outage_mask, "payment_status"] == "FAILED").mean()

print(f"Baseline payment failure rate (all other days): {baseline_failure_rate:.4f}")
print(f"Outage-day PayFast failure rate: {outage_failure_rate:.4f}")

for name, pdf in [
    ("theatres", theatres_pdf),
    ("movies", movies_pdf),
    ("customers", customers_pdf),
    ("shows", shows_pdf),
    ("bookings", bookings_pdf),
]:
    print(f"{name}: {len(pdf):,} rows")
