"""Reference data, distributions, and row-count constants for BookMyShow synthetic
data generation. Kept dependency-free (no pyspark/faker imports) so it can be unit
tested without a Spark session.
"""

from datetime import date

# (city, state) pairs — Indian metros, weighted toward larger markets via CITY_WEIGHTS.
CITIES: list[tuple[str, str]] = [
    ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Pune", "Maharashtra"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"),
    ("Chandigarh", "Chandigarh"),
    ("Kochi", "Kerala"),
    ("Surat", "Gujarat"),
    ("Nagpur", "Maharashtra"),
    ("Indore", "Madhya Pradesh"),
]
CITY_WEIGHTS: list[float] = [18, 16, 14, 10, 10, 7, 6, 4, 3, 3, 3, 2, 2, 1, 1]

SCREEN_TYPES: list[str] = ["STANDARD", "PREMIUM", "IMAX", "RECLINER"]
SCREEN_TYPE_WEIGHTS: list[float] = [55, 25, 10, 10]
SCREEN_TYPE_CAPACITY_RANGE: dict[str, tuple[int, int]] = {
    "STANDARD": (150, 260),
    "PREMIUM": (100, 180),
    "IMAX": (250, 400),
    "RECLINER": (60, 120),
}
SCREEN_TYPE_BASE_PRICE_RANGE: dict[str, tuple[int, int]] = {
    "STANDARD": (120, 220),
    "PREMIUM": (220, 350),
    "IMAX": (350, 600),
    "RECLINER": (300, 550),
}

GENRES: list[str] = [
    "Action",
    "Drama",
    "Comedy",
    "Thriller",
    "Romance",
    "Horror",
    "Sci-Fi",
    "Documentary",
]
GENRE_WEIGHTS: list[float] = [25, 20, 15, 15, 10, 5, 5, 5]

PAYMENT_GATEWAYS: list[str] = ["Razorpay", "Paytm", "PhonePe", "CCAvenue"]
PAYMENT_GATEWAY_WEIGHTS: list[float] = [45, 25, 20, 10]

BOOKING_CHANNELS: list[str] = ["mobile_app", "web", "box_office"]
BOOKING_CHANNEL_WEIGHTS: list[float] = [55, 35, 10]

# Matches the business problem statement's payment_status enum exactly.
PAYMENT_STATUSES: list[str] = ["success", "failed", "refunded"]
BASELINE_PAYMENT_STATUS_WEIGHTS: list[float] = [92, 5, 3]

# --- Story: a gateway outage silently fails payments for a full day in two major
# cities, mirroring the "recent incident" called out in the business problem
# statement. This is what the Trends & Reach dashboard page (built in a later step)
# must make immediately obvious via mv_payment_health sliced by gateway/city/date.
INCIDENT_GATEWAY: str = "CCAvenue"
INCIDENT_CITIES: list[str] = ["Mumbai", "Pune"]
INCIDENT_DATE: date = date(2026, 7, 30)
INCIDENT_FAILURE_RATE: float = 0.88

# Row counts for a full historical rebuild — matches the agreed architecture diagram
# (Business-Problem & Architecture/bookmyshow-architecture-animated.svg).
DEFAULT_N_THEATRES = 200
DEFAULT_N_MOVIES = 600
DEFAULT_N_CUSTOMERS = 150_000
DEFAULT_N_SHOWS = 120_000
DEFAULT_N_BOOKINGS = 2_000_000
DEFAULT_HISTORY_DAYS = 180

# Daily incremental volumes (simulating continuous operational growth).
DEFAULT_DAILY_NEW_CUSTOMERS = 500
DEFAULT_DAILY_NEW_SHOWS = 800
DEFAULT_DAILY_BOOKINGS = 15_000

DEFAULT_SEED = 42

# Fraction of bookings deliberately corrupted (bad FK, null, duplicate, negative
# amount) so the silver-layer data-quality expectations built in a later step have
# real violations to catch and report.
BAD_RECORD_FRACTION = 0.006
