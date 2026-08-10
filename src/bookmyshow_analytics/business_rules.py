"""Pure business logic shared by the synthetic data generator and the medallion pipeline.

Kept free of Spark/Databricks imports so it can run and be unit tested anywhere,
then wrapped in pandas UDFs or plain column expressions where it's used.
"""

from __future__ import annotations

LOYALTY_TIER_THRESHOLDS = (
    ("Platinum", 50_000.0),
    ("Gold", 20_000.0),
    ("Silver", 5_000.0),
)
DEFAULT_LOYALTY_TIER = "Bronze"

LOYALTY_DISCOUNT_RATES = {
    "Platinum": 0.15,
    "Gold": 0.10,
    "Silver": 0.05,
    "Bronze": 0.0,
}

MOVIE_PERFORMANCE_LABELS = {
    "high": "Blockbuster Hit",
    "mid": "Steady Performer",
    "low": "Underperformer",
}


def calculate_net_amount(ticket_amount: float, convenience_fee: float, discount_amount: float) -> float:
    """Net amount charged to the customer. Never negative."""
    net = ticket_amount + convenience_fee - discount_amount
    return round(max(net, 0.0), 2)


def calculate_discount_amount(ticket_amount: float, loyalty_tier: str) -> float:
    """Loyalty-tier discount applied on the gross ticket amount."""
    rate = LOYALTY_DISCOUNT_RATES.get(loyalty_tier, 0.0)
    return round(ticket_amount * rate, 2)


def classify_loyalty_tier(total_lifetime_spend: float) -> str:
    """Bucket a customer's lifetime spend into a loyalty tier."""
    for tier, threshold in LOYALTY_TIER_THRESHOLDS:
        if total_lifetime_spend >= threshold:
            return tier
    return DEFAULT_LOYALTY_TIER


def calculate_occupancy_rate(seats_booked: int, total_seats: int) -> float:
    """Fraction of a show's seats that were booked, clipped to [0, 1]."""
    if total_seats <= 0:
        return 0.0
    return round(min(max(seats_booked / total_seats, 0.0), 1.0), 4)


def is_within_outage_window(
    payment_gateway: str,
    city: str,
    outage_gateway: str,
    outage_cities: list[str],
    booking_date: str,
    outage_date: str,
) -> bool:
    """Whether a booking falls inside the simulated payment-gateway outage.

    The outage affects one gateway, in a handful of cities, on a single date --
    used both to generate the incident and later to explain it in the gold layer.
    """
    return booking_date == outage_date and payment_gateway == outage_gateway and city in outage_cities


def resolve_payment_status(is_outage_impacted: bool, failure_roll: float, refund_roll: float) -> str:
    """Pick SUCCESS/FAILED/REFUNDED given two independent uniform(0,1) draws.

    Baseline failure/refund rates are low; an outage-impacted booking has a much
    higher failure rate, which is what produces the traceable revenue dip.
    """
    failure_rate = 0.45 if is_outage_impacted else 0.03
    refund_rate = 0.02

    if failure_roll < failure_rate:
        return "FAILED"
    if refund_roll < refund_rate:
        return "REFUNDED"
    return "SUCCESS"


def pick_weighted_category(rand_value: float, weighted_options: list[tuple[str, float]]) -> str:
    """Map a uniform(0, 1) draw to a label using cumulative weights.

    Weights don't need to sum to 1 -- they're normalized internally. Used to
    generate skewed categorical columns (city, genre, gateway, ...) instead of
    uniform ones, both in the synthetic data generator and in tests.
    """
    total_weight = sum(weight for _, weight in weighted_options)
    if total_weight <= 0:
        raise ValueError("weighted_options must have a positive total weight")

    cumulative = 0.0
    for label, weight in weighted_options:
        cumulative += weight / total_weight
        if rand_value < cumulative:
            return label
    return weighted_options[-1][0]


def classify_movie_performance(revenue: float, high_threshold: float, low_threshold: float) -> str:
    """Label a movie's gross revenue relative to catalog-wide thresholds."""
    if revenue >= high_threshold:
        return MOVIE_PERFORMANCE_LABELS["high"]
    if revenue <= low_threshold:
        return MOVIE_PERFORMANCE_LABELS["low"]
    return MOVIE_PERFORMANCE_LABELS["mid"]
