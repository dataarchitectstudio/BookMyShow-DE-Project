"""Unit tests for the pure (non-Spark) pieces of the synthetic data generator.

Spark/Faker-backed generation is exercised against a live Databricks environment
(see resources/bookmyshow_daily_refresh.job.yml) rather than here, since it needs a real cluster.
"""

from bookmyshow import schemas
from bookmyshow.generator import (
    _cumulative_thresholds,
    movie_popularity_weight,
)


def test_movie_popularity_weight_is_deterministic():
    assert movie_popularity_weight("MOV-00001") == movie_popularity_weight("MOV-00001")


def test_movie_popularity_weight_in_unit_range():
    for movie_id in ["MOV-00001", "MOV-00042", "MOV-00600"]:
        weight = movie_popularity_weight(movie_id)
        assert 0.0 <= weight <= 1.0


def test_movie_popularity_weight_is_skewed_not_uniform():
    # Power-law transform (x**4) should push the average well below the ~0.5
    # midpoint a uniform distribution would give.
    weights = [movie_popularity_weight(f"MOV-{i:05d}") for i in range(1, 601)]
    assert sum(weights) / len(weights) < 0.3


def test_cumulative_thresholds_sum_to_one():
    thresholds = _cumulative_thresholds([92, 5, 3])
    assert thresholds[-1] == 1.0
    assert thresholds == sorted(thresholds)


def test_weight_lists_align_with_category_lists():
    pairs = [
        (schemas.CITIES, schemas.CITY_WEIGHTS),
        (schemas.SCREEN_TYPES, schemas.SCREEN_TYPE_WEIGHTS),
        (schemas.GENRES, schemas.GENRE_WEIGHTS),
        (schemas.PAYMENT_GATEWAYS, schemas.PAYMENT_GATEWAY_WEIGHTS),
        (schemas.BOOKING_CHANNELS, schemas.BOOKING_CHANNEL_WEIGHTS),
        (schemas.PAYMENT_STATUSES, schemas.BASELINE_PAYMENT_STATUS_WEIGHTS),
    ]
    for values, weights in pairs:
        assert len(values) == len(weights)
        assert all(w > 0 for w in weights)


def test_incident_gateway_and_cities_are_real_categories():
    assert schemas.INCIDENT_GATEWAY in schemas.PAYMENT_GATEWAYS
    assert set(schemas.INCIDENT_CITIES).issubset({c for c, _ in schemas.CITIES})


def test_screen_type_ranges_defined_for_every_screen_type():
    for screen_type in schemas.SCREEN_TYPES:
        cap_lo, cap_hi = schemas.SCREEN_TYPE_CAPACITY_RANGE[screen_type]
        price_lo, price_hi = schemas.SCREEN_TYPE_BASE_PRICE_RANGE[screen_type]
        assert 0 < cap_lo < cap_hi
        assert 0 < price_lo < price_hi
