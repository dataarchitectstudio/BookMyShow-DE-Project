"""Unit tests for pure business logic -- no Spark session required.

Run with: uv run pytest tests/test_business_rules.py -v
"""

import pytest

from bookmyshow_analytics.business_rules import (
    calculate_discount_amount,
    calculate_net_amount,
    calculate_occupancy_rate,
    classify_loyalty_tier,
    classify_movie_performance,
    is_within_outage_window,
    pick_weighted_category,
    resolve_payment_status,
)


class TestCalculateNetAmount:
    def test_typical_booking(self):
        assert calculate_net_amount(ticket_amount=500.0, convenience_fee=30.0, discount_amount=50.0) == 480.0

    def test_never_negative(self):
        assert calculate_net_amount(ticket_amount=100.0, convenience_fee=0.0, discount_amount=500.0) == 0.0

    def test_rounds_to_cents(self):
        assert calculate_net_amount(ticket_amount=99.999, convenience_fee=10.001, discount_amount=0.0) == 110.0


class TestCalculateDiscountAmount:
    @pytest.mark.parametrize(
        "tier,expected_rate",
        [("Platinum", 0.15), ("Gold", 0.10), ("Silver", 0.05), ("Bronze", 0.0)],
    )
    def test_applies_tier_rate(self, tier, expected_rate):
        assert calculate_discount_amount(1000.0, tier) == round(1000.0 * expected_rate, 2)

    def test_unknown_tier_defaults_to_zero_discount(self):
        assert calculate_discount_amount(1000.0, "Unknown") == 0.0


class TestClassifyLoyaltyTier:
    def test_platinum_boundary(self):
        assert classify_loyalty_tier(50_000.0) == "Platinum"

    def test_gold_range(self):
        assert classify_loyalty_tier(25_000.0) == "Gold"

    def test_silver_range(self):
        assert classify_loyalty_tier(6_000.0) == "Silver"

    def test_bronze_default(self):
        assert classify_loyalty_tier(100.0) == "Bronze"

    def test_zero_spend_is_bronze(self):
        assert classify_loyalty_tier(0.0) == "Bronze"


class TestCalculateOccupancyRate:
    def test_typical_occupancy(self):
        assert calculate_occupancy_rate(seats_booked=80, total_seats=100) == 0.8

    def test_full_house(self):
        assert calculate_occupancy_rate(seats_booked=100, total_seats=100) == 1.0

    def test_clips_overbooking_to_one(self):
        assert calculate_occupancy_rate(seats_booked=120, total_seats=100) == 1.0

    def test_zero_total_seats_is_zero_not_a_crash(self):
        assert calculate_occupancy_rate(seats_booked=0, total_seats=0) == 0.0

    def test_no_bookings(self):
        assert calculate_occupancy_rate(seats_booked=0, total_seats=200) == 0.0


class TestIsWithinOutageWindow:
    def test_matches_gateway_city_and_date(self):
        assert is_within_outage_window(
            payment_gateway="PayFast",
            city="Mumbai",
            outage_gateway="PayFast",
            outage_cities=["Mumbai", "Delhi"],
            booking_date="2026-03-15",
            outage_date="2026-03-15",
        )

    def test_different_gateway_is_unaffected(self):
        assert not is_within_outage_window(
            payment_gateway="Razorpay",
            city="Mumbai",
            outage_gateway="PayFast",
            outage_cities=["Mumbai", "Delhi"],
            booking_date="2026-03-15",
            outage_date="2026-03-15",
        )

    def test_different_city_is_unaffected(self):
        assert not is_within_outage_window(
            payment_gateway="PayFast",
            city="Chennai",
            outage_gateway="PayFast",
            outage_cities=["Mumbai", "Delhi"],
            booking_date="2026-03-15",
            outage_date="2026-03-15",
        )

    def test_different_date_is_unaffected(self):
        assert not is_within_outage_window(
            payment_gateway="PayFast",
            city="Mumbai",
            outage_gateway="PayFast",
            outage_cities=["Mumbai", "Delhi"],
            booking_date="2026-03-16",
            outage_date="2026-03-15",
        )


class TestResolvePaymentStatus:
    def test_outage_impacted_high_failure_roll_fails(self):
        assert resolve_payment_status(is_outage_impacted=True, failure_roll=0.1, refund_roll=0.9) == "FAILED"

    def test_outage_impacted_low_failure_roll_succeeds(self):
        assert resolve_payment_status(is_outage_impacted=True, failure_roll=0.9, refund_roll=0.9) == "SUCCESS"

    def test_baseline_booking_usually_succeeds(self):
        assert resolve_payment_status(is_outage_impacted=False, failure_roll=0.5, refund_roll=0.5) == "SUCCESS"

    def test_baseline_low_roll_still_fails_at_low_rate(self):
        assert resolve_payment_status(is_outage_impacted=False, failure_roll=0.01, refund_roll=0.9) == "FAILED"

    def test_refund_takes_over_when_not_failed(self):
        assert resolve_payment_status(is_outage_impacted=False, failure_roll=0.9, refund_roll=0.01) == "REFUNDED"


class TestPickWeightedCategory:
    OPTIONS = (("Mumbai", 0.5), ("Delhi", 0.3), ("Pune", 0.2))

    def test_low_draw_picks_first_bucket(self):
        assert pick_weighted_category(0.0, self.OPTIONS) == "Mumbai"

    def test_draw_just_under_first_boundary(self):
        assert pick_weighted_category(0.49, self.OPTIONS) == "Mumbai"

    def test_draw_in_second_bucket(self):
        assert pick_weighted_category(0.6, self.OPTIONS) == "Delhi"

    def test_draw_in_last_bucket(self):
        assert pick_weighted_category(0.95, self.OPTIONS) == "Pune"

    def test_draw_at_upper_edge_falls_back_to_last(self):
        assert pick_weighted_category(0.999999, self.OPTIONS) == "Pune"

    def test_unnormalized_weights_still_work(self):
        options = [("A", 5), ("B", 3), ("C", 2)]
        assert pick_weighted_category(0.0, options) == "A"
        assert pick_weighted_category(0.99, options) == "C"

    def test_zero_total_weight_raises(self):
        with pytest.raises(ValueError):
            pick_weighted_category(0.5, [("A", 0.0), ("B", 0.0)])


class TestClassifyMoviePerformance:
    def test_high_revenue_is_blockbuster(self):
        assert classify_movie_performance(revenue=1_000_000, high_threshold=500_000, low_threshold=50_000) == (
            "Blockbuster Hit"
        )

    def test_low_revenue_is_underperformer(self):
        assert classify_movie_performance(revenue=10_000, high_threshold=500_000, low_threshold=50_000) == (
            "Underperformer"
        )

    def test_mid_revenue_is_steady(self):
        assert classify_movie_performance(revenue=200_000, high_threshold=500_000, low_threshold=50_000) == (
            "Steady Performer"
        )
