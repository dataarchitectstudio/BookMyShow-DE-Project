-- Governed payment-gateway health KPI (business problem 2: spot a gateway/city/date
-- failure spike before it's discovered via customer complaints). Deployed as DDL via
-- the CLI -- see mv_booking_revenue.sql header for the substitution/runbook note.

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.mv_payment_health
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >
  Payment-gateway health: failure rate sliced by gateway, city, and date -- the
  direct mechanism for catching a spike like the CCAvenue Mumbai+Pune 2026-07-30
  incident immediately, instead of via customer complaints after the fact.
source: ${catalog}.${gold_schema}.gold_fact_bookings

joins:
  - name: theatre
    source: ${catalog}.${gold_schema}.gold_dim_theatre
    on: source.theatre_id = theatre.theatre_id

dimensions:
  - name: Payment Gateway
    expr: source.payment_gateway
  - name: City
    expr: theatre.city
  - name: Booking Date
    expr: source.booking_date

measures:
  - name: Total Bookings
    expr: COUNT(1)
  - name: Failed Bookings
    expr: COUNT(1) FILTER (WHERE source.payment_status = 'failed')
  - name: Payment Failure Rate
    expr: COUNT(1) FILTER (WHERE source.payment_status = 'failed') * 1.0 / COUNT(1)
    comment: Share of bookings with a failed payment status, for this gateway/city/date slice.
$$;
