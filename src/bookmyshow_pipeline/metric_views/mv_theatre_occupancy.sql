-- Governed theatre/show occupancy KPI. Deployed as DDL via the CLI -- see
-- mv_booking_revenue.sql header for the substitution/runbook note.

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.mv_theatre_occupancy
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >
  Theatre/show occupancy: seats sold vs. capacity, sliced by theatre, city,
  screen type, and show date.
source: ${catalog}.${gold_schema}.gold_fact_show_occupancy

joins:
  - name: theatre
    source: ${catalog}.${gold_schema}.gold_dim_theatre
    on: source.theatre_id = theatre.theatre_id

dimensions:
  - name: Theatre Name
    expr: theatre.name
  - name: City
    expr: source.city
  - name: Screen Type
    expr: source.screen_type
  - name: Show Date
    expr: source.show_date

measures:
  - name: Show Count
    expr: COUNT(1)
  - name: Total Seat Capacity
    expr: SUM(source.seat_capacity)
  - name: Total Seats Sold
    expr: SUM(source.seats_sold)
  - name: Occupancy Rate
    expr: SUM(source.seats_sold) * 1.0 / SUM(source.seat_capacity)
    comment: Sum-of-seats-sold over sum-of-capacity (not an average of per-show rates) so it re-aggregates safely across any grouping.
$$;
