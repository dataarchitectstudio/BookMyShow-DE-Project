-- Governed booking/revenue KPIs (business problems 1 & 3). Not a native DABs bundle
-- resource (no `metric_views:` resource type as of CLI v1.9.0) -- deployed as DDL via
-- the CLI. Substitute ${catalog}/${gold_schema} for the target environment before
-- running; see the implementation plan's Governance & CI/CD section for the runbook.

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.mv_booking_revenue
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: >
  Booking revenue KPIs: total/successful/failed bookings, average ticket price,
  cancellation rate, unique customers -- sliced by date, city, movie, theatre,
  booking channel, and payment gateway.
source: ${catalog}.${gold_schema}.gold_fact_bookings

joins:
  - name: theatre
    source: ${catalog}.${gold_schema}.gold_dim_theatre
    on: source.theatre_id = theatre.theatre_id
  - name: movie
    source: ${catalog}.${gold_schema}.gold_dim_movie
    on: source.movie_id = movie.movie_id

dimensions:
  - name: Booking Date
    expr: source.booking_date
    comment: Calendar date the booking was made.
  - name: City
    expr: theatre.city
  - name: Movie Title
    expr: movie.title
  - name: Genre
    expr: movie.genre
  - name: Theatre Name
    expr: theatre.name
  - name: Booking Channel
    expr: source.booking_channel
  - name: Payment Gateway
    expr: source.payment_gateway

measures:
  - name: Total Revenue
    expr: SUM(source.net_revenue) FILTER (WHERE source.payment_status = 'success' AND NOT source.is_cancelled)
    comment: Realized net revenue -- successful, non-cancelled bookings only.
  - name: Total Bookings
    expr: COUNT(1)
  - name: Successful Bookings
    expr: COUNT(1) FILTER (WHERE source.payment_status = 'success')
  - name: Failed Bookings
    expr: COUNT(1) FILTER (WHERE source.payment_status = 'failed')
  - name: Average Ticket Price
    expr: SUM(source.ticket_amount) FILTER (WHERE source.payment_status = 'success' AND NOT source.is_cancelled) / COUNT(1) FILTER (WHERE source.payment_status = 'success' AND NOT source.is_cancelled)
    comment: Average per-booking ticket price, realized bookings only.
  - name: Cancellation Rate
    expr: COUNT(1) FILTER (WHERE source.is_cancelled) * 1.0 / COUNT(1)
  - name: Unique Customers
    expr: COUNT(DISTINCT source.customer_id)
$$;
