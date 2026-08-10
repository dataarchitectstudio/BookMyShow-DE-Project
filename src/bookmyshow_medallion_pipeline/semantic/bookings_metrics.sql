-- Unity Catalog Metric View: governed semantic layer over gold.fact_bookings.
-- Powers the executive dashboard, Genie, and ad-hoc SQL with a single set of
-- measure definitions (no re-deriving revenue/failure-rate math per query).
--
-- __CATALOG__ is a literal placeholder substituted at deploy time (this isn't a
-- DAB-templated file -- see README.md "Deploying the semantic model").
CREATE OR REPLACE VIEW __CATALOG__.gold.bookings_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Governed BookMyShow booking KPIs: revenue, payment health, and customer engagement."
source: __CATALOG__.gold.fact_bookings
joins:
  - name: customer
    source: __CATALOG__.gold.dim_customers
    on: source.customer_id = customer.customer_id
dimensions:
  - name: Booking Date
    expr: booking_date
    comment: "Calendar date the booking transaction occurred"
  - name: City
    expr: city
  - name: State
    expr: state
  - name: Screen Type
    expr: screen_type
  - name: Genre
    expr: genre
  - name: Is Blockbuster
    expr: is_blockbuster
  - name: Payment Gateway
    expr: payment_gateway
  - name: Payment Status
    expr: payment_status
  - name: Booking Channel
    expr: booking_channel
  - name: Loyalty Tier
    expr: customer.loyalty_tier
measures:
  - name: Total Bookings
    expr: COUNT(1)
  - name: Successful Bookings
    expr: SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END)
  - name: Failed Bookings
    expr: SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END)
  - name: Total Revenue
    expr: SUM(CASE WHEN payment_status = 'SUCCESS' THEN net_amount ELSE 0 END)
    comment: "Net revenue from successful bookings only"
  - name: Avg Ticket Price
    expr: AVG(ticket_amount)
  - name: Payment Failure Rate
    expr: SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(1)
  - name: Cancellation Rate
    expr: "SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN payment_status = 'SUCCESS' THEN 1 ELSE 0 END), 0)"
  - name: Unique Customers
    expr: COUNT(DISTINCT customer_id)
  - name: Total Tickets Sold
    expr: SUM(CASE WHEN payment_status = 'SUCCESS' THEN seats_booked ELSE 0 END)
$$
