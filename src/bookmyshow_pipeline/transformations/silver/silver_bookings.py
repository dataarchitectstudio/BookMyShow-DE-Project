from pyspark import pipelines as dp

SILVER_SCHEMA = spark.conf.get("silver_schema")

VALID_PAYMENT_STATUSES_SQL = "payment_status IN ('success', 'failed', 'refunded')"


# expect_or_drop (not expect_or_fail) on booking_id/ticket_amount: the GENERATE step
# deliberately injects null booking_id / negative ticket_amount / orphan customer_id
# on every run (schemas.BAD_RECORD_FRACTION) so this layer has real violations to catch
# and report -- failing the whole pipeline update on every run would defeat that.
@dp.temporary_view(name="bookings_cleansed")
@dp.expect_or_drop("valid_booking_id", "booking_id IS NOT NULL")
@dp.expect_or_drop("valid_ticket_amount", "ticket_amount >= 0")
@dp.expect_or_drop("valid_payment_status", VALID_PAYMENT_STATUSES_SQL)
def bookings_cleansed():
    bookings = spark.readStream.table("bronze_bookings").select(
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
        "_ingested_at",
    )
    shows = spark.read.table(f"{SILVER_SCHEMA}.silver_shows").select("show_id")
    customers = spark.read.table(f"{SILVER_SCHEMA}.silver_customers").select("customer_id")
    # Referential integrity can't be an `@dp.expect*` predicate (no subqueries allowed
    # in expectations) -- an inner join against the cleaned dimensions drops orphan-FK
    # bad records (e.g. CUST-9999999) the GENERATE step injects.
    return bookings.join(shows, "show_id").join(customers, "customer_id")


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.silver_bookings",
    comment="Deduplicated, referentially-clean booking fact staged for the gold layer.",
)

# AUTO CDC (keyed on booking_id, sequenced by ingestion time) rather than
# dropDuplicates: dedups the exact-duplicate rows the GENERATE step injects and, per
# the implementation plan, is also the mechanism that will absorb later status updates
# (e.g. a cancellation/refund landing after the original booking).
dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.silver_bookings",
    source="bookings_cleansed",
    keys=["booking_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)
