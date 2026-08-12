from pyspark import pipelines as dp

SILVER_SCHEMA = spark.conf.get("silver_schema")

# Doubled backslashes: expectation predicates are SQL string literals, and Spark SQL
# unescapes a single backslash before the regex engine ever sees it (silently turning
# \s into a literal "s") -- \\s/\\. survive the SQL layer and reach the regex intact.
EMAIL_REGEX = r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"
PHONE_REGEX = r"^[0-9]{10}$"


@dp.temporary_view(name="customers_cleansed")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email_format", f"email RLIKE '{EMAIL_REGEX}'")
@dp.expect("valid_phone_format", f"phone RLIKE '{PHONE_REGEX}'")
def customers_cleansed():
    return spark.readStream.table("bronze_customers").select(
        "customer_id", "name", "email", "phone", "city", "signup_date", "_ingested_at"
    )


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.silver_customers",
    comment="Deduplicated customer dimension: latest record per customer_id via AUTO CDC.",
)

# AUTO CDC (rather than dropDuplicates) so the mechanism also absorbs future profile
# updates, not just today's exact-duplicate rows the GENERATE step injects.
dp.create_auto_cdc_flow(
    target=f"{SILVER_SCHEMA}.silver_customers",
    source="customers_cleansed",
    keys=["customer_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)
