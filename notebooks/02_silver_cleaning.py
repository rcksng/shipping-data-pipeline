# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# 02_SILVER_CLEANING
# Purpose : Read bronze Delta tables → apply type casting,
#           null handling, bad data rules, deduplication →
#           write clean Delta tables to silver layer
#
# What this notebook does NOT do:
#   - SCD2 logic (that's 03_silver_scd2)
#   - Business aggregations (that's gold layer)
#   - Joining tables together (that's gold layer)
# ============================================================

MOUNT        = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"
BRONZE_PATH  = f"{MOUNT}/bronze"
SILVER_PATH  = f"{MOUNT}/silver"

print(f"Bronze : {BRONZE_PATH}")
print(f"Silver : {SILVER_PATH}")

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

print("✓ Imports ready")

# COMMAND ----------

# DBTITLE 1,Clean users
# ── USERS ────────────────────────────────────────────────────
# No SCD2 needed — users don't have a changes file
# Clean job: cast types, standardise email, fix boolean

df_users_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/users")

df_users_clean = (df_users_raw

    # Cast booleans — source sends "True"/"False" strings
    # If the value is anything other than "True", treat as False
    # This is an explicit decision — document it
    .withColumn("is_active",
        F.when(F.upper(F.col("is_active")) == "TRUE", True)
         .otherwise(False)
         .cast("boolean"))

    # Standardise email — lowercase, trim whitespace
    # Reason: email matching downstream (e.g. joining to Salesforce user)
    # will silently fail if case differs
    .withColumn("email",
        F.lower(F.trim(F.col("email"))))

    # Cast dates — bronze stored as strings
    .withColumn("created_date",
        F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    .withColumn("last_login_date",
        F.to_date(F.col("last_login_date"), "yyyy-MM-dd"))

    # Standardise text fields — trim whitespace
    # A single trailing space in department = broken GROUP BY in gold
    .withColumn("department",  F.trim(F.col("department")))
    .withColumn("role",        F.trim(F.col("role")))
    .withColumn("region",      F.trim(F.col("region")))
    .withColumn("first_name",  F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name",   F.initcap(F.trim(F.col("last_name"))))

    # Add silver metadata
    .withColumn("_silver_cleaned_at", F.current_timestamp())

    # Drop bronze metadata columns — silver has its own
    # We keep _source_object for traceability but drop the rest
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

# Write to silver
(df_users_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/users"))

print(f"✓ users: {df_users_clean.count():,} rows written to silver")

# COMMAND ----------

# ── PORTS ─────────────────────────────────────────────────────
# Has a _changes file — so bronze has both full + changes rows
# At silver cleaning stage we just clean ALL rows together
# SCD2 logic (which version is current) happens in 03_silver_scd2

df_ports_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/ports")

print(f"Ports bronze total (full + changes): {df_ports_raw.count():,}")
print(f"Distinct source files: {df_ports_raw.select('_source_path').distinct().count()}")

df_ports_clean = (df_ports_raw

    .withColumn("is_active",
        F.when(F.upper(F.col("is_active")) == "TRUE", True)
         .otherwise(False)
         .cast("boolean"))

    .withColumn("last_updated",
        F.to_date(F.col("last_updated"), "yyyy-MM-dd"))

    # Standardise categoricals — these become dimension attributes
    # Consistent casing prevents duplicate category values in gold
    .withColumn("port_type",
        F.initcap(F.trim(F.col("port_type"))))
    .withColumn("region",
        F.trim(F.col("region")))
    .withColumn("country_code",
        F.upper(F.trim(F.col("country_code"))))
    .withColumn("port_code",
        F.upper(F.trim(F.col("port_code"))))

    # Validate capacity — negative capacity is physically impossible
    # Decision: set invalid capacities to NULL rather than drop the row
    # Reason: the port still exists and is useful — only this one field is bad
    .withColumn("annual_capacity_teu",
        F.when(F.col("annual_capacity_teu") <= 0, None)
         .otherwise(F.col("annual_capacity_teu")))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_ports_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/ports_all_versions"))

# Note: folder named ports_all_versions deliberately
# After SCD2 in notebook 03, the final table will be silver/dim_ports

print(f"✓ ports: {df_ports_clean.count():,} rows written to silver/ports_all_versions")

# COMMAND ----------

df_ports_raw.display()

# COMMAND ----------

df_ports_clean.display()

# COMMAND ----------

# DBTITLE 1,Clean vessels
# ── VESSELS ───────────────────────────────────────────────────
# Same pattern as ports — has _changes file
# Clean all rows together; SCD2 in notebook 03

df_vessels_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/vessels")

print(f"Vessels bronze total (full + changes): {df_vessels_raw.count():,}")

df_vessels_clean = (df_vessels_raw

    .withColumn("is_active",
        F.when(F.upper(F.col("is_active")) == "TRUE", True)
         .otherwise(False)
         .cast("boolean"))

    .withColumn("last_updated",
        F.to_date(F.col("last_updated"), "yyyy-MM-dd"))

    # Validate year_built — sanity range check
    # No commercial vessel was built before 1950 or after current year
    # Decision: NULL out impossible values, don't drop the vessel row
    .withColumn("year_built",
        F.when(
            (F.col("year_built") < 1950) |
            (F.col("year_built") > F.year(F.current_date())),
            None)
         .otherwise(F.col("year_built")))

    # Validate capacity — must be positive
    .withColumn("capacity_teu",
        F.when(F.col("capacity_teu") <= 0, None)
         .otherwise(F.col("capacity_teu")))

    # Standardise
    .withColumn("flag_country",
        F.upper(F.trim(F.col("flag_country"))))
    .withColumn("vessel_type",
        F.initcap(F.trim(F.col("vessel_type"))))
    .withColumn("operator",
        F.trim(F.col("operator")))
    .withColumn("vessel_name",
        F.trim(F.col("vessel_name")))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_vessels_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/vessels_all_versions"))

print(f"✓ vessels: {df_vessels_clean.count():,} rows written to silver/vessels_all_versions")

# COMMAND ----------

df_vessels_raw.display()

# COMMAND ----------

df_vessels_clean.display()

# COMMAND ----------

# DBTITLE 1,Clean accounts
# ── ACCOUNTS ──────────────────────────────────────────────────
# Has _changes file (tier upgrades + region reassignments)
# This is the most important dimension — drives most gold aggregations

df_accounts_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/accounts")

print(f"Accounts bronze total (full + changes): {df_accounts_raw.count():,}")

df_accounts_clean = (df_accounts_raw

    .withColumn("is_active",
        F.when(F.upper(F.col("is_active")) == "TRUE", True)
         .otherwise(False)
         .cast("boolean"))

    .withColumn("created_date",
        F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    .withColumn("last_modified_date",
        F.to_date(F.col("last_modified_date"), "yyyy-MM-dd"))

    # Validate revenue — must be positive
    # Decision: NULL rather than drop — account is still valid
    .withColumn("annual_revenue_usd",
        F.when(F.col("annual_revenue_usd") <= 0, None)
         .otherwise(F.col("annual_revenue_usd")))

    # Validate employee count
    .withColumn("employee_count",
        F.when(F.col("employee_count") <= 0, None)
         .otherwise(F.col("employee_count")))

    # Standardise tier — this becomes a key filter in gold
    # "gold", "Gold", "GOLD" must all become "Gold"
    .withColumn("account_tier",
        F.initcap(F.trim(F.col("account_tier"))))
    .withColumn("region",
        F.trim(F.col("region")))
    .withColumn("country_code",
        F.upper(F.trim(F.col("country_code"))))
    .withColumn("account_name",
        F.trim(F.col("account_name")))
    .withColumn("industry",
        F.initcap(F.trim(F.col("industry"))))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_accounts_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("region")              # accounts queried by region heavily
    .save(f"{SILVER_PATH}/accounts_all_versions"))

print(f"✓ accounts: {df_accounts_clean.count():,} rows written to silver/accounts_all_versions")

# COMMAND ----------

df_accounts_raw.display()

# COMMAND ----------

df_accounts_clean.display()

# COMMAND ----------

# DBTITLE 1,Clean contacts and contracts
# ── CONTACTS ──────────────────────────────────────────────────
df_contacts_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/contacts")

df_contacts_clean = (df_contacts_raw
    .withColumn("is_active",
        F.when(F.upper(F.col("is_active")) == "TRUE", True)
         .otherwise(False)
         .cast("boolean"))
    .withColumn("created_date",
        F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    .withColumn("email",
        F.lower(F.trim(F.col("email"))))
    .withColumn("first_name",
        F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name",
        F.initcap(F.trim(F.col("last_name"))))
    .withColumn("job_title",
        F.initcap(F.trim(F.col("job_title"))))
    .withColumn("contact_type",
        F.initcap(F.trim(F.col("contact_type"))))
    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_contacts_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/contacts"))

print(f"✓ contacts: {df_contacts_clean.count():,} rows written to silver")

# ── CONTRACTS ─────────────────────────────────────────────────
df_contracts_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/contracts")

df_contracts_clean = (df_contracts_raw
    .withColumn("start_date",
        F.to_date(F.col("start_date"), "yyyy-MM-dd"))
    .withColumn("end_date",
        F.to_date(F.col("end_date"), "yyyy-MM-dd"))
    .withColumn("created_date",
        F.to_date(F.col("created_date"), "yyyy-MM-dd"))

    # Validate contract value and rates
    .withColumn("contract_value_usd",
        F.when(F.col("contract_value_usd") <= 0, None)
         .otherwise(F.col("contract_value_usd")))
    .withColumn("rate_per_teu_usd",
        F.when(F.col("rate_per_teu_usd") <= 0, None)
         .otherwise(F.col("rate_per_teu_usd")))
    .withColumn("volume_commitment_teu",
        F.when(F.col("volume_commitment_teu") <= 0, None)
         .otherwise(F.col("volume_commitment_teu")))

    # Validate date logic — end must be after start
    # Decision: NULL end_date if invalid rather than drop contract
    .withColumn("end_date",
        F.when(F.col("end_date") <= F.col("start_date"), None)
         .otherwise(F.col("end_date")))

    .withColumn("status",
        F.initcap(F.trim(F.col("status"))))
    .withColumn("contract_type",
        F.initcap(F.trim(F.col("contract_type"))))
    .withColumn("rate_tier",
        F.initcap(F.trim(F.col("rate_tier"))))
    .withColumn("cargo_type",
        F.initcap(F.trim(F.col("cargo_type"))))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_contracts_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("status")
    .save(f"{SILVER_PATH}/contracts"))

print(f"✓ contracts: {df_contracts_clean.count():,} rows written to silver")

# COMMAND ----------

# DBTITLE 1,profile before cleaning
# ── BOOKINGS ──────────────────────────────────────────────────
# This is the core fact source — deserves the most care
# Three real data quality problems to handle:
#   1. Null cargo_type (~3% of rows)
#   2. Negative teu_count (~2% of rows)
#   3. Missing actual_departure / actual_arrival (expected — voyage not complete)

df_bookings_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/bookings")

total_raw = df_bookings_raw.count()
print(f"Raw bookings from bronze: {total_raw:,}")

# Profile the issues BEFORE cleaning
# This is what you'd do in a real project to understand the data
null_cargo    = df_bookings_raw.filter(F.col("cargo_type").isNull()).count()
negative_teu  = df_bookings_raw.filter(F.col("teu_count") < 0).count()
null_actual_dep = df_bookings_raw.filter(F.col("actual_departure").isNull()).count()

print(f"\nData quality profile (pre-cleaning):")
print(f"  Null cargo_type      : {null_cargo:,}  ({100*null_cargo/total_raw:.1f}%)")
print(f"  Negative teu_count   : {negative_teu:,}  ({100*negative_teu/total_raw:.1f}%)")
print(f"  Null actual_departure: {null_actual_dep:,}  ({100*null_actual_dep/total_raw:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Clean bookings
df_bookings_clean = (df_bookings_raw

    # ── Date casts ────────────────────────────────────────────
    .withColumn("booking_date",
        F.to_timestamp(F.col("booking_date"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("etd",
        F.to_date(F.col("etd"), "yyyy-MM-dd"))
    .withColumn("eta",
        F.to_date(F.col("eta"), "yyyy-MM-dd"))
    .withColumn("actual_departure",
        F.to_date(F.col("actual_departure"), "yyyy-MM-dd"))
    .withColumn("actual_arrival",
        F.to_date(F.col("actual_arrival"), "yyyy-MM-dd"))
    .withColumn("created_date",
        F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    .withColumn("last_modified_date",
        F.to_date(F.col("last_modified_date"), "yyyy-MM-dd"))

    # ── Handle null cargo_type ────────────────────────────────
    # Decision: replace with "Unknown" NOT drop the row
    # Reason: the booking still happened and has revenue
    # Dropping it would undercount total revenue in gold
    # "Unknown" is visible in reports — prompts investigation at source
    # The WRONG decision would be to impute a category (guess) — that
    # would make the data look clean while being factually wrong
    .withColumn("cargo_type",
        F.coalesce(
            F.initcap(F.trim(F.col("cargo_type"))),
            F.lit("Unknown")))

    # ── Handle negative teu_count ─────────────────────────────
    # Decision: take absolute value (abs) NOT drop or NULL
    # Reason: negative TEU is almost certainly a data entry error
    # (minus sign accidentally added). The magnitude is likely correct.
    # We flag it with a new column so analysts can filter if needed
    .withColumn("teu_count_is_corrected",
        F.when(F.col("teu_count") < 0, True).otherwise(False))
    .withColumn("teu_count",
        F.abs(F.col("teu_count")))

    # ── Validate ETD/ETA logic ────────────────────────────────
    # ETA must be after ETD — flag invalid records
    .withColumn("has_invalid_dates",
        F.when(
            F.col("eta").isNotNull() &
            F.col("etd").isNotNull() &
            (F.col("eta") <= F.col("etd")),
            True).otherwise(False))

    # ── Validate revenue ──────────────────────────────────────
    # Negative revenue is not correctable — NULL it
    # A booking with negative revenue needs source investigation
    .withColumn("total_revenue_usd",
        F.when(F.col("total_revenue_usd") < 0, None)
         .otherwise(F.col("total_revenue_usd")))

    # ── Derive booking_month for partitioning ─────────────────
    # Bookings are almost always filtered by date in gold queries
    # Partitioning by booking_month makes those queries cheap
    .withColumn("booking_year",
        F.year(F.col("booking_date")))
    .withColumn("booking_month",
        F.month(F.col("booking_date")))

    # ── Standardise categoricals ──────────────────────────────
    .withColumn("booking_status",
        F.initcap(F.trim(F.col("booking_status"))))
    .withColumn("container_size",
        F.upper(F.trim(F.col("container_size"))))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

# Write partitioned by year + month — most gold queries filter on booking date
(df_bookings_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("booking_year", "booking_month")
    .save(f"{SILVER_PATH}/bookings"))

# Post-cleaning profile — confirms decisions were applied
total_clean       = df_bookings_clean.count()
null_cargo_after  = df_bookings_clean.filter(F.col("cargo_type") == "Unknown").count()
corrected_teu     = df_bookings_clean.filter(F.col("teu_count_is_corrected") == True).count()
invalid_dates     = df_bookings_clean.filter(F.col("has_invalid_dates") == True).count()

print(f"\nData quality profile (post-cleaning):")
print(f"  Total rows           : {total_clean:,}")
print(f"  cargo_type='Unknown' : {null_cargo_after:,}  (was null, now visible)")
print(f"  teu_count corrected  : {corrected_teu:,}  (abs value applied, flagged)")
print(f"  Invalid ETD/ETA      : {invalid_dates:,}  (flagged for investigation)")
print(f"\n✓ bookings written to silver — partitioned by booking_year, booking_month")

# COMMAND ----------

# DBTITLE 1,Clean cargo events and cases
# ── CARGO EVENTS ──────────────────────────────────────────────
df_events_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/cargo_events")

df_events_clean = (df_events_raw
    .withColumn("event_timestamp",
        F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # Validate coordinates
    .withColumn("location_lat",
        F.when(
            (F.col("location_lat") < -90) |
            (F.col("location_lat") > 90),
            None).otherwise(F.col("location_lat")))
    .withColumn("location_lon",
        F.when(
            (F.col("location_lon") < -180) |
            (F.col("location_lon") > 180),
            None).otherwise(F.col("location_lon")))

    # Validate delay hours — must be non-negative
    .withColumn("delay_hours",
        F.when(F.col("delay_hours") < 0, 0)
         .otherwise(F.col("delay_hours")))

    .withColumn("event_type",
        F.initcap(F.trim(F.col("event_type"))))

    # Derive event date for partitioning
    .withColumn("event_date",
        F.to_date(F.col("event_timestamp")))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_events_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(f"{SILVER_PATH}/cargo_events"))

print(f"✓ cargo_events: {df_events_clean.count():,} rows written to silver")

# ── CASES ─────────────────────────────────────────────────────
df_cases_raw = spark.read.format("delta").load(f"{BRONZE_PATH}/cases")

df_cases_clean = (df_cases_raw
    .withColumn("created_date",
        F.to_date(F.col("created_date"), "yyyy-MM-dd"))
    .withColumn("resolved_date",
        F.to_date(F.col("resolved_date"), "yyyy-MM-dd"))

    # Validate resolution_days — must be non-negative
    # Negative = resolved before created = impossible
    .withColumn("resolution_days",
        F.when(F.col("resolution_days") < 0, None)
         .otherwise(F.col("resolution_days")))

    # Validate customer_rating — must be 1–5
    .withColumn("customer_rating",
        F.when(
            (F.col("customer_rating") < 1) |
            (F.col("customer_rating") > 5),
            None).otherwise(F.col("customer_rating")))

    .withColumn("status",   F.initcap(F.trim(F.col("status"))))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("priority", F.initcap(F.trim(F.col("priority"))))

    .withColumn("_silver_cleaned_at", F.current_timestamp())
    .drop("_bronze_ingested_at", "_source_path",
          "_bronze_year", "_bronze_month")
)

(df_cases_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("status")
    .save(f"{SILVER_PATH}/cases"))

print(f"✓ cases: {df_cases_clean.count():,} rows written to silver")

# COMMAND ----------

# DBTITLE 1,Silver summary
# ── Final summary across all silver tables ───────────────────
silver_tables = [
    "users", "ports_all_versions", "vessels_all_versions",
    "accounts_all_versions", "contacts", "contracts",
    "bookings", "cargo_events", "cases"
]

print("SILVER CLEANING SUMMARY")
print("="*55)
print(f"{'Table':<30} {'Rows':>10}  {'Status'}")
print("-"*55)

for table in silver_tables:
    try:
        df = spark.read.format("delta").load(f"{SILVER_PATH}/{table}")
        count = df.count()
        print(f"{table:<30} {count:>10,}  ✓")
    except Exception as e:
        print(f"{table:<30} {'—':>10}   ✗ {str(e)[:40]}")

print("="*55)
print("""
WHAT HAPPENS NEXT:
  03_silver_scd2  → process ports_all_versions, vessels_all_versions,
                    accounts_all_versions into proper SCD2 dimensions
                    with valid_from, valid_to, is_current columns
""")

# COMMAND ----------

# DBTITLE 1,Export reminder
print("""
EXPORT THIS NOTEBOOK:
  File → Export → Source File (.py)
  Save to: notebooks/02_silver_cleaning.py

THEN PUSH TO GITHUB:
  git add notebooks/02_silver_cleaning.py
  git commit -m "Add silver cleaning notebook — type casting, null handling, bad data rules"
  git push origin main
""")