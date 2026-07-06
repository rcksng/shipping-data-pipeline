# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# 05_GOLD_FACTS
# Purpose : Build the gold fact layer
#           - fact_bookings      (grain: one row per booking)
#           - fact_cargo_events  (grain: one row per vessel event)
#           - fact_cases         (grain: one row per service case)
#
# Key design principle — SCD2 surrogate key resolution:
#   Fact tables join to dimensions using surrogate keys, not
#   natural keys. For SCD2 dimensions (account, vessel, port),
#   we must find the surrogate key that was valid AT THE TIME
#   of the booking — not the current surrogate key.
#
#   This is what makes historical reporting correct.
# ============================================================

MOUNT       = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"
SILVER_PATH = f"{MOUNT}/silver"
GOLD_PATH   = f"{MOUNT}/gold"

print(f"Silver : {SILVER_PATH}")
print(f"Gold   : {GOLD_PATH}")

# COMMAND ----------

# DBTITLE 1,Load all dimensions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load all gold dimensions
# These are the lookup tables fact tables will join against

dim_account  = spark.read.format("delta").load(f"{GOLD_PATH}/dim_account")
dim_vessel   = spark.read.format("delta").load(f"{GOLD_PATH}/dim_vessel")
dim_port     = spark.read.format("delta").load(f"{GOLD_PATH}/dim_port")
dim_contract = spark.read.format("delta").load(f"{GOLD_PATH}/dim_contract")
dim_user     = spark.read.format("delta").load(f"{GOLD_PATH}/dim_user")
dim_date     = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")

print("Gold dimensions loaded:")
print(f"  dim_account  : {dim_account.count():,} rows")
print(f"  dim_vessel   : {dim_vessel.count():,} rows")
print(f"  dim_port     : {dim_port.count():,} rows")
print(f"  dim_contract : {dim_contract.count():,} rows")
print(f"  dim_user     : {dim_user.count():,} rows")
print(f"  dim_date     : {dim_date.count():,} rows")

# COMMAND ----------

# DBTITLE 1,SCD2 surrogate key resolution (critical — read this carefully)
# ── SCD2 Surrogate Key Resolution ─────────────────────────────
#
# Problem:
#   A booking has account_id = "ACC001" and booking_date = 2022-06-15
#   dim_account has TWO rows for ACC001:
#     account_sk=1001, valid_from=2022-01-01, valid_to=2023-11-14  ← Bronze tier
#     account_sk=8842, valid_from=2023-11-15, valid_to=NULL        ← Gold tier
#
#   A naive join ON account_id would return BOTH rows → duplicate fact rows
#   Joining on is_current=True would return only sk=8842 → wrong tier for 2022
#
# Solution:
#   Join on account_id AND booking_date falls within valid_from → valid_to
#   This returns exactly one row — the version that was active on booking_date
#
# This join pattern is called a "point-in-time join" or "range join"
# It's the core technique that makes SCD2 meaningful in a fact table
# ─────────────────────────────────────────────────────────────

# Prepare SCD2-aware dimension lookups
# We only need the surrogate key + natural key + date range
# The full attributes stay in the dimension table

account_keys = dim_account.select(
    "account_sk",
    "account_id",
    "valid_from",
    F.coalesce(
        F.col("valid_to"),
        F.lit("9999-12-31").cast("date")   # NULL valid_to = still current
    ).alias("valid_to_safe")               # replace NULL with far-future date
)                                          # so date comparison works cleanly

vessel_keys = dim_vessel.select(
    "vessel_sk",
    "vessel_id",
    "valid_from",
    F.coalesce(
        F.col("valid_to"),
        F.lit("9999-12-31").cast("date")
    ).alias("valid_to_safe")
)

port_keys = dim_port.select(
    "port_sk",
    "port_id",
    "valid_from",
    F.coalesce(
        F.col("valid_to"),
        F.lit("9999-12-31").cast("date")
    ).alias("valid_to_safe")
)

print("✓ SCD2 key lookup tables prepared")
print("""
Why coalesce valid_to to '9999-12-31'?
  The current version has valid_to = NULL (no end date yet).
  SQL date comparisons with NULL always return NULL (not True/False).
  Replacing NULL with a far-future date makes the range comparison work:
    booking_date BETWEEN valid_from AND valid_to_safe
  A 2024 booking correctly matches the current version's range.
""")

# COMMAND ----------

# DBTITLE 1,fact_bookings
# ── FACT_BOOKINGS ─────────────────────────────────────────────
# Grain: one row per booking
# Primary fact for commercial/revenue analysis
#
# Measures (what you aggregate):
#   teu_count         → volume analysis
#   freight_rate_usd  → rate analysis
#   total_revenue_usd → revenue analysis
#   transit_days      → derived: ETA - ETD
#   delay_days        → derived: actual_arrival - ETA (if available)
#
# Dimensions (what you slice by):
#   date_sk           → joins to dim_date (booking date)
#   account_sk        → SCD2 join (historical tier at booking time)
#   vessel_sk         → SCD2 join (historical flag/operator)
#   origin_port_sk    → SCD2 join (historical port type)
#   dest_port_sk      → SCD2 join (historical port type)
#   contract_id       → natural key join to dim_contract
#   owner_user_id     → natural key join to dim_user
# ─────────────────────────────────────────────────────────────

df_bookings = spark.read.format("delta").load(f"{SILVER_PATH}/bookings")

print(f"Silver bookings loaded: {df_bookings.count():,} rows")

# ── Step 1: Derive date_sk from booking_date ──────────────────
# dim_date uses YYYYMMDD integer key
# Convert booking_date to same format for join

df_bookings = df_bookings.withColumn(
    "date_sk",
    (F.date_format(F.col("booking_date"), "yyyyMMdd")).cast("integer")
)

# ── Step 2: SCD2 join — resolve account_sk ───────────────────
# Find which account version was active on the booking date
# booking_date must fall within [valid_from, valid_to_safe]

df_bookings = (df_bookings
    .join(
        account_keys,
        on=(
            (df_bookings["account_id"] == account_keys["account_id"]) &
            (F.to_date(df_bookings["booking_date"]) >= account_keys["valid_from"]) &
            (F.to_date(df_bookings["booking_date"]) <= account_keys["valid_to_safe"])
        ),
        how="left"   # left join: keep bookings even if no dimension match
    )
    .drop(account_keys["account_id"])
    .drop("valid_from", "valid_to_safe")
)

# ── Step 3: SCD2 join — resolve vessel_sk ────────────────────
df_bookings = (df_bookings
    .join(
        vessel_keys,
        on=(
            (df_bookings["vessel_id"] == vessel_keys["vessel_id"]) &
            (F.to_date(df_bookings["booking_date"]) >= vessel_keys["valid_from"]) &
            (F.to_date(df_bookings["booking_date"]) <= vessel_keys["valid_to_safe"])
        ),
        how="left"
    )
    .drop(vessel_keys["vessel_id"])
    .drop("valid_from", "valid_to_safe")
)

# ── Step 4: SCD2 join — resolve origin_port_sk ───────────────
origin_port_keys = port_keys.withColumnRenamed("port_sk",  "origin_port_sk") \
                             .withColumnRenamed("port_id",  "origin_port_id_key")

df_bookings = (df_bookings
    .join(
        origin_port_keys,
        on=(
            (df_bookings["origin_port_id"] == origin_port_keys["origin_port_id_key"]) &
            (F.to_date(df_bookings["booking_date"]) >= origin_port_keys["valid_from"]) &
            (F.to_date(df_bookings["booking_date"]) <= origin_port_keys["valid_to_safe"])
        ),
        how="left"
    )
    .drop("origin_port_id_key", "valid_from", "valid_to_safe")
)

# ── Step 5: SCD2 join — resolve dest_port_sk ─────────────────
dest_port_keys = port_keys.withColumnRenamed("port_sk", "dest_port_sk") \
                           .withColumnRenamed("port_id", "dest_port_id_key")

df_bookings = (df_bookings
    .join(
        dest_port_keys,
        on=(
            (df_bookings["destination_port_id"] == dest_port_keys["dest_port_id_key"]) &
            (F.to_date(df_bookings["booking_date"]) >= dest_port_keys["valid_from"]) &
            (F.to_date(df_bookings["booking_date"]) <= dest_port_keys["valid_to_safe"])
        ),
        how="left"
    )
    .drop("dest_port_id_key", "valid_from", "valid_to_safe")
)

# ── Step 6: Derive measures ───────────────────────────────────
df_bookings = (df_bookings

    # Transit days: scheduled ETA - ETD
    # Useful for trade lane analysis — how long does each route take?
    .withColumn("scheduled_transit_days",
        F.datediff(F.col("eta"), F.col("etd")))

    # Delay days: actual arrival vs expected arrival
    # Only computable when actual_arrival is known
    # Positive = late, Negative = early, NULL = voyage incomplete
    .withColumn("arrival_delay_days",
        F.when(
            F.col("actual_arrival").isNotNull() &
            F.col("eta").isNotNull(),
            F.datediff(F.col("actual_arrival"), F.col("eta"))
        ).otherwise(None))

    # Is delayed: flag for easy filtering in Power BI
    .withColumn("is_delayed",
        F.when(F.col("arrival_delay_days") > 0, True)
         .when(F.col("arrival_delay_days").isNotNull(), False)
         .otherwise(None))   # NULL = unknown (voyage incomplete)

    # Booking month label — useful for Power BI time slicers
    .withColumn("booking_month_label",
        F.date_format(F.col("booking_date"), "MMM yyyy"))
)

# ── Step 7: Select final fact columns ─────────────────────────
df_fact_bookings = df_bookings.select(

    # Surrogate key for this fact row
    F.monotonically_increasing_id().alias("booking_fact_sk"),

    # Natural key — link back to source system
    "booking_id",
    "booking_reference",

    # Foreign keys to dimensions
    "date_sk",           # → dim_date
    "account_sk",        # → dim_account (SCD2 — historical version)
    "vessel_sk",         # → dim_vessel  (SCD2 — historical version)
    "origin_port_sk",    # → dim_port    (SCD2 — historical version)
    "dest_port_sk",      # → dim_port    (SCD2 — historical version)
    "contract_id",       # → dim_contract (natural key)
    "owner_user_id",     # → dim_user    (natural key)

    # Degenerate dimensions (descriptive, no dim table needed)
    "booking_status",
    "cargo_type",
    "container_size",
    "booking_month_label",

    # Measures
    "teu_count",
    "freight_rate_usd",
    "total_revenue_usd",
    "scheduled_transit_days",
    "arrival_delay_days",
    "is_delayed",
    "teu_count_is_corrected",  # data quality flag from silver

    # Date columns (kept for reference alongside date_sk)
    "booking_date",
    "etd",
    "eta",
    "actual_departure",
    "actual_arrival",
    "booking_year",
    "booking_month",

    # Metadata
    F.current_timestamp().alias("_gold_created_at")
)

# ── Step 8: Write fact_bookings ───────────────────────────────
(df_fact_bookings.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("booking_year", "booking_month")
    .save(f"{GOLD_PATH}/fact_bookings"))

# ── Step 9: Verify ────────────────────────────────────────────
df_verify = spark.read.format("delta").load(f"{GOLD_PATH}/fact_bookings")

total          = df_verify.count()
matched_acct   = df_verify.filter(F.col("account_sk").isNotNull()).count()
matched_vessel = df_verify.filter(F.col("vessel_sk").isNotNull()).count()
delayed        = df_verify.filter(F.col("is_delayed") == True).count()
unknown_cargo  = df_verify.filter(F.col("cargo_type") == "Unknown").count()

print(f"\nfact_bookings results:")
print(f"  Total rows              : {total:,}")
print(f"  account_sk resolved     : {matched_acct:,}  ({100*matched_acct/total:.1f}%)")
print(f"  vessel_sk resolved      : {matched_vessel:,}  ({100*matched_vessel/total:.1f}%)")
print(f"  Delayed bookings        : {delayed:,}")
print(f"  Unknown cargo type      : {unknown_cargo:,}")

# SCD2 correctness check — no booking should have 2+ account_sk matches
duplicates = (df_verify
    .groupBy("booking_id")
    .count()
    .filter(F.col("count") > 1)
    .count())

print(f"\n  Duplicate booking_id check: "
      f"{'✓ None — grain intact' if duplicates == 0 else f'✗ {duplicates} duplicates — investigate SCD2 join'}")

# COMMAND ----------

# DBTITLE 1,fact_cargo_events
# ── FACT_CARGO_EVENTS ─────────────────────────────────────────
# Grain: one row per cargo event (vessel movement)
# Primary fact for operational/delay analysis
#
# Measures:
#   delay_hours       → how much delay at this event
#
# Dimensions:
#   date_sk           → event date → dim_date
#   vessel_sk         → SCD2: which vessel version was operating
#   port_sk           → SCD2: which port version was used
#   booking_id        → links back to fact_bookings (not a dim join)
# ─────────────────────────────────────────────────────────────

df_events = spark.read.format("delta").load(f"{SILVER_PATH}/cargo_events")

# date_sk from event_timestamp
df_events = df_events.withColumn(
    "date_sk",
    F.date_format(F.to_date(F.col("event_timestamp")), "yyyyMMdd").cast("integer")
)

# SCD2 join — vessel version active on event date
event_vessel_keys = dim_vessel.select(
    "vessel_sk", "vessel_id", "valid_from",
    F.coalesce(F.col("valid_to"),
               F.lit("9999-12-31").cast("date")).alias("valid_to_safe")
)

df_events = (df_events
    .join(event_vessel_keys,
        on=(
            (df_events["vessel_id"] == event_vessel_keys["vessel_id"]) &
            (F.to_date(df_events["event_timestamp"]) >= event_vessel_keys["valid_from"]) &
            (F.to_date(df_events["event_timestamp"]) <= event_vessel_keys["valid_to_safe"])
        ),
        how="left")
    .drop(event_vessel_keys["vessel_id"])
    .drop("valid_from", "valid_to_safe")
)

# SCD2 join — port version active on event date
event_port_keys = dim_port.select(
    "port_sk", "port_id", "valid_from",
    F.coalesce(F.col("valid_to"),
               F.lit("9999-12-31").cast("date")).alias("valid_to_safe")
)

df_events = (df_events
    .join(event_port_keys,
        on=(
            (df_events["port_id"] == event_port_keys["port_id"]) &
            (F.to_date(df_events["event_timestamp"]) >= event_port_keys["valid_from"]) &
            (F.to_date(df_events["event_timestamp"]) <= event_port_keys["valid_to_safe"])
        ),
        how="left")
    .drop(event_port_keys["port_id"])
    .drop("valid_from", "valid_to_safe")
)

# Derive: is this a delay event?
df_events = df_events.withColumn("is_delay_event",
    F.when(F.col("delay_hours") > 0, True).otherwise(False))

# Final column selection
df_fact_events = df_events.select(
    F.monotonically_increasing_id().alias("event_fact_sk"),
    "event_id",
    "booking_id",         # link to fact_bookings
    "date_sk",            # → dim_date
    "vessel_sk",          # → dim_vessel (SCD2)
    "port_sk",            # → dim_port   (SCD2)
    "event_type",         # degenerate dimension
    "event_timestamp",
    "event_date",
    "location_lat",
    "location_lon",
    "delay_hours",        # measure
    "is_delay_event",     # derived flag
    "notes",
    F.current_timestamp().alias("_gold_created_at")
)

(df_fact_events.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(f"{GOLD_PATH}/fact_cargo_events"))

total = df_fact_events.count()
delays = df_fact_events.filter(F.col("is_delay_event") == True).count()
print(f"✓ fact_cargo_events: {total:,} rows  ({delays:,} delay events)")

# COMMAND ----------

# DBTITLE 1,fact_cases
# ── FACT_CASES ────────────────────────────────────────────────
# Grain: one row per customer service case
# Primary fact for customer service / SLA analysis
#
# Measures:
#   resolution_days   → how long to resolve
#   customer_rating   → satisfaction score (1–5)
#
# Dimensions:
#   date_sk           → case created date → dim_date
#   account_sk        → SCD2: account tier when case was raised
#   owner_user_id     → which agent handled it
# ─────────────────────────────────────────────────────────────

df_cases = spark.read.format("delta").load(f"{SILVER_PATH}/cases")

# date_sk from created_date
df_cases = df_cases.withColumn(
    "date_sk",
    F.date_format(F.col("created_date"), "yyyyMMdd").cast("integer")
)

# SCD2 join — account version active when case was created
case_account_keys = dim_account.select(
    "account_sk", "account_id", "valid_from",
    F.coalesce(F.col("valid_to"),
               F.lit("9999-12-31").cast("date")).alias("valid_to_safe")
)

df_cases = (df_cases
    .join(case_account_keys,
        on=(
            (df_cases["account_id"] == case_account_keys["account_id"]) &
            (df_cases["created_date"] >= case_account_keys["valid_from"]) &
            (df_cases["created_date"] <= case_account_keys["valid_to_safe"])
        ),
        how="left")
    .drop(case_account_keys["account_id"])
    .drop("valid_from", "valid_to_safe")
)

# Derive: SLA breach flag
# Define SLA thresholds by priority
# Critical = 1 day, High = 3 days, Medium = 7 days, Low = 14 days
df_cases = df_cases.withColumn("sla_days_target",
    F.when(F.col("priority") == "Critical", 1)
     .when(F.col("priority") == "High",     3)
     .when(F.col("priority") == "Medium",   7)
     .when(F.col("priority") == "Low",     14)
     .otherwise(7))

df_cases = df_cases.withColumn("is_sla_breached",
    F.when(
        F.col("resolution_days").isNotNull() &
        (F.col("resolution_days") > F.col("sla_days_target")),
        True)
     .otherwise(False))

# Final column selection
df_fact_cases = df_cases.select(
    F.monotonically_increasing_id().alias("case_fact_sk"),
    "case_id",
    "case_number",
    "booking_id",         # link to fact_bookings
    "date_sk",            # → dim_date (created date)
    "account_sk",         # → dim_account (SCD2)
    "owner_user_id",      # → dim_user
    "category",           # degenerate dimension
    "priority",           # degenerate dimension
    "status",             # degenerate dimension
    "created_date",
    "resolved_date",
    "resolution_days",    # measure
    "customer_rating",    # measure
    "sla_days_target",    # derived
    "is_sla_breached",    # derived flag
    F.current_timestamp().alias("_gold_created_at")
)

(df_fact_cases.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("status")
    .save(f"{GOLD_PATH}/fact_cases"))

total   = df_fact_cases.count()
breached = df_fact_cases.filter(F.col("is_sla_breached") == True).count()
print(f"✓ fact_cases: {total:,} rows  ({breached:,} SLA breaches)")

# COMMAND ----------

# DBTITLE 1,Gold layer summary and star schema proof
# ── Full gold layer summary ───────────────────────────────────
print("COMPLETE GOLD LAYER")
print("="*60)

gold_tables = {
    "dim_date":          f"{GOLD_PATH}/dim_date",
    "dim_account":       f"{GOLD_PATH}/dim_account",
    "dim_vessel":        f"{GOLD_PATH}/dim_vessel",
    "dim_port":          f"{GOLD_PATH}/dim_port",
    "dim_contract":      f"{GOLD_PATH}/dim_contract",
    "dim_user":          f"{GOLD_PATH}/dim_user",
    "fact_bookings":     f"{GOLD_PATH}/fact_bookings",
    "fact_cargo_events": f"{GOLD_PATH}/fact_cargo_events",
    "fact_cases":        f"{GOLD_PATH}/fact_cases",
}

print(f"{'Table':<25} {'Rows':>10}   {'Type'}")
print("-"*55)
for name, path in gold_tables.items():
    df    = spark.read.format("delta").load(path)
    count = df.count()
    ttype = "DIMENSION" if name.startswith("dim") else "FACT"
    print(f"{name:<25} {count:>10,}   {ttype}")

print("="*60)

# ── Star schema proof query ───────────────────────────────────
# This is the query that proves your entire pipeline works end-to-end
# Revenue by account tier (historical) by month — the key business question
print("\nSTAR SCHEMA PROOF QUERY")
print("Revenue by account tier (historical) and month:")
print("-"*55)

df_fact  = spark.read.format("delta").load(f"{GOLD_PATH}/fact_bookings")
df_acct  = spark.read.format("delta").load(f"{GOLD_PATH}/dim_account")
df_date  = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")

result = (df_fact
    .join(df_acct, on="account_sk", how="left")
    .join(df_date, on="date_sk",    how="left")
    .filter(F.col("booking_status") != "Cancelled")
    .groupBy(
        "account_tier",
        "month_year_label",
        F.col("year"),
        F.col("month")
    )
    .agg(
        F.sum("total_revenue_usd").alias("total_revenue"),
        F.sum("teu_count").alias("total_teu"),
        F.countDistinct("booking_id").alias("booking_count"),
        F.avg("arrival_delay_days").alias("avg_delay_days")
    )
    .orderBy("year", "month", "account_tier")
)

display(result)

print("""
This result proves your pipeline is end-to-end correct:
  - Revenue is attributed to the tier the account HELD at booking time
  - A 2022 booking from a then-Bronze account shows under Bronze
    even if that account is now Gold
  - This is SCD2 working correctly through the full stack
""")

# COMMAND ----------

# DBTITLE 1,Export reminder
print("""
EXPORT THIS NOTEBOOK:
  File → Export → Source File (.py)
  Save as: notebooks/05_gold_facts.py

PUSH TO GITHUB:
  git add notebooks/05_gold_facts.py
  git commit -m "Add gold facts — fact_bookings, fact_cargo_events, fact_cases with SCD2 joins"
  git push origin main

YOUR COMPLETE PIPELINE IS NOW BUILT:
  landing → bronze → silver (clean) → silver (SCD2) → gold (dims) → gold (facts)

REMAINING:
  06_quality_checks  → automated assertions that run after every load
  README + docs      → the portfolio artifact
  Interview prep     → translating all of this into stories
""")