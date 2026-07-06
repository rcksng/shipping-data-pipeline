# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# 04_GOLD_DIMENSIONS
# Purpose : Build the gold dimension layer
#           - dim_date        (calendar table — generated)
#           - dim_account     (promoted from silver SCD2)
#           - dim_vessel      (promoted from silver SCD2)
#           - dim_port        (promoted from silver SCD2)
#           - dim_contract    (contract attributes)
#           - dim_case_type   (case category/priority lookup)
#
# Design principle:
#   Gold dimensions are the final analyst-facing tables.
#   They are denormalised (all attributes in one table),
#   documented, and stable — analysts and Power BI connect here.
#   They never reference silver paths directly.
# ============================================================

MOUNT       = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"
SILVER_PATH = f"{MOUNT}/silver"
GOLD_PATH   = f"{MOUNT}/gold"

print(f"Silver : {SILVER_PATH}")
print(f"Gold   : {GOLD_PATH}")

# COMMAND ----------

# DBTITLE 1,dim_date
# ── DIM_DATE ──────────────────────────────────────────────────
# The most important dimension in any warehouse.
# Generated entirely in code — no source data needed.
# Covers 2020–2030 (adjust as needed).
#
# Why build it in code rather than load from a CSV?
#   - No dependency on any source system
#   - Fully controlled — you decide every attribute
#   - Reproducible — rerun anytime to regenerate
#   - Cheap — 3,653 rows for 10 years
# ─────────────────────────────────────────────────────────────

from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

def generate_dim_date(start_year=2020, end_year=2030):

    # Generate one row per calendar date
    start_date = datetime.date(start_year, 1, 1)
    end_date   = datetime.date(end_year, 12, 31)
    delta      = end_date - start_date

    rows = []
    for i in range(delta.days + 1):
        d = start_date + datetime.timedelta(days=i)

        # date_sk: integer key in YYYYMMDD format
        # e.g. 20240315 for March 15 2024
        # Why integer not date? Faster join, smaller storage,
        # readable as a number in debugging
        date_sk = int(d.strftime("%Y%m%d"))

        quarter = (d.month - 1) // 3 + 1
        is_weekend = d.weekday() >= 5  # 5=Saturday, 6=Sunday

        rows.append((
            date_sk,
            d,
            d.year,
            quarter,
            d.month,
            d.strftime("%B"),           # "January", "February" etc
            d.strftime("%b"),           # "Jan", "Feb" etc
            (d.month - 1) // 3 + 1,    # fiscal quarter (same as calendar here)
            d.isocalendar()[1],         # ISO week number
            d.weekday() + 1,            # day of week 1=Mon, 7=Sun
            d.strftime("%A"),           # "Monday", "Tuesday" etc
            d.strftime("%a"),           # "Mon", "Tue" etc
            d.day,
            is_weekend,
            False,                      # is_holiday placeholder
            f"Q{quarter} {d.year}",     # "Q1 2024"
            f"{d.strftime('%b')} {d.year}",  # "Jan 2024"
            d.strftime("%Y-W%V"),       # "2024-W03"
        ))

    schema = StructType([
        StructField("date_sk",          IntegerType(),  False),
        StructField("full_date",        DateType(),     False),
        StructField("year",             IntegerType(),  False),
        StructField("quarter",          IntegerType(),  False),
        StructField("month",            IntegerType(),  False),
        StructField("month_name",       StringType(),   False),
        StructField("month_name_short", StringType(),   False),
        StructField("fiscal_quarter",   IntegerType(),  False),
        StructField("week_of_year",     IntegerType(),  False),
        StructField("day_of_week",      IntegerType(),  False),
        StructField("day_name",         StringType(),   False),
        StructField("day_name_short",   StringType(),   False),
        StructField("day_of_month",     IntegerType(),  False),
        StructField("is_weekend",       BooleanType(),  False),
        StructField("is_holiday",       BooleanType(),  False),
        StructField("quarter_label",    StringType(),   False),
        StructField("month_year_label", StringType(),   False),
        StructField("week_label",       StringType(),   False),
    ])

    return spark.createDataFrame(rows, schema)

df_dim_date = generate_dim_date(2020, 2030)

(df_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_date"))

print(f"✓ dim_date: {df_dim_date.count():,} rows (2020–2030)")
print(f"  Sample rows:")
display(df_dim_date.filter(F.col("full_date").between("2024-01-01", "2024-01-03")))

# COMMAND ----------

# DBTITLE 1,dim_account
# ── DIM_ACCOUNT ───────────────────────────────────────────────
# Promoted directly from silver SCD2 output.
# Gold adds: derived attributes useful for BI slicing
#
# Key design decision:
#   We keep ALL versions (is_current=True and False) in gold.
#   Why? The fact table joins on account_sk (surrogate key).
#   A 2022 booking carries the account_sk that was current in 2022.
#   If we only kept is_current=True rows, that join would return
#   no match for historical bookings → NULL dimensions in Power BI.
#
#   Power BI filter: when users want "current accounts only",
#   they filter is_current=True in their report.
#   The dimension itself must contain all versions.
# ─────────────────────────────────────────────────────────────

df_silver_accounts = spark.read.format("delta") \
    .load(f"{SILVER_PATH}/dim_accounts")

df_dim_account = (df_silver_accounts

    # Derived attribute: revenue band — useful BI filter
    # Analysts want "large accounts" without knowing USD thresholds
    .withColumn("revenue_band",
        F.when(F.col("annual_revenue_usd") >= 100_000_000, "Enterprise")
         .when(F.col("annual_revenue_usd") >= 10_000_000,  "Large")
         .when(F.col("annual_revenue_usd") >= 1_000_000,   "Mid-Market")
         .otherwise("SMB"))

    # Derived attribute: full name for display in Power BI tooltips
    # Avoids analysts having to concatenate in DAX
    .withColumn("account_display_label",
        F.concat_ws(" | ",
            F.col("account_name"),
            F.col("account_tier"),
            F.col("country_code")))

    # Derived attribute: is_historical flag
    # Cleaner than asking Power BI users to filter on is_current=False
    .withColumn("is_historical",
        (~F.col("is_current")).cast("boolean"))

    # Add gold metadata
    .withColumn("_gold_created_at", F.current_timestamp())
)

(df_dim_account.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_account"))

total   = df_dim_account.count()
current = df_dim_account.filter(F.col("is_current") == True).count()
print(f"✓ dim_account: {total:,} total rows ({current:,} current, {total-current:,} historical)")

# COMMAND ----------

df_dim_account.display()

# COMMAND ----------

# DBTITLE 1,dim_vessel
# ── DIM_VESSEL ────────────────────────────────────────────────

df_silver_vessels = spark.read.format("delta") \
    .load(f"{SILVER_PATH}/dim_vessels")

df_dim_vessel = (df_silver_vessels

    # Derived: vessel size category — useful for capacity analysis
    .withColumn("vessel_size_category",
        F.when(F.col("capacity_teu") >= 18000, "Ultra Large (ULCV)")
         .when(F.col("capacity_teu") >= 10000, "Very Large")
         .when(F.col("capacity_teu") >= 5000,  "Large")
         .when(F.col("capacity_teu") >= 2000,  "Medium")
         .otherwise("Small / Feeder"))

    # Derived: vessel age at current date
    .withColumn("vessel_age_years",
        F.year(F.current_date()) - F.col("year_built"))

    # Derived: age band — cleaner for BI filters
    .withColumn("vessel_age_band",
        F.when(F.col("vessel_age_years") <= 5,  "0–5 years")
         .when(F.col("vessel_age_years") <= 10, "6–10 years")
         .when(F.col("vessel_age_years") <= 20, "11–20 years")
         .otherwise("20+ years"))

    .withColumn("_gold_created_at", F.current_timestamp())
)

(df_dim_vessel.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_vessel"))

total   = df_dim_vessel.count()
current = df_dim_vessel.filter(F.col("is_current") == True).count()
print(f"✓ dim_vessel: {total:,} total rows ({current:,} current, {total-current:,} historical)")

# COMMAND ----------

df_dim_vessel.display()

# COMMAND ----------

# DBTITLE 1,dim_port
# ── DIM_PORT ──────────────────────────────────────────────────

df_silver_ports = spark.read.format("delta") \
    .load(f"{SILVER_PATH}/dim_ports")

df_dim_port = (df_silver_ports

    # Derived: capacity band — avoids analysts knowing TEU numbers
    .withColumn("capacity_band",
        F.when(F.col("annual_capacity_teu") >= 20_000_000, "Mega Hub")
         .when(F.col("annual_capacity_teu") >= 5_000_000,  "Major Hub")
         .when(F.col("annual_capacity_teu") >= 1_000_000,  "Regional Hub")
         .otherwise("Feeder Port"))

    # Derived: display label for Power BI map tooltips
    .withColumn("port_display_label",
        F.concat_ws(" ",
            F.col("port_name"),
            F.concat(F.lit("("), F.col("port_code"), F.lit(")"))))

    .withColumn("_gold_created_at", F.current_timestamp())
)

(df_dim_port.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_port"))

total   = df_dim_port.count()
current = df_dim_port.filter(F.col("is_current") == True).count()
print(f"✓ dim_port: {total:,} total rows ({current:,} current, {total-current:,} historical)")

# COMMAND ----------

df_dim_port.display()

# COMMAND ----------

# DBTITLE 1,dim_contract and dim_user
# ── DIM_CONTRACT ──────────────────────────────────────────────
# Contracts are reference data for bookings
# Not SCD2 — contract terms are fixed at signing
# Type 1 updates only (corrections)

df_contracts = spark.read.format("delta") \
    .load(f"{SILVER_PATH}/contracts")

df_dim_contract = (df_contracts

    # Derived: contract duration in days
    .withColumn("contract_duration_days",
        F.datediff(F.col("end_date"), F.col("start_date")))

    # Derived: is contract currently active?
    .withColumn("is_currently_active",
        F.when(
            (F.col("status") == "Active") &
            (F.col("end_date") >= F.current_date()),
            True).otherwise(False))

    # Derived: rate tier label for BI
    .withColumn("rate_tier_rank",
        F.when(F.col("rate_tier") == "Standard",   1)
         .when(F.col("rate_tier") == "Preferred",  2)
         .when(F.col("rate_tier") == "Premium",    3)
         .when(F.col("rate_tier") == "Key Account",4)
         .otherwise(0))

    .withColumn("_gold_created_at", F.current_timestamp())

    # Drop silver metadata
    .drop("_silver_cleaned_at", "_source_object")
)

(df_dim_contract.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_contract"))

print(f"✓ dim_contract: {df_dim_contract.count():,} rows")

# ── DIM_USER ──────────────────────────────────────────────────
df_users = spark.read.format("delta").load(f"{SILVER_PATH}/users")

df_dim_user = (df_users
    .withColumn("full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("_gold_created_at", F.current_timestamp())
    .drop("_silver_cleaned_at", "_source_object")
)

(df_dim_user.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_user"))

print(f"✓ dim_user: {df_dim_user.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Gold dimensions summary
# ── Summary ───────────────────────────────────────────────────
gold_dims = {
    "dim_date":     f"{GOLD_PATH}/dim_date",
    "dim_account":  f"{GOLD_PATH}/dim_account",
    "dim_vessel":   f"{GOLD_PATH}/dim_vessel",
    "dim_port":     f"{GOLD_PATH}/dim_port",
    "dim_contract": f"{GOLD_PATH}/dim_contract",
    "dim_user":     f"{GOLD_PATH}/dim_user",
}

print("GOLD DIMENSIONS SUMMARY")
print("="*55)
print(f"{'Table':<20} {'Rows':>8}   {'Key Column'}")
print("-"*55)

key_cols = {
    "dim_date":     "date_sk (YYYYMMDD integer)",
    "dim_account":  "account_sk (surrogate, SCD2)",
    "dim_vessel":   "vessel_sk  (surrogate, SCD2)",
    "dim_port":     "port_sk    (surrogate, SCD2)",
    "dim_contract": "contract_id (natural key)",
    "dim_user":     "user_id     (natural key)",
}

for name, path in gold_dims.items():
    df    = spark.read.format("delta").load(path)
    count = df.count()
    print(f"{name:<20} {count:>8,}   {key_cols[name]}")

print("="*55)
print("""
NEXT NOTEBOOK: 05_gold_facts
  - fact_bookings    (grain: one row per booking)
  - fact_cargo_events (grain: one row per vessel movement event)
  - fact_cases       (grain: one row per customer service case)

  Each fact joins to gold dims via surrogate/natural keys.
  SCD2 join on account_sk resolves historical tier correctly.
""")

print("""
EXPORT + PUSH:
  File → Export → Source File (.py)
  Save as: notebooks/04_gold_dimensions.py

  git add notebooks/04_gold_dimensions.py
  git commit -m "Add gold dimensions — dim_date, dim_account, dim_vessel, dim_port, dim_contract, dim_user"
  git push origin main
""")