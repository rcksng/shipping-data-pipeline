# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# 01_BRONZE_INGESTION
# Purpose : Read raw CSVs from landing zone → write as
#           partitioned, immutable Delta tables in bronze layer
# Author  : Rocky
# ============================================================

# ── Path configuration ───────────────────────────────────────
MOUNT         = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"
LANDING_PATH  = f"{MOUNT}/landing"
BRONZE_PATH   = f"{MOUNT}/bronze"

# ── Objects to ingest ────────────────────────────────────────
# Each entry: (object_name, partition_column, merge_cols_for_changes)
OBJECTS = [
    ("users",        "department",    None),
    ("ports",        None,            "port_id"),
    ("vessels",      None,            "vessel_id"),
    ("accounts",     "region",        "account_id"),
    ("contacts",     None,            None),
    ("contracts",    "status",        None),
    ("bookings",     "booking_status","booking_id"),
    ("cargo_events", None,            "event_id"),
    ("cases",        "status",        None),
]

print(f"Landing  : {LANDING_PATH}")
print(f"Bronze   : {BRONZE_PATH}")
print(f"Objects  : {[o[0] for o in OBJECTS]}")

# COMMAND ----------

# DBTITLE 1,Schema definitions
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DecimalType, BooleanType, DateType, TimestampType
)

# Explicit schemas for every object
# Never infer schema from CSV in production — inference is slow and
# can change silently when source data changes

SCHEMAS = {

    "users": StructType([
        StructField("user_id",          StringType(),  False),
        StructField("first_name",       StringType(),  True),
        StructField("last_name",        StringType(),  True),
        StructField("email",            StringType(),  True),
        StructField("department",       StringType(),  True),
        StructField("role",             StringType(),  True),
        StructField("region",           StringType(),  True),
        StructField("is_active",        StringType(),  True),  # read as string, cast later
        StructField("created_date",     StringType(),  True),
        StructField("last_login_date",  StringType(),  True),
        StructField("_extracted_at",    StringType(),  True),
    ]),

    "ports": StructType([
        StructField("port_id",                StringType(),     False),
        StructField("port_code",              StringType(),     True),
        StructField("port_name",              StringType(),     True),
        StructField("country_code",           StringType(),     True),
        StructField("region",                 StringType(),     True),
        StructField("port_type",              StringType(),     True),
        StructField("annual_capacity_teu",    LongType(),       True),
        StructField("is_active",              StringType(),     True),
        StructField("last_updated",           StringType(),     True),
        StructField("_extracted_at",          StringType(),     True),
    ]),

    "vessels": StructType([
        StructField("vessel_id",        StringType(),   False),
        StructField("imo_number",       StringType(),   True),
        StructField("vessel_name",      StringType(),   True),
        StructField("vessel_type",      StringType(),   True),
        StructField("flag_country",     StringType(),   True),
        StructField("operator",         StringType(),   True),
        StructField("capacity_teu",     IntegerType(),  True),
        StructField("year_built",       IntegerType(),  True),
        StructField("is_active",        StringType(),   True),
        StructField("home_port_code",   StringType(),   True),
        StructField("last_updated",     StringType(),   True),
        StructField("_extracted_at",    StringType(),   True),
    ]),

    "accounts": StructType([
        StructField("account_id",            StringType(),   False),
        StructField("account_name",          StringType(),   True),
        StructField("industry",              StringType(),   True),
        StructField("account_tier",          StringType(),   True),
        StructField("region",                StringType(),   True),
        StructField("country_code",          StringType(),   True),
        StructField("annual_revenue_usd",    LongType(),     True),
        StructField("employee_count",        IntegerType(),  True),
        StructField("primary_trade_lane",    StringType(),   True),
        StructField("owner_user_id",         StringType(),   True),
        StructField("is_active",             StringType(),   True),
        StructField("created_date",          StringType(),   True),
        StructField("last_modified_date",    StringType(),   True),
        StructField("_extracted_at",         StringType(),   True),
    ]),

    "contacts": StructType([
        StructField("contact_id",    StringType(),  False),
        StructField("account_id",    StringType(),  True),
        StructField("first_name",    StringType(),  True),
        StructField("last_name",     StringType(),  True),
        StructField("email",         StringType(),  True),
        StructField("phone",         StringType(),  True),
        StructField("job_title",     StringType(),  True),
        StructField("contact_type",  StringType(),  True),
        StructField("is_active",     StringType(),  True),
        StructField("created_date",  StringType(),  True),
        StructField("_extracted_at", StringType(),  True),
    ]),

    "contracts": StructType([
        StructField("contract_id",              StringType(),      False),
        StructField("account_id",               StringType(),      True),
        StructField("contract_name",            StringType(),      True),
        StructField("contract_type",            StringType(),      True),
        StructField("rate_tier",                StringType(),      True),
        StructField("origin_region",            StringType(),      True),
        StructField("destination_region",       StringType(),      True),
        StructField("cargo_type",               StringType(),      True),
        StructField("volume_commitment_teu",    IntegerType(),     True),
        StructField("rate_per_teu_usd",         IntegerType(),     True),
        StructField("contract_value_usd",       LongType(),        True),
        StructField("start_date",               StringType(),      True),
        StructField("end_date",                 StringType(),      True),
        StructField("status",                   StringType(),      True),
        StructField("owner_user_id",            StringType(),      True),
        StructField("created_date",             StringType(),      True),
        StructField("_extracted_at",            StringType(),      True),
    ]),

    "bookings": StructType([
        StructField("booking_id",           StringType(),      False),
        StructField("booking_reference",    StringType(),      True),
        StructField("account_id",           StringType(),      True),
        StructField("contact_id",           StringType(),      True),
        StructField("contract_id",          StringType(),      True),
        StructField("vessel_id",            StringType(),      True),
        StructField("origin_port_id",       StringType(),      True),
        StructField("destination_port_id",  StringType(),      True),
        StructField("cargo_type",           StringType(),      True),
        StructField("container_size",       StringType(),      True),
        StructField("teu_count",            IntegerType(),     True),
        StructField("booking_status",       StringType(),      True),
        StructField("freight_rate_usd",     IntegerType(),     True),
        StructField("total_revenue_usd",    DecimalType(12,2), True),
        StructField("booking_date",         StringType(),      True),
        StructField("etd",                  StringType(),      True),
        StructField("eta",                  StringType(),      True),
        StructField("actual_departure",     StringType(),      True),
        StructField("actual_arrival",       StringType(),      True),
        StructField("owner_user_id",        StringType(),      True),
        StructField("created_date",         StringType(),      True),
        StructField("last_modified_date",   StringType(),      True),
        StructField("_extracted_at",        StringType(),      True),
    ]),

    "cargo_events": StructType([
        StructField("event_id",          StringType(),      False),
        StructField("booking_id",        StringType(),      True),
        StructField("vessel_id",         StringType(),      True),
        StructField("port_id",           StringType(),      True),
        StructField("event_type",        StringType(),      True),
        StructField("event_timestamp",   StringType(),      True),
        StructField("location_lat",      DecimalType(9,6),  True),
        StructField("location_lon",      DecimalType(9,6),  True),
        StructField("delay_hours",       IntegerType(),     True),
        StructField("notes",             StringType(),      True),
        StructField("_extracted_at",     StringType(),      True),
    ]),

    "cases": StructType([
        StructField("case_id",           StringType(),   False),
        StructField("case_number",       StringType(),   True),
        StructField("account_id",        StringType(),   True),
        StructField("contact_id",        StringType(),   True),
        StructField("booking_id",        StringType(),   True),
        StructField("subject",           StringType(),   True),
        StructField("category",          StringType(),   True),
        StructField("priority",          StringType(),   True),
        StructField("status",            StringType(),   True),
        StructField("owner_user_id",     StringType(),   True),
        StructField("created_date",      StringType(),   True),
        StructField("resolved_date",     StringType(),   True),
        StructField("resolution_days",   IntegerType(),  True),
        StructField("customer_rating",   IntegerType(),  True),
        StructField("_extracted_at",     StringType(),   True),
    ]),
}

print("✓ Schemas defined for all objects")
print(f"  Objects with schemas: {list(SCHEMAS.keys())}")

# COMMAND ----------

print(MOUNT)
print(BRONZE_PATH)
print(LANDING_PATH)

# COMMAND ----------

# DBTITLE 1,Ingestion function
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def ingest_to_bronze(object_name, partition_col=None):
    """
    Reads all CSV files for an object from landing zone.
    Adds bronze metadata columns.
    Writes as Delta table to bronze layer.
    Uses overwrite on first load (idempotent — safe to rerun).
    """

    landing_folder = f"{LANDING_PATH}/{object_name}"
    bronze_folder  = f"{BRONZE_PATH}/{object_name}"
    schema         = SCHEMAS[object_name]

    print(f"\n{'='*55}")
    print(f"  Ingesting: {object_name}")
    print(f"  From     : {landing_folder}")
    print(f"  To       : {bronze_folder}")

    # ── Step 1: Read all CSVs in the landing folder ──────────
    # We read ALL csv files (full + changes) in one shot at bronze
    # Bronze doesn't distinguish — it takes everything as-is
    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("nullValue", "")        # treat empty strings as null
          .option("multiLine", "false")   # faster for single-line records
          .schema(schema)                 # explicit schema — never infer
          .load(landing_folder)
    )

    raw_count = df.count()
    print(f"  Raw rows : {raw_count:,}")

    # ── Step 2: Add bronze metadata columns ──────────────────
    # These columns are added by the DE pipeline — not from source
    # They answer: when did THIS pipeline see this record?
    df_bronze = (df
        .withColumn("_bronze_ingested_at",
                    F.current_timestamp())          # when pipeline ran
        .withColumn("_source_object",
                    F.lit(object_name))             # which object
        .withColumn("_source_path",
                    F.input_file_name())            # which exact file
        .withColumn("_bronze_year",
                    F.year(F.current_timestamp()))  # partition helpers
        .withColumn("_bronze_month",
                    F.month(F.current_timestamp()))
    )

    # ── Step 3: Write to bronze as Delta ─────────────────────
    writer = (df_bronze.write
              .format("delta")
              .mode("overwrite")                    # idempotent — rerun safe
              .option("overwriteSchema", "true")    # allow schema evolution
    )

    # Partition only if a partition column is specified
    # Not every small table needs partitioning
    if partition_col:
        writer = writer.partitionBy(partition_col)
        print(f"  Partition: {partition_col}")
    else:
        print(f"  Partition: none (small table)")

    writer.save(bronze_folder)

    # ── Step 4: Verify write ──────────────────────────────────
    df_verify = spark.read.format("delta").load(bronze_folder)
    written_count = df_verify.count()

    print(f"  Written  : {written_count:,} rows")
    print(f"  Status   : {'✓ OK' if written_count == raw_count else '✗ MISMATCH — investigate'}")

    return written_count

# COMMAND ----------

OBJECTS

# COMMAND ----------

# DBTITLE 1,Run the ingestion
# ── Run ingestion for all objects ────────────────────────────
results = {}

for obj_name, partition_col, _ in OBJECTS:
    try:
        count = ingest_to_bronze(obj_name, partition_col)
        results[obj_name] = {"status": "SUCCESS", "rows": count}
    except Exception as e:
        results[obj_name] = {"status": "FAILED", "error": str(e)}
        print(f"\n✗ FAILED: {obj_name}")
        print(f"  Error: {e}")

# ── Summary ───────────────────────────────────────────────────
print(f"\n{'='*55}")
print("BRONZE INGESTION SUMMARY")
print(f"{'='*55}")
print(f"{'Object':<20} {'Status':<10} {'Rows':>10}")
print(f"{'-'*45}")

total_rows = 0
for obj, result in results.items():
    if result["status"] == "SUCCESS":
        print(f"{obj:<20} {'✓ OK':<10} {result['rows']:>10,}")
        total_rows += result["rows"]
    else:
        print(f"{obj:<20} {'✗ FAIL':<10} {'—':>10}")
        print(f"  └─ {result['error'][:80]}")

print(f"{'-'*45}")
print(f"{'TOTAL':<20} {'':<10} {total_rows:>10,}")

# COMMAND ----------

# DBTITLE 1,Sanity checks
# ── Quick sanity checks on key tables ────────────────────────
# These are the checks you'd run after every bronze load
# to confirm data landed correctly before triggering silver

print("BRONZE SANITY CHECKS")
print("="*55)

# Check 1: bookings — most important table
bookings_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/bookings")

total          = bookings_bronze.count()
null_cargo     = bookings_bronze.filter(F.col("cargo_type").isNull()).count()
negative_teu   = bookings_bronze.filter(F.col("teu_count") < 0).count()
null_booking_id= bookings_bronze.filter(F.col("booking_id").isNull()).count()

print(f"\nbookings:")
print(f"  Total rows          : {total:,}")
print(f"  Null cargo_type     : {null_cargo:,}  {'⚠ expected ~3%' if null_cargo > 0 else ''}")
print(f"  Negative teu_count  : {negative_teu:,}  {'⚠ expected ~2%' if negative_teu > 0 else ''}")
print(f"  Null booking_id     : {null_booking_id:,}  {'✗ PROBLEM' if null_booking_id > 0 else '✓ OK'}")

# Check 2: vessels — SCD2 changes landed
vessels_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/vessels")
vessel_files   = vessels_bronze.select("_source_path").distinct().count()
print(f"\nvessels:")
print(f"  Total rows          : {vessels_bronze.count():,}")
print(f"  Distinct source files: {vessel_files}  {'✓ full + changes' if vessel_files == 2 else '⚠ check landing'}")

# Check 3: accounts — SCD2 changes landed
accounts_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/accounts")
acct_files      = accounts_bronze.select("_source_path").distinct().count()
print(f"\naccounts:")
print(f"  Total rows          : {accounts_bronze.count():,}")
print(f"  Distinct source files: {acct_files}  {'✓ full + changes' if acct_files == 2 else '⚠ check landing'}")

# Check 4: metadata columns present on all tables
print(f"\nMetadata column check (bookings sample):")
sample = bookings_bronze.select(
    "_bronze_ingested_at",
    "_source_object",
    "_source_path",
    "_bronze_year",
    "_bronze_month"
).limit(1)
display(sample)

print("\n✓ Bronze sanity checks complete")
print("  Null/negative values in bookings are EXPECTED — silver layer will handle them")
print("  If null_booking_id > 0, investigate before proceeding to silver")

# COMMAND ----------

# DBTITLE 1,Save notebook to GitHub
# ── This cell reminds you what to do after every notebook ────
print("""
NEXT STEPS AFTER RUNNING THIS NOTEBOOK:
========================================

1. Export this notebook:
   File → Export → Source File (.py)
   Save as: notebooks/01_bronze_ingestion.py
   in your local shipping-data-pipeline/ folder

2. Push to GitHub:
   git add notebooks/01_bronze_ingestion.py
   git commit -m "Add bronze ingestion notebook — all 9 objects"
   git push origin main

3. Verify in ADLS:
   Check /mnt/adls_dev_bdi/rocky/shipping_project/bronze/
   Each object folder should contain Delta files (_delta_log/ folder
   is the sign that it wrote as Delta, not just Parquet)
""")