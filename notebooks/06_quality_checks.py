# Databricks notebook source
# DBTITLE 1,Configuration and check framework
# ============================================================
# 06_QUALITY_CHECKS
# Purpose : Automated data quality assertions across all layers
#
# Check severity levels:
#   FATAL   → raises exception → stops pipeline → ADF marks Failed
#   WARNING → logs clearly → pipeline continues → review next day
#
# Results written to:
#   gold/dq_results_log  (Delta table — queryable history)
#   Notebook output      (visible in current run)
# ============================================================

MOUNT       = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"
BRONZE_PATH = f"{MOUNT}/bronze"
SILVER_PATH = f"{MOUNT}/silver"
GOLD_PATH   = f"{MOUNT}/gold"
DQ_LOG_PATH = f"{GOLD_PATH}/dq_results_log"

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# ── Check result collector ────────────────────────────────────
# Every check appends a result dict here
# At the end we write all results to Delta in one shot
check_results = []

# Track run metadata
RUN_ID        = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
RUN_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Quality check run started")
print(f"Run ID    : {RUN_ID}")
print(f"Timestamp : {RUN_TIMESTAMP}")
print(f"DQ Log    : {DQ_LOG_PATH}")

# COMMAND ----------

# DBTITLE 1,The check engine
# ── Check engine — two functions you'll use throughout ────────

def run_check(
    layer,          # "bronze", "silver", "gold"
    table,          # table name e.g. "fact_bookings"
    check_name,     # short identifier e.g. "no_null_booking_id"
    description,    # human-readable e.g. "booking_id must never be null"
    severity,       # "FATAL" or "WARNING"
    passed,         # Boolean — did the check pass?
    actual_value,   # what we measured e.g. 23 (number of nulls)
    expected_value, # what we expected e.g. 0
    unit=""         # e.g. "rows", "%", "days"
):
    """
    Records a check result, prints it, appends to results list.
    Does NOT raise exception here — fatal handling is separate.
    """
    status = "PASS" if passed else ("FATAL" if severity == "FATAL" else "WARNING")

    icon = "✓" if passed else ("✗" if severity == "FATAL" else "⚠")

    result = {
        "run_id":         RUN_ID,
        "run_timestamp":  RUN_TIMESTAMP,
        "layer":          layer,
        "table":          table,
        "check_name":     check_name,
        "description":    description,
        "severity":       severity,
        "status":         status,
        "actual_value":   str(actual_value),
        "expected_value": str(expected_value),
        "unit":           unit,
    }

    check_results.append(result)

    # Print with alignment
    print(f"  {icon} [{severity:<7}] [{layer:<6}] {table}.{check_name}")
    if not passed:
        print(f"      Expected: {expected_value} {unit}")
        print(f"      Actual  : {actual_value} {unit}")

    return passed


def assert_fatal(passed, check_name, actual_value, expected_value):
    """
    Call this after run_check for FATAL checks.
    Raises exception if check failed — stops pipeline,
    ADF marks run as Failed, alert is triggered.
    """
    if not passed:
        raise Exception(
            f"FATAL quality check failed: {check_name} | "
            f"Expected: {expected_value} | Actual: {actual_value} | "
            f"Run ID: {RUN_ID} — pipeline halted to prevent bad data reaching gold"
        )


print("✓ Check engine ready")
print("""
How checks work:
  run_check()    → records result, prints status, returns True/False
  assert_fatal() → if check failed, raises exception → stops pipeline

Pattern for every FATAL check:
  passed = run_check(..., severity="FATAL", passed=some_condition, ...)
  assert_fatal(passed, ...)

Pattern for every WARNING check:
  run_check(..., severity="WARNING", passed=some_condition, ...)
  # no assert — pipeline continues regardless
""")

# COMMAND ----------

# DBTITLE 1,Bronze layer checks
# ══════════════════════════════════════════════════════════════
# BRONZE LAYER CHECKS
# What we're verifying: did all data land correctly from landing?
# Bronze checks are mostly about completeness and row counts.
# ══════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("BRONZE LAYER CHECKS")
print("="*60)

# ── Expected minimum row counts per object ────────────────────
# If row count drops below minimum → something went wrong in ingestion
# Set at 80% of generated counts to allow for reruns with different seeds

BRONZE_MIN_ROWS = {
    "users":        50,
    "ports":        15,
    "vessels":      40,
    "accounts":     300,
    "contacts":     500,
    "contracts":    400,
    "bookings":     1500,
    "cargo_events": 3000,
    "cases":        600,
}

for table_name, min_rows in BRONZE_MIN_ROWS.items():
    df    = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")
    count = df.count()

    passed = run_check(
        layer="bronze",
        table=table_name,
        check_name="minimum_row_count",
        description=f"Table must have at least {min_rows:,} rows",
        severity="FATAL",
        passed=count >= min_rows,
        actual_value=count,
        expected_value=f">= {min_rows}",
        unit="rows"
    )
    assert_fatal(passed, f"bronze.{table_name}.minimum_row_count", count, min_rows)

# ── Bronze metadata columns present ──────────────────────────
# Every bronze table must have the 3 metadata columns
# If missing → ingestion notebook was modified incorrectly

df_bookings_b = spark.read.format("delta").load(f"{BRONZE_PATH}/bookings")
required_meta = ["_bronze_ingested_at", "_source_object", "_source_path"]

for col_name in required_meta:
    col_present = col_name in df_bookings_b.columns

    passed = run_check(
        layer="bronze",
        table="bookings",
        check_name=f"metadata_col_{col_name}",
        description=f"Bronze metadata column {col_name} must exist",
        severity="FATAL",
        passed=col_present,
        actual_value="present" if col_present else "MISSING",
        expected_value="present"
    )
    assert_fatal(passed, f"bronze.bookings.{col_name}", col_present, True)

# ── No completely empty files landed ─────────────────────────
# If a source file had 0 bytes, Spark reads 0 rows
# This is different from the min_rows check — catches silent empty loads

for table_name in BRONZE_MIN_ROWS.keys():
    df    = spark.read.format("delta").load(f"{BRONZE_PATH}/{table_name}")
    count = df.count()

    passed = run_check(
        layer="bronze",
        table=table_name,
        check_name="not_empty",
        description="Table must not be empty",
        severity="FATAL",
        passed=count > 0,
        actual_value=count,
        expected_value="> 0",
        unit="rows"
    )
    assert_fatal(passed, f"bronze.{table_name}.not_empty", count, "> 0")

print(f"\n  Bronze checks complete.")

# COMMAND ----------

# DBTITLE 1,Silver layer checks
# ══════════════════════════════════════════════════════════════
# SILVER LAYER CHECKS
# What we're verifying:
#   - Cleaning rules were applied correctly
#   - Data quality issues are within acceptable bounds
#   - SCD2 integrity holds on all three dimensions
# ══════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("SILVER LAYER CHECKS")
print("="*60)

# ── bookings: null rate on cargo_type ─────────────────────────
# After cleaning, cargo_type should have 0 nulls
# (nulls were replaced with "Unknown" in silver cleaning)
# If nulls exist → cleaning notebook has a bug

df_bkg = spark.read.format("delta").load(f"{SILVER_PATH}/bookings")
total  = df_bkg.count()
null_cargo = df_bkg.filter(F.col("cargo_type").isNull()).count()

passed = run_check(
    layer="silver",
    table="bookings",
    check_name="no_null_cargo_type",
    description="cargo_type nulls must be 0 — cleaned to 'Unknown' in silver",
    severity="FATAL",
    passed=null_cargo == 0,
    actual_value=null_cargo,
    expected_value=0,
    unit="null rows"
)
assert_fatal(passed, "silver.bookings.no_null_cargo_type", null_cargo, 0)

# ── bookings: no negative teu_count ──────────────────────────
# After cleaning, abs() was applied — no negatives should remain

negative_teu = df_bkg.filter(F.col("teu_count") < 0).count()

passed = run_check(
    layer="silver",
    table="bookings",
    check_name="no_negative_teu",
    description="teu_count must be >= 0 after abs() applied in cleaning",
    severity="FATAL",
    passed=negative_teu == 0,
    actual_value=negative_teu,
    expected_value=0,
    unit="negative rows"
)
assert_fatal(passed, "silver.bookings.no_negative_teu", negative_teu, 0)

# ── bookings: Unknown cargo rate warning ──────────────────────
# "Unknown" cargo_type is expected (~3%) but if it spikes
# it may indicate a source system problem worth investigating

unknown_cargo = df_bkg.filter(F.col("cargo_type") == "Unknown").count()
unknown_pct   = round(100 * unknown_cargo / total, 2)

run_check(
    layer="silver",
    table="bookings",
    check_name="unknown_cargo_rate",
    description="Unknown cargo_type rate should stay below 10%",
    severity="WARNING",
    passed=unknown_pct <= 10.0,
    actual_value=unknown_pct,
    expected_value="<= 10",
    unit="%"
)
# No assert_fatal — WARNING continues pipeline

# ── bookings: booking_id uniqueness ──────────────────────────
# Every booking must have a unique ID — duplicates break fact grain

total_bkg    = df_bkg.count()
distinct_bkg = df_bkg.select("booking_id").distinct().count()
duplicates   = total_bkg - distinct_bkg

passed = run_check(
    layer="silver",
    table="bookings",
    check_name="booking_id_unique",
    description="booking_id must be unique — no duplicates allowed",
    severity="FATAL",
    passed=duplicates == 0,
    actual_value=duplicates,
    expected_value=0,
    unit="duplicate rows"
)
assert_fatal(passed, "silver.bookings.booking_id_unique", duplicates, 0)

# ── bookings: booking_date not null ──────────────────────────
null_dates = df_bkg.filter(F.col("booking_date").isNull()).count()

passed = run_check(
    layer="silver",
    table="bookings",
    check_name="no_null_booking_date",
    description="booking_date must never be null — needed for date_sk join",
    severity="FATAL",
    passed=null_dates == 0,
    actual_value=null_dates,
    expected_value=0,
    unit="null rows"
)
assert_fatal(passed, "silver.bookings.no_null_booking_date", null_dates, 0)

# ── SCD2 integrity: dim_accounts ─────────────────────────────
# Each account_id must have exactly ONE is_current=True row
# More than one = SCD2 is broken, joins will duplicate fact rows

df_accts = spark.read.format("delta").load(f"{SILVER_PATH}/dim_accounts")

duplicate_current = (df_accts
    .filter(F.col("is_current") == True)
    .groupBy("account_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

passed = run_check(
    layer="silver",
    table="dim_accounts",
    check_name="scd2_single_current_row",
    description="Each account_id must have exactly one is_current=True row",
    severity="FATAL",
    passed=duplicate_current == 0,
    actual_value=duplicate_current,
    expected_value=0,
    unit="accounts with duplicate current rows"
)
assert_fatal(passed, "silver.dim_accounts.scd2_integrity",
             duplicate_current, 0)

# ── SCD2 integrity: valid_from must be before valid_to ────────
invalid_dates = (df_accts
    .filter(
        F.col("valid_to").isNotNull() &
        (F.col("valid_from") >= F.col("valid_to"))
    )
    .count()
)

passed = run_check(
    layer="silver",
    table="dim_accounts",
    check_name="scd2_valid_date_range",
    description="valid_from must be strictly before valid_to",
    severity="FATAL",
    passed=invalid_dates == 0,
    actual_value=invalid_dates,
    expected_value=0,
    unit="rows with invalid date range"
)
assert_fatal(passed, "silver.dim_accounts.scd2_date_range",
             invalid_dates, 0)

# ── SCD2 integrity: vessels and ports ────────────────────────
for dim_name in ["dim_vessels", "dim_ports"]:
    natural_key = "vessel_id" if dim_name == "dim_vessels" else "port_id"
    df_dim      = spark.read.format("delta").load(f"{SILVER_PATH}/{dim_name}")

    dup = (df_dim
        .filter(F.col("is_current") == True)
        .groupBy(natural_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    passed = run_check(
        layer="silver",
        table=dim_name,
        check_name="scd2_single_current_row",
        description=f"Each {natural_key} must have exactly one is_current=True row",
        severity="FATAL",
        passed=dup == 0,
        actual_value=dup,
        expected_value=0,
        unit="entities with duplicate current rows"
    )
    assert_fatal(passed, f"silver.{dim_name}.scd2_integrity", dup, 0)

# ── contracts: end_date after start_date ─────────────────────
df_ctr = spark.read.format("delta").load(f"{SILVER_PATH}/contracts")
bad_dates = (df_ctr
    .filter(
        F.col("end_date").isNotNull() &
        (F.col("end_date") <= F.col("start_date"))
    )
    .count()
)

run_check(
    layer="silver",
    table="contracts",
    check_name="end_date_after_start_date",
    description="Contract end_date must be after start_date",
    severity="WARNING",
    passed=bad_dates == 0,
    actual_value=bad_dates,
    expected_value=0,
    unit="rows with invalid date range"
)

# ── cases: customer_rating in valid range ─────────────────────
df_cases = spark.read.format("delta").load(f"{SILVER_PATH}/cases")
bad_rating = (df_cases
    .filter(
        F.col("customer_rating").isNotNull() &
        ((F.col("customer_rating") < 1) | (F.col("customer_rating") > 5))
    )
    .count()
)

passed = run_check(
    layer="silver",
    table="cases",
    check_name="customer_rating_range",
    description="customer_rating must be between 1 and 5",
    severity="FATAL",
    passed=bad_rating == 0,
    actual_value=bad_rating,
    expected_value=0,
    unit="out-of-range rows"
)
assert_fatal(passed, "silver.cases.customer_rating_range", bad_rating, 0)

print(f"\n  Silver checks complete.")

# COMMAND ----------

# DBTITLE 1,Gold layer checks
# ══════════════════════════════════════════════════════════════
# GOLD LAYER CHECKS
# What we're verifying:
#   - Fact grain integrity (no duplicates)
#   - Dimension join coverage (SK resolution rate)
#   - Measure validity (no negative revenue)
#   - dim_date completeness (all fact dates have a date row)
# ══════════════════════════════════════════════════════════════

print("\n" + "="*60)
print("GOLD LAYER CHECKS")
print("="*60)

df_fact_bkg   = spark.read.format("delta").load(f"{GOLD_PATH}/fact_bookings")
df_fact_evt   = spark.read.format("delta").load(f"{GOLD_PATH}/fact_cargo_events")
df_fact_cases = spark.read.format("delta").load(f"{GOLD_PATH}/fact_cases")
df_dim_date   = spark.read.format("delta").load(f"{GOLD_PATH}/dim_date")

# ── fact_bookings: grain integrity ────────────────────────────
# One row per booking_id — no duplicates allowed
total_fact   = df_fact_bkg.count()
distinct_bkg = df_fact_bkg.select("booking_id").distinct().count()
grain_dupes  = total_fact - distinct_bkg

passed = run_check(
    layer="gold",
    table="fact_bookings",
    check_name="grain_integrity",
    description="One row per booking_id — grain must not be broken",
    severity="FATAL",
    passed=grain_dupes == 0,
    actual_value=grain_dupes,
    expected_value=0,
    unit="duplicate booking_ids"
)
assert_fatal(passed, "gold.fact_bookings.grain_integrity",
             grain_dupes, 0)

# ── fact_bookings: account_sk resolution rate ─────────────────
# SCD2 join should resolve > 95% of bookings to a valid account_sk
# If lower → SCD2 date ranges have gaps worth investigating

unmatched_acct = df_fact_bkg.filter(F.col("account_sk").isNull()).count()
match_pct      = round(100 * (total_fact - unmatched_acct) / total_fact, 2)

passed = run_check(
    layer="gold",
    table="fact_bookings",
    check_name="account_sk_resolution",
    description="account_sk must resolve for >= 95% of bookings",
    severity="WARNING",
    passed=match_pct >= 95.0,
    actual_value=match_pct,
    expected_value=">= 95",
    unit="%"
)

# ── fact_bookings: no negative revenue ───────────────────────
negative_rev = df_fact_bkg.filter(
    F.col("total_revenue_usd").isNotNull() &
    (F.col("total_revenue_usd") < 0)
).count()

passed = run_check(
    layer="gold",
    table="fact_bookings",
    check_name="no_negative_revenue",
    description="total_revenue_usd must not be negative",
    severity="FATAL",
    passed=negative_rev == 0,
    actual_value=negative_rev,
    expected_value=0,
    unit="rows with negative revenue"
)
assert_fatal(passed, "gold.fact_bookings.no_negative_revenue",
             negative_rev, 0)

# ── fact_bookings: date_sk coverage in dim_date ───────────────
# Every date_sk in fact_bookings must exist in dim_date
# If not → Power BI time intelligence breaks silently

fact_dates = df_fact_bkg.select("date_sk").distinct()
dim_dates  = df_dim_date.select("date_sk").distinct()

unmatched_dates = fact_dates.join(dim_dates, on="date_sk", how="left_anti").count()

passed = run_check(
    layer="gold",
    table="fact_bookings",
    check_name="date_sk_in_dim_date",
    description="Every fact date_sk must exist in dim_date",
    severity="FATAL",
    passed=unmatched_dates == 0,
    actual_value=unmatched_dates,
    expected_value=0,
    unit="unmatched date_sk values"
)
assert_fatal(passed, "gold.fact_bookings.date_sk_coverage",
             unmatched_dates, 0)

# ── fact_bookings: teu_count must be positive ─────────────────
zero_teu = df_fact_bkg.filter(F.col("teu_count") <= 0).count()

passed = run_check(
    layer="gold",
    table="fact_bookings",
    check_name="positive_teu_count",
    description="teu_count must be > 0 after cleaning",
    severity="FATAL",
    passed=zero_teu == 0,
    actual_value=zero_teu,
    expected_value=0,
    unit="rows with zero or negative TEU"
)
assert_fatal(passed, "gold.fact_bookings.positive_teu",
             zero_teu, 0)

# ── fact_cargo_events: grain integrity ────────────────────────
total_evt   = df_fact_evt.count()
distinct_evt = df_fact_evt.select("event_id").distinct().count()
evt_dupes   = total_evt - distinct_evt

passed = run_check(
    layer="gold",
    table="fact_cargo_events",
    check_name="grain_integrity",
    description="One row per event_id",
    severity="FATAL",
    passed=evt_dupes == 0,
    actual_value=evt_dupes,
    expected_value=0,
    unit="duplicate event_ids"
)
assert_fatal(passed, "gold.fact_cargo_events.grain_integrity",
             evt_dupes, 0)

# ── fact_cases: grain integrity ───────────────────────────────
total_cases   = df_fact_cases.count()
distinct_cases = df_fact_cases.select("case_id").distinct().count()
case_dupes    = total_cases - distinct_cases

passed = run_check(
    layer="gold",
    table="fact_cases",
    check_name="grain_integrity",
    description="One row per case_id",
    severity="FATAL",
    passed=case_dupes == 0,
    actual_value=case_dupes,
    expected_value=0,
    unit="duplicate case_ids"
)
assert_fatal(passed, "gold.fact_cases.grain_integrity",
             case_dupes, 0)

# ── fact_cases: resolution_days non-negative ──────────────────
negative_res = df_fact_cases.filter(
    F.col("resolution_days").isNotNull() &
    (F.col("resolution_days") < 0)
).count()

passed = run_check(
    layer="gold",
    table="fact_cases",
    check_name="non_negative_resolution_days",
    description="resolution_days must be >= 0",
    severity="FATAL",
    passed=negative_res == 0,
    actual_value=negative_res,
    expected_value=0,
    unit="rows with negative resolution days"
)
assert_fatal(passed, "gold.fact_cases.resolution_days",
             negative_res, 0)

# ── dim_date: no gaps in date range ──────────────────────────
# Count expected dates vs actual dates in dim_date
# A gap means time intelligence in Power BI will break

dim_date_count  = df_dim_date.count()
expected_count  = 3653  # 2020-01-01 to 2030-12-31

run_check(
    layer="gold",
    table="dim_date",
    check_name="no_date_gaps",
    description=f"dim_date must have {expected_count:,} rows (2020–2030)",
    severity="FATAL",
    passed=dim_date_count == expected_count,
    actual_value=dim_date_count,
    expected_value=expected_count,
    unit="rows"
)

# ── Revenue reasonableness check ──────────────────────────────
# Total revenue should be > 0 and < a sensible upper bound
# Catches cases where revenue was accidentally zeroed out or exploded

total_revenue = df_fact_bkg.agg(
    F.sum("total_revenue_usd")).collect()[0][0] or 0

run_check(
    layer="gold",
    table="fact_bookings",
    check_name="revenue_reasonableness",
    description="Total revenue must be > 0 (not zeroed out)",
    severity="FATAL",
    passed=total_revenue > 0,
    actual_value=round(total_revenue, 2),
    expected_value="> 0",
    unit="USD"
)

print(f"\n  Gold checks complete.")

# COMMAND ----------

# DBTITLE 1,Write results to Delta log
# ══════════════════════════════════════════════════════════════
# WRITE ALL CHECK RESULTS TO DELTA LOG TABLE
# This is what enables quality trend analysis over time
# ══════════════════════════════════════════════════════════════

schema = StructType([
    StructField("run_id",         StringType(), False),
    StructField("run_timestamp",  StringType(), False),
    StructField("layer",          StringType(), False),
    StructField("table",          StringType(), False),
    StructField("check_name",     StringType(), False),
    StructField("description",    StringType(), True),
    StructField("severity",       StringType(), False),
    StructField("status",         StringType(), False),
    StructField("actual_value",   StringType(), True),
    StructField("expected_value", StringType(), True),
    StructField("unit",           StringType(), True),
])

df_results = spark.createDataFrame(check_results, schema)

# Append to Delta log — each run adds new rows
# Old runs are preserved — you can query history
(df_results.write
    .format("delta")
    .mode("append")           # append not overwrite — preserve history
    .option("mergeSchema", "true")
    .save(DQ_LOG_PATH))

print(f"✓ {len(check_results)} check results written to Delta log")
print(f"  Path: {DQ_LOG_PATH}")

# COMMAND ----------

# DBTITLE 1,Run summary and trend query
# ── Current run summary ───────────────────────────────────────
total_checks  = len(check_results)
passed_checks = sum(1 for r in check_results if r["status"] == "PASS")
warnings      = sum(1 for r in check_results if r["status"] == "WARNING")
fatals        = sum(1 for r in check_results if r["status"] == "FATAL")

print("\n" + "="*60)
print("QUALITY CHECK SUMMARY")
print("="*60)
print(f"  Run ID      : {RUN_ID}")
print(f"  Total checks: {total_checks}")
print(f"  ✓ Passed    : {passed_checks}")
print(f"  ⚠ Warnings  : {warnings}")
print(f"  ✗ Fatal     : {fatals}")
print(f"  Overall     : {'✓ CLEAN' if fatals == 0 and warnings == 0 else '⚠ WARNINGS PRESENT' if fatals == 0 else '✗ FATAL FAILURES'}")
print("="*60)

# ── Historical trend query ────────────────────────────────────
# This is what you show in interviews — quality tracked over time
print("\nQUALITY TREND — all runs in this project:")

df_log = spark.read.format("delta").load(DQ_LOG_PATH)

trend = (df_log
    .groupBy("run_id", "run_timestamp")
    .agg(
        F.count("*").alias("total_checks"),
        F.sum(F.when(F.col("status") == "PASS",    1).otherwise(0)).alias("passed"),
        F.sum(F.when(F.col("status") == "WARNING", 1).otherwise(0)).alias("warnings"),
        F.sum(F.when(F.col("status") == "FATAL",   1).otherwise(0)).alias("fatals"),
    )
    .orderBy("run_timestamp")
)

display(trend)

# ── Failed checks detail ──────────────────────────────────────
failed = df_log.filter(
    (F.col("run_id") == RUN_ID) &
    (F.col("status") != "PASS")
)

if failed.count() > 0:
    print("\nFailed / Warning checks this run:")
    display(failed.select(
        "severity", "layer", "table", "check_name",
        "actual_value", "expected_value", "unit"
    ).orderBy("severity"))
else:
    print("\n✓ All checks passed — no failures or warnings this run")

print("""
EXPORT + PUSH:
  File → Export → Source File (.py)
  Save as: notebooks/06_quality_checks.py

  git add notebooks/06_quality_checks.py
  git commit -m "Add quality checks notebook — fatal + warning checks across all layers"
  git push origin main
""")