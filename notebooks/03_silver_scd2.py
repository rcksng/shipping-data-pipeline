# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# 03_SILVER_SCD2
# Purpose : Apply SCD Type 2 logic to slowly changing
#           dimensions — vessels, ports, accounts
#
# Input  : silver/xxx_all_versions  (cleaned, all versions)
# Output : silver/dim_xxx           (SCD2 historised dimension)
#
# SCD2 columns added:
#   valid_from   DATE     — when this version became active
#   valid_to     DATE     — when this version was superseded
#                           NULL = currently active
#   is_current   BOOLEAN  — True for the current version only
#   row_hash     STRING   — MD5 of tracked columns (change detection)
#   surrogate_sk BIGINT   — warehouse-generated unique key
# ============================================================

MOUNT       = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"
BRONZE_PATH  = f"{MOUNT}/bronze"
SILVER_PATH = f"{MOUNT}/silver"

print(f"Silver : {SILVER_PATH}")

# COMMAND ----------

# DBTITLE 1,Imports and helper function
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import pyspark.sql.types as T

def compute_hash(df, tracked_cols):
    """
    Computes MD5 hash of all tracked columns.
    Uses || separator to prevent false hash collisions.
    NULLs are coalesced to empty string before hashing
    so NULL == NULL doesn't trigger a false change.
    """
    return df.withColumn("row_hash",
        F.md5(
            F.concat_ws("||",
                *[F.coalesce(F.col(c).cast("string"), F.lit(""))
                  for c in tracked_cols]
            )
        )
    )

print("✓ Helper functions ready")

# COMMAND ----------

# DBTITLE 1,Bronze layer data
# where change and main file reside - 40 rows found.
spark.read.format("delta").load(f"{BRONZE_PATH}/accounts").display()

# COMMAND ----------

# DBTITLE 1,Silver layer data
# where change and main file reside - 80 rows found.(duplicate values) we are implementing SCD-2 with row_hashing
spark.read.format("delta").load(f"{SILVER_PATH}/accounts_all_versions").filter(F.col("account_id").isin("ACC64441431ABCC", "ACC59C2C75D1233", "ACCDCF205F2DAD8", "ACCBC8556543E1F", "ACCDAA3A2C37A28", "ACCA012470B509D", "ACCD933A1F8CCEB", "ACC938B9DB0F149", "ACCA220763123BB", "ACC7CC70F650B76", "ACC33F434AE016E", "ACC05B7DE71153E", "ACC7A4633D6DE7D", "ACC4FBDAA4A22BB", "ACCECF12BF6240F", "ACC78F4BE89B0D4", "ACC6B349845B905", "ACC19D4A5307B33", "ACCACEA24E87B09", "ACC2B78FB82CAE4", "ACC2107FA6AEFBD", "ACC54E38CF61EAC", "ACC1BE4DC5D4AFA", "ACCF93BDBBBE03A", "ACCDF4917DB61DC", "ACCD1BCABC50681", "ACC308D9E21CFFF", "ACC9B4C21FF293F", "ACC01594092CCEF", "ACC3360FA849C3A", "ACCA29469208805", "ACC6D4A914DE443", "ACCED366DD57499", "ACC6D36C9F8E67B", "ACC5CFCFB1735C8", "ACC084D36202D5C", "ACC1BE271495132", "ACC09E0DE9CFB47", "ACC85ACB6027ACC", "ACC0C068320F0B8")).orderBy(F.col("account_id").desc()).display()

# COMMAND ----------

# DBTITLE 1,SCD2 for accounts
# ── ACCOUNTS SCD2 ─────────────────────────────────────────────
#
# Tracked columns (changes to these create a new SCD2 row):
#   account_tier  → tier upgrade (Bronze→Silver→Gold) is a business event
#   region        → region reassignment affects reporting
#   country_code  → country change affects geographic analysis
#   is_active     → activation/deactivation is meaningful history
#
# NOT tracked (intentionally excluded from hash):
#   account_name      → name corrections are Type 1 (just update)
#   annual_revenue_usd → financial estimate, not a stable tracked attribute
#   last_modified_date → Salesforce metadata, changes on every edit
# ─────────────────────────────────────────────────────────────

ACCOUNTS_TRACKED = ["account_tier", "region", "country_code", "is_active"]
DIM_ACCOUNTS_PATH = f"{SILVER_PATH}/dim_accounts"

# ── Step 1: Load all versions from silver cleaning output ─────
df_all = spark.read.format("delta").load(f"{SILVER_PATH}/accounts_all_versions")

print(f"Total rows (full + changes): {df_all.count():,}")
print(f"Distinct account_ids       : {df_all.select('account_id').distinct().count():,}")

# ── Step 2: Compute row hash on all versions ──────────────────
df_hashed = compute_hash(df_all, ACCOUNTS_TRACKED)

# ── Step 3: Reconstruct history per account ───────────────────
# Each account may have multiple rows (original + 1 change)
# We need to order them by last_modified_date to know which came first
# Then assign valid_from / valid_to per version

# Window: for each account, order versions by when they were modified
w_account = Window.partitionBy("account_id").orderBy("last_modified_date")

df_versioned = (df_hashed

    # Assign version number within each account
    .withColumn("version_number",
        F.row_number().over(w_account))

    # valid_from = the date this version was modified/created
    .withColumn("valid_from",
        F.col("last_modified_date"))

    # valid_to = the day before the NEXT version's valid_from
    # For the latest version: valid_to = NULL (still current)
    .withColumn("valid_to",
        F.date_sub(
            F.lead("last_modified_date", 1)
             .over(w_account),
            1)
    )

    # is_current = True only for the latest version per account
    .withColumn("is_current",
        F.lead("last_modified_date", 1).over(w_account).isNull())

    # Surrogate key = hash of account_id + version for uniqueness
    # In production you'd use a sequence/identity column
    # Here we use a monotonically increasing ID assigned after ordering
)

# Assign surrogate key — unique integer per row
df_versioned = df_versioned.withColumn(
    "account_sk",
    F.monotonically_increasing_id()
)

# ── Step 4: Select final dimension columns ────────────────────
df_dim_accounts = df_versioned.select(
    # Surrogate key (warehouse-generated — used in fact table FKs)
    F.col("account_sk"),

    # Natural key (from source system — never used as FK)
    F.col("account_id"),

    # Tracked attributes (the ones in our hash)
    F.col("account_tier"),
    F.col("region"),
    F.col("country_code"),
    F.col("is_active"),

    # Non-tracked attributes (Type 1 — just take latest value)
    F.col("account_name"),
    F.col("industry"),
    F.col("annual_revenue_usd"),
    F.col("employee_count"),
    F.col("primary_trade_lane"),
    F.col("owner_user_id"),

    # SCD2 control columns
    F.col("valid_from"),
    F.col("valid_to"),
    F.col("is_current"),
    F.col("row_hash"),
    F.col("version_number"),
    F.col("_silver_cleaned_at")
)

# ── Step 5: Write dim_accounts ────────────────────────────────
(df_dim_accounts.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DIM_ACCOUNTS_PATH))

# ── Step 6: Verify the SCD2 output ───────────────────────────
df_verify = spark.read.format("delta").load(DIM_ACCOUNTS_PATH)

total_rows    = df_verify.count()
current_rows  = df_verify.filter(F.col("is_current") == True).count()
history_rows  = df_verify.filter(F.col("is_current") == False).count()
accounts_with_history = df_verify.groupBy("account_id") \
    .count().filter(F.col("count") > 1).count()

print(f"\ndim_accounts SCD2 results:")
print(f"  Total rows              : {total_rows:,}")
print(f"  Current rows (is_current=True)  : {current_rows:,}")
print(f"  Historical rows (is_current=False): {history_rows:,}")
print(f"  Accounts with > 1 version       : {accounts_with_history:,}")
print(f"\n  ✓ Each account_id has exactly one is_current=True row:")

# Integrity check — if any account has 2+ current rows, SCD2 is broken
duplicates = (df_verify
    .filter(F.col("is_current") == True)
    .groupBy("account_id")
    .count()
    .filter(F.col("count") > 1)
    .count())

if duplicates == 0:
    print(f"    ✓ No duplicate current rows — SCD2 integrity confirmed")
else:
    print(f"    ✗ {duplicates} accounts have duplicate current rows — INVESTIGATE")

# COMMAND ----------

df_dim_accounts.filter(F.col("account_id").isin("ACC64441431ABCC", "ACC59C2C75D1233", "ACCDCF205F2DAD8", "ACCBC8556543E1F", "ACCDAA3A2C37A28", "ACCA012470B509D", "ACCD933A1F8CCEB", "ACC938B9DB0F149", "ACCA220763123BB", "ACC7CC70F650B76", "ACC33F434AE016E", "ACC05B7DE71153E", "ACC7A4633D6DE7D", "ACC4FBDAA4A22BB", "ACCECF12BF6240F", "ACC78F4BE89B0D4", "ACC6B349845B905", "ACC19D4A5307B33", "ACCACEA24E87B09", "ACC2B78FB82CAE4", "ACC2107FA6AEFBD", "ACC54E38CF61EAC", "ACC1BE4DC5D4AFA", "ACCF93BDBBBE03A", "ACCDF4917DB61DC", "ACCD1BCABC50681", "ACC308D9E21CFFF", "ACC9B4C21FF293F", "ACC01594092CCEF", "ACC3360FA849C3A", "ACCA29469208805", "ACC6D4A914DE443", "ACCED366DD57499", "ACC6D36C9F8E67B", "ACC5CFCFB1735C8", "ACC084D36202D5C", "ACC1BE271495132", "ACC09E0DE9CFB47", "ACC85ACB6027ACC", "ACC0C068320F0B8")).orderBy(F.col("account_id").desc()).display()

# COMMAND ----------

# DBTITLE 1,SCD2 for vessels
# ── VESSELS SCD2 ──────────────────────────────────────────────
#
# Tracked columns:
#   flag_country → vessel re-flagging is a legal/compliance event
#   operator     → operator change affects service attribution
#   is_active    → vessel retirement is meaningful history
#
# NOT tracked:
#   vessel_name  → naming corrections are Type 1
#   capacity_teu → physical property, doesn't change
# ─────────────────────────────────────────────────────────────

VESSELS_TRACKED  = ["flag_country", "operator", "is_active"]
DIM_VESSELS_PATH = f"{SILVER_PATH}/dim_vessels"

df_vessels_all = spark.read.format("delta") \
    .load(f"{SILVER_PATH}/vessels_all_versions")

df_vessels_hashed = compute_hash(df_vessels_all, VESSELS_TRACKED)

w_vessel = Window.partitionBy("vessel_id").orderBy("last_updated")

df_dim_vessels = (df_vessels_hashed
    .withColumn("version_number",
        F.row_number().over(w_vessel))
    .withColumn("valid_from",
        F.col("last_updated"))
    .withColumn("valid_to",
        F.date_sub(
            F.lead("last_updated", 1).over(w_vessel), 1))
    .withColumn("is_current",
        F.lead("last_updated", 1).over(w_vessel).isNull())
    .withColumn("vessel_sk",
        F.monotonically_increasing_id())
    .select(
        "vessel_sk", "vessel_id", "imo_number", "vessel_name",
        "vessel_type", "flag_country", "operator", "capacity_teu",
        "year_built", "is_active", "home_port_code",
        "valid_from", "valid_to", "is_current",
        "row_hash", "version_number", "_silver_cleaned_at"
    )
)

(df_dim_vessels.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DIM_VESSELS_PATH))

total    = df_dim_vessels.count()
current  = df_dim_vessels.filter(F.col("is_current") == True).count()
history  = df_dim_vessels.filter(F.col("is_current") == False).count()

print(f"dim_vessels: {total:,} rows  "
      f"({current:,} current, {history:,} historical)")

# COMMAND ----------

# DBTITLE 1,SCD2 for ports
# ── PORTS SCD2 ────────────────────────────────────────────────
#
# Tracked columns:
#   port_type             → reclassification affects routing analysis
#   annual_capacity_teu   → capacity expansion is a notable event
#   is_active             → port closures matter historically
# ─────────────────────────────────────────────────────────────

PORTS_TRACKED  = ["port_type", "annual_capacity_teu", "is_active"]
DIM_PORTS_PATH = f"{SILVER_PATH}/dim_ports"

df_ports_all = spark.read.format("delta") \
    .load(f"{SILVER_PATH}/ports_all_versions")

df_ports_hashed = compute_hash(df_ports_all, PORTS_TRACKED)

w_port = Window.partitionBy("port_id").orderBy("last_updated")

df_dim_ports = (df_ports_hashed
    .withColumn("version_number",
        F.row_number().over(w_port))
    .withColumn("valid_from",
        F.col("last_updated"))
    .withColumn("valid_to",
        F.date_sub(
            F.lead("last_updated", 1).over(w_port), 1))
    .withColumn("is_current",
        F.lead("last_updated", 1).over(w_port).isNull())
    .withColumn("port_sk",
        F.monotonically_increasing_id())
    .select(
        "port_sk", "port_id", "port_code", "port_name",
        "country_code", "region", "port_type",
        "annual_capacity_teu", "is_active",
        "valid_from", "valid_to", "is_current",
        "row_hash", "version_number", "_silver_cleaned_at"
    )
)

(df_dim_ports.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DIM_PORTS_PATH))

total   = df_dim_ports.count()
current = df_dim_ports.filter(F.col("is_current") == True).count()
history = df_dim_ports.filter(F.col("is_current") == False).count()

print(f"dim_ports: {total:,} rows  "
      f"({current:,} current, {history:,} historical)")

# COMMAND ----------

# DBTITLE 1,Point-in-time query proof
# ── Prove SCD2 works — point-in-time correctness ─────────────
# Pick an account that has 2 versions (upgraded tier)
# Show that querying at different dates returns different results

print("POINT-IN-TIME CORRECTNESS PROOF")
print("="*55)

df_dim = spark.read.format("delta").load(DIM_ACCOUNTS_PATH)

# Find an account that has history (2+ versions)
accounts_with_change = (df_dim
    .groupBy("account_id", "account_name")
    .agg(F.count("*").alias("versions"))
    .filter(F.col("versions") > 1)
    .orderBy("account_id")
)

print("\nAccounts with SCD2 history (sample):")
display(accounts_with_change.limit(5))

# Pick first account with changes and show its full history
sample_id = accounts_with_change.first()["account_id"]

print(f"\nFull SCD2 history for account: {sample_id}")
display(
    df_dim
    .filter(F.col("account_id") == sample_id)
    .select("account_sk", "account_id", "account_tier",
            "region", "valid_from", "valid_to", "is_current",
            "version_number")
    .orderBy("valid_from")
)

# Now demonstrate point-in-time query
# "What tier was this account in as of 2023-01-01?"
query_date = "2023-10-01"

print(f"\nPoint-in-time query: what was the account tier on {query_date}?")
result = df_dim.filter(
    (F.col("account_id") == sample_id) &
    (F.col("valid_from") <= F.lit(query_date)) &
    (
        F.col("valid_to").isNull() |
        (F.col("valid_to") >= F.lit(query_date))
    )
).select("account_id", "account_tier", "region",
         "valid_from", "valid_to", "is_current")

display(result)
print("""
This is what makes SCD2 powerful:
  - The same account_id returns DIFFERENT tier values at different dates
  - Historical reports automatically reflect what was true AT THAT TIME
  - No overwriting of history — every version is preserved permanently
""")

# COMMAND ----------

# DBTITLE 1,Summary and export reminder
# ── Final summary ─────────────────────────────────────────────
dims = [
    ("dim_accounts", DIM_ACCOUNTS_PATH),
    ("dim_vessels",  DIM_VESSELS_PATH),
    ("dim_ports",    DIM_PORTS_PATH),
]

print("SCD2 SUMMARY")
print("="*55)
print(f"{'Dimension':<20} {'Total':>8} {'Current':>10} {'Historical':>12}")
print("-"*55)

for name, path in dims:
    df = spark.read.format("delta").load(path)
    total   = df.count()
    current = df.filter(F.col("is_current") == True).count()
    history = total - current
    print(f"{name:<20} {total:>8,} {current:>10,} {history:>12,}")

print("="*55)
print("""
EXPORT THIS NOTEBOOK:
  File → Export → Source File (.py)
  Save to: notebooks/03_silver_scd2.py

PUSH TO GITHUB:
  git add notebooks/03_silver_scd2.py
  git commit -m "Add SCD2 notebook — dim_accounts, dim_vessels, dim_ports"
  git push origin main

NEXT: 04_gold_dimensions — build dim_date, and promote
      silver dims into gold with surrogate key resolution
""")