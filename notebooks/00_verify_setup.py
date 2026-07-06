# Databricks notebook source
display(dbutils.fs.ls("/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project/landing"))

# COMMAND ----------

# Cell 3 — verify landing files are all there
import os

PROJECT_ROOT = "/mnt/commercialbdi_lob_commercialbdi_dev/rocky/shipping_project"

objects = ["accounts", "vessels", "ports", "bookings",
           "contacts", "contracts", "cargo_events", "cases", "users"]

print("Landing zone verification:")
print("=" * 50)

for obj in objects:
    path = f"{PROJECT_ROOT}/landing/{obj}"
    try:
        files = dbutils.fs.ls(path)
        for f in files:
            size_kb = round(f.size / 1024, 1)
            print(f"  ✓ {obj}/{f.name}  ({size_kb} KB)")
    except Exception as e:
        print(f"  ✗ {obj} — NOT FOUND: {e}")

print("=" * 50)
print("If all ✓ above — you are ready for bronze ingestion")