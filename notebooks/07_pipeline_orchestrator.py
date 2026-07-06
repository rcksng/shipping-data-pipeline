# Databricks notebook source
# DBTITLE 1,Configuration
# ============================================================
# 07_PIPELINE_ORCHESTRATOR
# Purpose : Master controller — runs all pipeline notebooks
#           in dependency order using dbutils.notebook.run()
#
# Execution order and dependencies:
#   01_bronze_ingestion    (no dependency — reads from landing)
#         ↓ must succeed
#   02_silver_cleaning     (depends on bronze)
#         ↓ must succeed
#   03_silver_scd2         (depends on silver cleaning)
#         ↓ must succeed
#   04_gold_dimensions     (depends on silver SCD2)
#         ↓ must succeed
#   05_gold_facts          (depends on gold dimensions)
#         ↓ must succeed
#   06_quality_checks      (depends on gold facts)
#
# If ANY notebook fails → exception raised → pipeline stops
# → Databricks Workflows marks the job as Failed
# → downstream notebooks never execute
# → bad data never reaches gold or Power BI
# ============================================================

import json
from datetime import datetime

# ── Replace with your actual notebook paths ───────────────────
# In Databricks: right-click any notebook → Copy Path
# Looks like: /Users/your.email@maersk.com/notebook_name

BASE_PATH = "/Workspace/Users/rocky.singh@maersk.com/Projects/DE Project"   # ← replace this

NOTEBOOKS = {
    "01_bronze_ingestion":  f"{BASE_PATH}/01_bronze_ingestion",
    "02_silver_cleaning":   f"{BASE_PATH}/02_silver_cleaning",
    "03_silver_scd2":       f"{BASE_PATH}/03_silver_scd2",
    "04_gold_dimensions":   f"{BASE_PATH}/04_gold_dimensions",
    "05_gold_facts":        f"{BASE_PATH}/05_gold_facts",
    "06_quality_checks":    f"{BASE_PATH}/06_quality_checks",
}

# Timeout per notebook in seconds
# 1800 = 30 minutes max per notebook before Workflows kills it
TIMEOUT_SECONDS = 1800

# Pipeline run metadata
PIPELINE_RUN_ID  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
PIPELINE_START   = datetime.utcnow()

print(f"Pipeline orchestrator initialised")
print(f"Run ID : {PIPELINE_RUN_ID}")
print(f"Start  : {PIPELINE_START.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"\nNotebooks to execute ({len(NOTEBOOKS)}):")
for name, path in NOTEBOOKS.items():
    print(f"  {name}")

# COMMAND ----------

# DBTITLE 1,The orchestration engine
# ── Orchestration engine ──────────────────────────────────────

def run_notebook(step_name, notebook_path, params={}):
    """
    Executes a single notebook using dbutils.notebook.run().
    Records timing and result.
    Raises exception on failure — stops the pipeline.

    Why dbutils.notebook.run() and not %run?
      %run executes inline in the same session — variables bleed
      across notebooks and errors are harder to isolate.
      dbutils.notebook.run() executes in a separate context,
      returns an exit value, and respects the timeout.
      This is the production pattern.
    """
    step_start = datetime.utcnow()
    print(f"\n{'='*55}")
    print(f"  STARTING : {step_name}")
    print(f"  Path     : {notebook_path}")
    print(f"  Time     : {step_start.strftime('%H:%M:%S')} UTC")
    print(f"{'='*55}")

    try:
        # dbutils.notebook.run() returns the exit value
        # set by dbutils.notebook.exit() in the called notebook
        # If the notebook raises an exception → this raises too
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=TIMEOUT_SECONDS,
            arguments=params       # pass parameters if needed
        )

        step_end      = datetime.utcnow()
        duration_secs = (step_end - step_start).seconds
        duration_mins = round(duration_secs / 60, 1)

        print(f"\n  ✓ COMPLETED : {step_name}")
        print(f"  Duration   : {duration_mins} mins ({duration_secs} secs)")
        print(f"  Exit value : {result}")

        return {
            "step":     step_name,
            "status":   "SUCCESS",
            "start":    step_start.strftime("%H:%M:%S"),
            "duration": f"{duration_mins} mins",
            "result":   result
        }

    except Exception as e:
        step_end      = datetime.utcnow()
        duration_secs = (step_end - step_start).seconds

        print(f"\n  ✗ FAILED    : {step_name}")
        print(f"  Duration   : {duration_secs} secs")
        print(f"  Error      : {str(e)[:300]}")

        # Re-raise — this stops the orchestrator
        # Databricks Workflows marks the job as Failed
        # All downstream notebooks are skipped
        raise Exception(
            f"Pipeline failed at step [{step_name}] "
            f"after {duration_secs} secs. "
            f"Error: {str(e)[:200]}"
        )

print("✓ Orchestration engine ready")
print("""
Key concept — why re-raise the exception:
  When 02_silver_cleaning fails, there is no point running
  03_silver_scd2 — it would run on incomplete silver data
  and produce a corrupted gold layer.

  Re-raising stops everything immediately.
  The pipeline is in a known bad state — better to halt
  than to continue and silently corrupt downstream tables.
""")

# COMMAND ----------

# DBTITLE 1,Execute the pipeline
# ── Execute all notebooks in dependency order ─────────────────

execution_log = []
pipeline_failed = False

# Parameters passed to each notebook
# Add any runtime parameters here — e.g. load date, env flag
PARAMS = {
    "pipeline_run_id": PIPELINE_RUN_ID,
    "triggered_by":    "07_pipeline_orchestrator"
}

print(f"PIPELINE EXECUTION STARTING")
print(f"Run ID : {PIPELINE_RUN_ID}")
print(f"{'='*55}")

try:
    # ── Step 1: Bronze ingestion ──────────────────────────────
    result = run_notebook(
        "01_bronze_ingestion",
        NOTEBOOKS["01_bronze_ingestion"],
        PARAMS
    )
    execution_log.append(result)

    # ── Step 2: Silver cleaning ───────────────────────────────
    # Only runs if bronze succeeded
    result = run_notebook(
        "02_silver_cleaning",
        NOTEBOOKS["02_silver_cleaning"],
        PARAMS
    )
    execution_log.append(result)

    # ── Step 3: Silver SCD2 ───────────────────────────────────
    # Only runs if silver cleaning succeeded
    result = run_notebook(
        "03_silver_scd2",
        NOTEBOOKS["03_silver_scd2"],
        PARAMS
    )
    execution_log.append(result)

    # ── Step 4: Gold dimensions ───────────────────────────────
    # Only runs if SCD2 succeeded
    result = run_notebook(
        "04_gold_dimensions",
        NOTEBOOKS["04_gold_dimensions"],
        PARAMS
    )
    execution_log.append(result)

    # ── Step 5: Gold facts ────────────────────────────────────
    # Only runs if dimensions succeeded
    result = run_notebook(
        "05_gold_facts",
        NOTEBOOKS["05_gold_facts"],
        PARAMS
    )
    execution_log.append(result)

    # ── Step 6: Quality checks ────────────────────────────────
    # Only runs if facts succeeded
    # If quality checks raise a FATAL → pipeline marked Failed
    result = run_notebook(
        "06_quality_checks",
        NOTEBOOKS["06_quality_checks"],
        PARAMS
    )
    execution_log.append(result)

except Exception as e:
    pipeline_failed = True
    pipeline_error  = str(e)
    print(f"\n{'='*55}")
    print(f"PIPELINE HALTED")
    print(f"{'='*55}")
    print(f"Error: {pipeline_error[:500]}")

# COMMAND ----------

# DBTITLE 1,Pipeline summary
# ── Pipeline run summary ──────────────────────────────────────

PIPELINE_END      = datetime.utcnow()
total_duration    = (PIPELINE_END - PIPELINE_START).seconds
total_mins        = round(total_duration / 60, 1)

print(f"\n{'='*55}")
print(f"PIPELINE RUN SUMMARY")
print(f"{'='*55}")
print(f"Run ID     : {PIPELINE_RUN_ID}")
print(f"Started    : {PIPELINE_START.strftime('%H:%M:%S')} UTC")
print(f"Ended      : {PIPELINE_END.strftime('%H:%M:%S')} UTC")
print(f"Duration   : {total_mins} mins")
print(f"Status     : {'✓ SUCCESS' if not pipeline_failed else '✗ FAILED'}")
print(f"\nStep results:")
print(f"  {'Step':<30} {'Status':<10} {'Duration'}")
print(f"  {'-'*55}")

for step in execution_log:
    icon = "✓" if step["status"] == "SUCCESS" else "✗"
    print(f"  {icon} {step['step']:<28} {step['status']:<10} {step['duration']}")

# Steps that didn't run (because a previous step failed)
completed_steps = {s["step"] for s in execution_log}
all_steps       = list(NOTEBOOKS.keys())

for step in all_steps:
    if step not in completed_steps:
        print(f"  — {step:<28} {'SKIPPED':<10} (upstream failure)")

print(f"{'='*55}")

# If pipeline failed — re-raise so Workflows marks job as Failed
if pipeline_failed:
    raise Exception(
        f"Pipeline {PIPELINE_RUN_ID} failed. "
        f"Check step logs above for details."
    )
else:
    print(f"\n✓ All {len(execution_log)} steps completed successfully")
    print(f"  Gold layer is ready for Power BI consumption")

    # Signal success to Databricks Workflows
    dbutils.notebook.exit(json.dumps({
        "run_id":    PIPELINE_RUN_ID,
        "status":    "SUCCESS",
        "steps":     len(execution_log),
        "duration":  f"{total_mins} mins"
    }))

# COMMAND ----------

# File → Export → Source File (.py)
# Save as: notebooks/07_pipeline_orchestrator.py

# git add notebooks/07_pipeline_orchestrator.py
# git commit -m "Add pipeline orchestrator — Databricks Workflows + ADF setup"
# git push origin main