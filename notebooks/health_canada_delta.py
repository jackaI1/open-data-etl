# Databricks notebook source
# MAGIC %md
# MAGIC # Keep a dated drug catalogue and review unusual strengths
# MAGIC I pass source, output and date parameters to reuse the same job for each download.
# MAGIC The package must be installed on the compute first. See docs/delta.md.

# COMMAND ----------
import json

from dpd_etl.delta_job import get_delta_spark, parameters, run_delta

if "dbutils" in globals():
    for name, default in {
        "raw_dir": "", "delta_root": "", "snapshot_date": "", "report_dir": "",
        "fixture": "false",
    }.items():
        dbutils.widgets.text(name, default)  # noqa: F821
    params = {name: dbutils.widgets.get(name) for name in (  # noqa: F821
        "raw_dir", "delta_root", "snapshot_date", "report_dir", "fixture",
    )}
    if any(not params[key].strip() for key in ("raw_dir", "delta_root", "report_dir")):
        raise ValueError("Set raw_dir, delta_root and report_dir job parameters")
    if params["fixture"].lower() not in ("true", "false"):
        raise ValueError("fixture must be true or false")
    params["fixture"] = params["fixture"].lower() == "true"
else:
    params = parameters()

# COMMAND ----------
owns_spark = "spark" not in globals()
if owns_spark:
    spark = get_delta_spark()
try:
    summary = run_delta(spark, **params)
    print(json.dumps(summary, indent=2))
    # The SQL view is created from the two saved Delta tables by run_delta.
    spark.sql("""
        SELECT snapshot_date, drug_code, brand_name, ingredient, strength,
               strength_unit, robust_z
        FROM flagged_strengths
        WHERE snapshot_date = :snapshot_date
        ORDER BY abs(robust_z) DESC, drug_code, ingredient
        LIMIT 20
    """, args={"snapshot_date": params["snapshot_date"]}).show(truncate=False)
finally:
    if owns_spark:
        spark.stop()
