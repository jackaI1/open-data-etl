# Databricks notebook source
"""Run the same bronze/silver/gold job in a notebook or from the command line."""
# COMMAND ----------
import json
from datetime import date

from dpd_etl.delta_job import get_delta_spark, parameters
from dpd_etl.lakehouse import run_lakehouse

# COMMAND ----------
if "dbutils" in globals():
    widgets = globals()["dbutils"].widgets
    defaults = {"raw_dir": "", "delta_root": "", "snapshot_date": "", "report_dir": "", "fixture": "false"}
    for key, value in defaults.items():
        widgets.text(key, value)
    params = {key: widgets.get(key) for key in defaults}
    params["fixture"] = params["fixture"].lower() == "true"
else:
    params = parameters()

# COMMAND ----------
owns_spark = "spark" not in globals()
if owns_spark:
    spark = get_delta_spark()
try:
    # An explicit date also permits replay of a pinned historical snapshot.
    summary = run_lakehouse(spark, **params, as_of=date.fromisoformat(params["snapshot_date"]))
    print(json.dumps(summary, indent=2))
    spark.sql("SELECT * FROM rpt_catalogue ORDER BY snapshot_date, drug_class").show()
    spark.sql("SELECT * FROM dosage_cohorts ORDER BY snapshot_date, dosage_form").show()
finally:
    if owns_spark:
        spark.stop()
