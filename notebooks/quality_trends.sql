-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Compare quality checks across saved dates
-- MAGIC Load the gold_quality Delta table as a temporary view before running this cell.
-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW quality_trends AS
SELECT snapshot_date, raw_rows, raw_duplicate_rows, cleaned_products,
       gold_products, ingredient_records, flagged_strengths,
       flagged_strengths - LAG(flagged_strengths) OVER (ORDER BY snapshot_date)
         AS flagged_change
FROM gold_quality
