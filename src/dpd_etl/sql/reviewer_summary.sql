-- One row per product. Count children before joining to avoid multiplying flags.
-- Every scan has a literal date filter, including scans underneath the view.
WITH ingredients AS (
  SELECT drug_code, COUNT(*) AS ingredient_rows
  FROM `{{dataset}}.drug_ingredients`
  WHERE snapshot_date = DATE '{{snapshot}}' AND load_id = '{{load_id}}'
  GROUP BY drug_code
), flags AS (
  SELECT drug_code,
    COUNTIF(is_anomaly) AS flagged_strengths,
    COUNTIF(NOT is_evaluated) AS unscored_strengths
  FROM `{{dataset}}.strength_anomalies`
  WHERE snapshot_date = DATE '{{snapshot}}' AND load_id = '{{load_id}}'
  GROUP BY drug_code
)
SELECT p.snapshot_date, p.drug_code, p.din, p.brand_name, p.company_name,
  p.current_status, p.atc_group,
  COALESCE(i.ingredient_rows, 0) AS ingredient_rows,
  COALESCE(f.flagged_strengths, 0) AS flagged_strengths,
  COALESCE(f.unscored_strengths, 0) AS unscored_strengths
FROM `{{dataset}}.drug_products` AS p
LEFT JOIN ingredients AS i ON p.drug_code = i.drug_code
LEFT JOIN flags AS f ON p.drug_code = f.drug_code
WHERE p.snapshot_date = DATE '{{snapshot}}' AND p.load_id = '{{load_id}}'
