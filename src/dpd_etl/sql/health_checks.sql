-- Separate aggregates prevent child joins from multiplying counts.
WITH products AS (
 SELECT COUNT(*) AS rows_processed,
   COUNTIF(din IS NULL) AS null_din,
   COUNTIF(brand_name IS NULL) AS null_brand,
   COUNTIF(company_name IS NULL) AS null_company
 FROM `{{dataset}}.drug_products`
 WHERE snapshot_date = CAST(@snapshot_date AS DATE)
), ingredients AS (
 SELECT COUNT(*) AS ingredient_rows FROM `{{dataset}}.drug_ingredients`
 WHERE snapshot_date = CAST(@snapshot_date AS DATE)
), strengths AS (
 SELECT COUNT(*) AS strength_rows, COUNTIF(is_anomaly) AS flagged_strengths,
   COUNT(DISTINCT IF(is_anomaly, drug_code, NULL)) AS flagged_records,
   COUNTIF(is_evaluated AND (
     peer_count >= 8 AND peer_mad > 0
     AND ABS(0.6745 * (numeric_strength - peer_median) / NULLIF(peer_mad, 0)) > @z_threshold
   )) AS recomputed_strength_flags
 FROM `{{dataset}}.strength_anomalies`
 WHERE snapshot_date = CAST(@snapshot_date AS DATE)
), pairs AS (
 SELECT COUNT(*) AS candidate_pairs, COUNTIF(is_match) AS duplicate_pairs,
   COUNTIF(reason IN ('exact_din', 'fuzzy_match')) AS recomputed_duplicate_pairs
 FROM `{{dataset}}.product_links`
 WHERE snapshot_date = CAST(@snapshot_date AS DATE)
)
SELECT p.rows_processed, i.ingredient_rows, s.strength_rows, s.flagged_strengths,
 s.flagged_records, s.recomputed_strength_flags, d.candidate_pairs, d.duplicate_pairs,
 d.recomputed_duplicate_pairs,
 SAFE_DIVIDE(p.null_din, p.rows_processed) AS null_rate_din,
 SAFE_DIVIDE(p.null_brand, p.rows_processed) AS null_rate_brand_name,
 SAFE_DIVIDE(p.null_company, p.rows_processed) AS null_rate_company_name
FROM products p CROSS JOIN ingredients i CROSS JOIN strengths s CROSS JOIN pairs d
