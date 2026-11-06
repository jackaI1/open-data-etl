-- All saved dates remain queryable. Filter snapshot_date when reviewing one run.
CREATE OR REPLACE TEMP VIEW flagged_strengths AS
SELECT
    s.snapshot_date, s.drug_code, p.din, p.brand_name, p.company_name,
    s.ingredient, s.strength, s.strength_unit,
    s.numeric_strength, s.peer_count, s.peer_median, s.peer_mad, s.robust_z, s.reason
FROM dpd_strength_anomalies s
JOIN dpd_drug_products p
  ON p.drug_code = s.drug_code AND p.snapshot_date = s.snapshot_date
WHERE s.is_anomaly = true
