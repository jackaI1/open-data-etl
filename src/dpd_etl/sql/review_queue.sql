-- LIMIT caps returned rows, not scanned bytes. The underlying view binds the day.
SELECT snapshot_date, drug_code, din, brand_name, company_name,
       ingredient_rows, flagged_strengths, unscored_strengths
FROM `{{dataset}}.reviewer_summary_{{day}}`
WHERE flagged_strengths > 0 OR unscored_strengths > 0
ORDER BY flagged_strengths DESC, unscored_strengths DESC, drug_code
LIMIT 100
