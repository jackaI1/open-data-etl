{{ config(materialized='view') }}
SELECT snapshot_date, drug_class, count(*) as products,
       sum(ingredient_records) as ingredient_records,
       sum(flagged_strengths) as flagged_strengths
FROM {{ ref('fct_product') }}
GROUP BY snapshot_date, drug_class
