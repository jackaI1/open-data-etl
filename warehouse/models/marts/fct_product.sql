select p.product_key, p.company_key, p.snapshot_date, p.drug_class,
       coalesce(i.ingredient_records, 0) as ingredient_records,
       coalesce(s.flagged_strengths, 0) as flagged_strengths
from {{ ref('dim_product') }} p
left join (
  select product_key, count(*) as ingredient_records
  from {{ ref('stg_ingredients') }} group by product_key
) i on p.product_key = i.product_key
left join (
  select {{ product_key() }} as product_key,
         sum(case when is_anomaly then 1 else 0 end) as flagged_strengths
  from {{ source('catalogue', 'strengths') }} group by snapshot_date, drug_code
) s on p.product_key = s.product_key
