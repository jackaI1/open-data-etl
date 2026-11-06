select p.snapshot_date from {{ ref('fct_product') }} p group by p.snapshot_date
having sum(p.ingredient_records) != (
  select count(*) from {{ source('catalogue', 'ingredients') }} i where i.snapshot_date = p.snapshot_date
) or sum(p.flagged_strengths) != (
  select count(*) from {{ source('catalogue', 'strengths') }} s where s.snapshot_date = p.snapshot_date and s.is_anomaly
)
