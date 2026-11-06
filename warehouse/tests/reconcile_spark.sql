-- Return only differences. A date with an expected zero is still tested.
with actual as (
  select snapshot_date, 'products' as table_name, count(*) as row_count
  from {{ ref('dim_product') }} group by snapshot_date
  union all
  select snapshot_date, 'ingredients', count(*) from {{ ref('fct_product_ingredient') }} group by snapshot_date
  union all
  select snapshot_date, 'pairs', count(*) from {{ ref('fct_duplicate_pair') }} group by snapshot_date
), expected as (
  select * from {{ source('catalogue', 'manifest') }} where table_name in ('products', 'ingredients', 'pairs')
)
select e.* from expected e left join actual a
on e.snapshot_date = a.snapshot_date and e.table_name = a.table_name
where e.row_count != coalesce(a.row_count, 0)
