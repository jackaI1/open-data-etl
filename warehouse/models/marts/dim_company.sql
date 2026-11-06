select company_key, snapshot_date, company_code, company_name from (
  select *, row_number() over (partition by company_key order by company_name, drug_code) as rn
  from {{ ref('stg_products') }}
) ranked where rn = 1
