select product_key, snapshot_date, drug_code, din, brand_name, drug_class,
       atc_group, current_status, company_key
from {{ ref('stg_products') }}
