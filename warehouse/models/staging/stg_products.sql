select *, {{ product_key() }} as product_key, {{ company_key() }} as company_key,
case when length(din) = 8
  and replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    din, '0', ''), '1', ''), '2', ''), '3', ''), '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', '') = ''
  then 'valid' else 'invalid' end as din_status
from {{ source('catalogue', 'products') }}
