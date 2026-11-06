select {{ product_key('snapshot_date', 'left_id') }} || ':' || cast(right_id as {{ dbt.type_string() }}) as pair_key,
       {{ product_key('snapshot_date', 'left_id') }} as left_product_key,
       {{ product_key('snapshot_date', 'right_id') }} as right_product_key,
       snapshot_date, score, is_match, reason
from {{ source('catalogue', 'pairs') }}
