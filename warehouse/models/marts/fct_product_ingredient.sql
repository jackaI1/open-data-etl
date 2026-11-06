select product_key || ':' || strength_key as product_ingredient_key,
       product_key, ingredient_key, snapshot_date, strength, strength_unit
from {{ ref('stg_ingredients') }}
