select *, {{ product_key() }} as product_key,
{{ dbt.hash("ingredient") }} as ingredient_key,
{{ dbt.hash("coalesce(ingredient, '') || '|' || coalesce(strength, '') || '|' || coalesce(strength_unit, '')") }} as strength_key
from {{ source('catalogue', 'ingredients') }}
