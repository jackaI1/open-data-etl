select distinct ingredient_key, ingredient from {{ ref('stg_ingredients') }}
