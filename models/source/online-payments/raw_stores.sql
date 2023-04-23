{{
    config(
        tags=["12hourly", "weekly"],
        on_schema_change="sync_all_columns",
        unique_key="id",
    )
}}
with base as (
SELECT
    cast(id as int64) as store_id,
    cast(name as string) as store_name,
    cast(address as string) as store_address,
    cast(city as string) as city,
    cast(country as string) as country,
    cast(created_at as timestamp) as created_at,
    cast(typology as string) as typology,
    cast(customer_id as int) as customer_id,
    row_number() over (partition by id order by created_at desc) as deduplicate
FROM 
    {{ source('sumup', 'store') }} 
)
select
   customer_id,
   store_id,
   store_name,
   store_address,
   city,
   country,
   created_at,
   typology
from
    base
where
    deduplicate = 1
    