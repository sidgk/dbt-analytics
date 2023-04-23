{{
    config(
        tags=["payments", "12hourly", "weekly"],
        on_schema_change="sync_all_columns",
        unique_key="id",
    )
}}
with
    base as (
        select
            cast(id as int64) as device_id,
            cast(type as string) as device_type,
            cast(store_id as int64) as store_id,
            row_number() over (partition by id order by id desc) as deduplicate
        from {{ source("sumup", "devices") }}
    )
select 
    device_id, 
    device_type, 
    store_id
from 
    base
where 
    deduplicate = 1
