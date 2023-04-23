{{
    config(
        tags=["payments", "2hourly", "weekly"],
        on_schema_change="sync_all_columns",
        unique_key="id",
    )
}}

with
    base as (
        select
            cast(id as int64) as transaction_id,
            cast(device_id as int64) as device_id,
            cast(product_name as string) as product_name,
            cast(product_sku as string) as product_sku,
            cast(product_name_4 as string) as product_name_4,
            cast(amount as int) as amount,
            cast(status as string) as status,
            cast(card_number as string) as card_number,
            cast(cvv as int) as cvv,
            cast(created_at as timestamp) as created_at,
            cast(happened_at as timestamp) as happened_at,
            row_number() over (partition by id order by created_at desc) as deduplicate
        from {{ source("sumup", "transactions") }}
    )
select 
    transaction_id
    ,device_id
    ,product_name
    ,product_sku
    ,product_name_4
    ,amount
    ,status
    ,card_number
    ,cvv
    ,created_at
    ,happened_at
from 
    base
where
    deduplicate = 1
    {% if target.name == "dev" %}
    -- in case of a build in dev target the dev limit is applied (see macro call below)
    -- this limits the initial select to 10 rows in order to speed up local build
    -- times.
    -- If you want to disable this feature in dev, please add "--var
    -- 'dev_limit_enabled: false'" at the end of the CL build in dbt.
    -- Example: dbt run --model <model.sql> --var 'dev_limit_enabled: false'
    -- this limit will be entirely ignored in production
    {{ dev_limit_sql() }}

    {% endif %}
