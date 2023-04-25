{{
    config(
        on_schema_change='sync_all_columns',
        unique_key = 'store_id',
        materialized='incremental',
        tags=["core","2hourly", "weekly"]
    )
}}

-- the below incremental model will append the newly created stores to the table, It will append the data that is greater then the maximium timestamp of created_at in the table. 


with

raw_stores as (

    select 
        * 
    from 
        {{ ref('raw_stores') }}
    

    {% if is_incremental() %}

        where created_at > (select max(created_at) from {{ this }})

    {% endif %}

)
select
    *
from 
    raw_stores