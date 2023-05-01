{{
    config(
        on_schema_change='sync_all_columns',
        unique_key = 'transaction_id',
        tags=["core","nightly", "weekly"],
        materialized='table'
    )
}}

-- the below incremental model will append the newly created stores to the table. I am using pre_hook which first drops the past 6 hours data in the table and then appends it again. By doing this we ensure zero data loss.

with raw_transactions as (
    select
        *
    from
        {{ ref('raw_transactions') }} 
    -- where
    --     1 = 1
     --     pre_hook = '
    --         {% if is_incremental() %}          
    --         delete from {{this}} where inserted_at >= dateadd(hour, -6, getdate())            
    --         {% endif %}'

    --     {% if is_incremental() %}

    --     and _metadata__timestamp >= dateadd(hour, -6, getdate())

    --     {% endif %}
),
raw_devices as(
    select
        *
    from
        {{ ref('raw_devices') }}
),
dim_stores as(

    select
        store_id,
        store_name,
        typology as store_typology,
        country as store_country
    from
        {{ ref('raw_stores') }}
)
-- joining transactions with raw_devices and dim_stores to create a dim_transaction table where each row is a single transaction that gives all the details related to the transaction.
select 
    t.transaction_id,
    s.store_id,
    t.device_id,
    d.device_type,
    t.product_name,
    t.category_name,
    t.amount,
    t.status,
    t.card_number,
    t.cvv,
    t.inserted_at,
    t.transaction_happened_at,
    s.store_typology,
    s.store_country

from 
    raw_transactions t
    join  raw_devices d ON t.device_id = d.device_id
    join dim_stores s ON d.store_id = s.store_id 

