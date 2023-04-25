{{    
    config(
        on_schema_change='sync_all_columns',
        unique_key = 'id',
        tags=["core", "daily", "weekly"]
    )
}}

with

raw_device as (

    select 
        distinct device_type
    from {{ ref('raw_devices') }}
)
select
    -- created unique idenitifer of a device type, this is unique to every device type
    row_number() over(order by device_type) as id,
    device_type,
    -- created new field to provide the name for the device_type and called it as device_type_name. These device type names were picked from the website
    case 
        when device_type = '1' then 'SumUP Air' 
        when device_type = '2' then 'SumUp Air and Cradle' 
        when device_type = '3' then 'SumUp Solo and Charging Station' 
        when device_type = '4' then 'SumUP Solo and printer' 
        when device_type = '5' then 'Point of Sale Lite plus Solo and Charging Station' 
        else 'New Device Type'
    end as device_type_name,
    -- every device has a launch date associated with it. i.e. first time when a particular device type was introduced into our system
    null as lauch_date
    
    -- we can further add any dimensions associated with the device type
from 
    raw_device
order by 
    device_type