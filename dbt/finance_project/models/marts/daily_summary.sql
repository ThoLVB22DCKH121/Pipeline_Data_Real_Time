{{
    config(
        materialized='incremental',
        order_by=['trade_date'],
        unique_key=['trade_date']
    )
}}

with source as (
    select * from {{ ref('stg_trades') }}
    {% if is_incremental() %}
    where trade_date >= (select max(trade_date) from {{ this }})
    {% endif %}
)

select
    trade_date,
    count(1) as total_trades,
    sum(quantity) as total_volume,
    sum(notional_value) as total_notional_value
from source
group by trade_date
