{{
    config(
        materialized='incremental',
        order_by=['symbol', 'trade_date'],
        unique_key=['symbol', 'trade_date']
    )
}}

with source as (
    select * from {{ ref('stg_trades') }}
    {% if is_incremental() %}
    -- Chỉ lấy dữ liệu từ ngày lớn nhất đã có trong bảng đích trở đi
    where trade_date >= (select max(trade_date) from {{ this }})
    {% endif %}
)

select
    symbol,
    trade_date,
    any(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    anyLast(price) as close_price,
    sum(quantity) as total_volume,
    sum(notional_value) as total_notional
from source
group by symbol, trade_date
