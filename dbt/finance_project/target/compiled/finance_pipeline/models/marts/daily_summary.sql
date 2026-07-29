

with source as (
    select * from `default`.`stg_trades`
    
    where trade_date >= (select max(trade_date) from `default`.`daily_summary`)
    
)

select
    trade_date,
    count(1) as total_trades,
    sum(quantity) as total_volume,
    sum(notional_value) as total_notional_value
from source
group by trade_date