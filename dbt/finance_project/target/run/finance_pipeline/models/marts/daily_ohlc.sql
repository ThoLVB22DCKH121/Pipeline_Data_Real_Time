
        
  
    
    
    
        
         


        
  

  insert into `default`.`daily_ohlc`
        ("symbol", "trade_date", "open_price", "high_price", "low_price", "close_price", "total_volume", "total_notional")

with source as (
    select * from `default`.`stg_trades`
    
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
  
    