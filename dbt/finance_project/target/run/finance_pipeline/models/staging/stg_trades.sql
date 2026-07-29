

  create or replace view `default`.`stg_trades` 
  
    
  
  
    
    
  as (
    

with raw_data as (
    select * from default.trades
)

select
    symbol,
    price,
    quantity,
    toDateTime(trade_time_ms / 1000) as trade_time,
    notional_value,
    toDate(toDateTime(trade_time_ms / 1000)) as trade_date
from raw_data
    
  )
      
      
                    -- end_of_sql
                    
                    