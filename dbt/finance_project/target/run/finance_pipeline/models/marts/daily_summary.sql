
        
  
    
    
    
        
         


        
  

  insert into `default`.`daily_summary`
        ("trade_date", "total_trades", "total_volume", "total_notional_value")

with source as (
    select * from `default`.`stg_trades`
    
)

select
    trade_date,
    count(1) as total_trades,
    sum(quantity) as total_volume,
    sum(notional_value) as total_notional_value
from source
group by trade_date
  
    