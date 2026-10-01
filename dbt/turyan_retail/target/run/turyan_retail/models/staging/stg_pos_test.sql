

  create or replace view `analytics_dev`.`stg_pos_test` 
  
    
  
  
    
    
  as (
    -- models/staging/stg_pos_test.sql
select
    transaction_id,
    store_code,
    pos_sku,
    customer_id,
    quantity,
    unit_price,
    quantity * unit_price as line_total,
    transaction_date,
    updated_at
from `raw_dev`.`pos_test`
    
  )
      
      
                    -- end_of_sql
                    
                    