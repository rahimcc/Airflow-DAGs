SELECT 
    store_code,
    count(*) as transaction_count,
    sum(quantity) as units_sold,
    round(sum(line_total),2) as total_revenue
FROM {{ ref('stg_pos_test') }}
GROUP BY store_code
ORDER BY total_revenue