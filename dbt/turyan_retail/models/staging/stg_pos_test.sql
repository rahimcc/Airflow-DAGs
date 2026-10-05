-- models/staging/stg_pos_test.sql
{{ config(
    meta={
        'openlineage': {
            'namespace': 'turyan_clickhouse',
            'name': 'stg_pos_test'
        }
    }
) }}


{% if target.name == 'prod' %}
    {% set source_ref = source('raw', 'pos_test') %}
{% else %}
    {% set source_ref = source('raw_dev', 'pos_test') %}
{% endif %}


with source as (
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
        from {{ source_ref }}
),

renamed as ( 
    select 
        toInt64(transaction_id)         as transaction_id,
        trim(store_code)                as store_code,
        UPPER(TRIM(pos_sku))            as pos_sku,
        trim(customer_id)               as customer_id,
        toInt32(quantity)               as quantity,
        toDecimal64(unit_price,2)       as unit_price,
        toDate(transaction_date)        as transaction_date,
        updated_at
    from source
  ), 

deduped as ( 
    select *
    from renamed 
    order by updated_at DESC
    limit 1 by transaction_id
)

select 
     *,
     quantity * unit_price as line_total

from deduped
where quantity > 0 