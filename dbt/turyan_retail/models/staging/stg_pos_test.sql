-- models/staging/stg_pos_test.sql
{% if target.name == 'prod' %}
    {% set source_ref = source('raw', 'pos_test') %}
{% else %}
    {% set source_ref = source('raw_dev', 'pos_test') %}
{% endif %}


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