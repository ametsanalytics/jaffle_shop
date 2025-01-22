select 
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    {{cents_to_dollars('amount',2)}} as amount,
    created as payment_created,
    _batched_at as loaded_at_date,
    max(_batched_at) over (partition by order_id) max_load_date

from {{ source('stripe', 'payment') }}

{{dev_limit_rows()}}