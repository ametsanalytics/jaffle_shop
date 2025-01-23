select
   -- {{dbt_utils.generate_surrogate_key(['user_id','id'])}} as customer_order_id,
    id as order_id,
    user_id as customer_id,
    order_date,
    status as order_status, 
    _etl_loaded_at,
    case when order_status not in ('returned','return_pending') then order_date end as valid_order_date 


from {{ source('jaffle_shop', 'orders') }}

{{dev_limit_rows('order_date')}}