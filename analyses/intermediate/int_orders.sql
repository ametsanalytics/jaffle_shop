with orders as (
    
    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'

),

order_totals as (

    select
    order_id,
    payment_status,
    sum(amount) as order_value_dollars
    from orders
    inner join payments using(order_id)
    group by 1,2

),

order_totals_joined as (

    select * from orders
    inner join order_totals using (order_id)

)

select * from order_totals_joined