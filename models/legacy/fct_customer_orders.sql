-- import

with orders as (

   select * from {{ ref('int_orders') }}
),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'

),

customer_order_history as (

    select 
        customer_id,
        full_name,
        surname,
        givenname,
        total_lifetime_value,
        first_order_date,
        order_count
    from {{ ref('customer_order_history') }}
),

final as (

    select 
        orders.order_id,
        orders.customer_id,
        surname,
        givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        order_value_dollars,
        order_status,
        payments.payment_status
        
    from orders
    inner join customer_order_history
    on orders.customer_id = customer_order_history.customer_id
    left outer join payments
    on orders.order_id = payments.order_id


)

select * from final







