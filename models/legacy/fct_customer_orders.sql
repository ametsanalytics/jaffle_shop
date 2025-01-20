-- import

with orders as (

    select *,
     row_number() over (partition by customer_id order by order_date, order_id) as user_order_seq
    
    from {{ ref("stg_jaffle_shop__orders") }}
),


customers as (

    select 
        customer_id,
        full_name,
        last_name as surname,
        first_name as givenname

    from {{ ref('stg_jaffle_shop__customers') }}
),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'

),

customer_order_history as (

    select 
        customer_id,
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
        amount as order_value_dollars,
        order_status,
        payment_status
        
    from orders
    inner join customers
    on orders.customer_id = customers.customer_id
    inner join customer_order_history
    on orders.customer_id = customer_order_history.customer_id
    left outer join payments
    on orders.order_id = payments.order_id


)

select * from final







