--- import

with orders as (

    select  
        order_id,
        customer_id,
        order_date as order_placed_at,
        order_status, 
    from  {{ ref('int_orders') }}
),


payments as (

    select * from {{ ref('stg_stripe__payments') }}
),

-- logic
paid_orders as (

        select 
            order_id,
            max(payment_created) as payment_finalized_date, 
            sum(amount) as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by 1


),

final as ( 
    select 
        orders.order_id,
        orders.customer_id,
        order_placed_at,
        order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date

        /*
        min(order_placed_at) over (partition by orders.customer_id) as first_order_date,
        max(order_placed_at) over (partition by orders.customer_id) as most_recent_order_date,
        count(orders.order_id) over (partition by orders.customer_id) as number_of_orders,*/
        
    from orders
    left join paid_orders  on orders.order_id = paid_orders.order_id


)

select * from final