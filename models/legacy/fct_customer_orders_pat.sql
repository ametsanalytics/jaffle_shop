--- import

with orders as (

    select  
        order_id,
        customer_id,
        order_date as order_placed_at,
        order_status, 
    from  {{ ref('int_orders') }}
),

customers as (

    select    
        customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from {{ ref('stg_jaffle_shop__customers') }}

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

customer_paid_orders as ( 
    select 
        orders.order_id,
        orders.customer_id,
        order_placed_at,
        order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        customer_first_name,
        customer_last_name,
        sum(total_amount_paid) over (partition by orders.customer_id) as customer_lifetime_value,
        min(order_placed_at) over (partition by orders.customer_id) as first_order_date,
        max(order_placed_at) over (partition by orders.customer_id) as most_recent_order_date,
        count(orders.order_id) over (partition by orders.customer_id) as number_of_orders,
        row_number() over (order by orders.order_id) as transaction_seq,
        row_number() over (partition by orders.customer_id order by orders.order_id) as customer_sales_seq

    from orders
    left join paid_orders p on orders.order_id = p.order_id
    left join customers c on orders.customer_id = c.customer_id 


),

final as (

    select
        order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
        transaction_seq,
        customer_sales_seq,
        case 
            when first_order_date = order_placed_at
            then 'new' else 'return' 
        end as nvsr,
        customer_lifetime_value,
        first_order_date as fdos   

    from customer_paid_orders 
      
)

select * from final