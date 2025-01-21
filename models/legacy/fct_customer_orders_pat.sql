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
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        customer_first_name,
        customer_last_name

        /*
        min(order_placed_at) over (partition by orders.customer_id) as first_order_date,
        max(order_placed_at) over (partition by orders.customer_id) as most_recent_order_date,
        count(orders.order_id) over (partition by orders.customer_id) as number_of_orders,*/
        
    from orders
    left join paid_orders  on orders.order_id = paid_orders.order_id
    left join customers  on orders.customer_id = customers.customer_id 


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

        row_number() over (order by order_id ) as transaction_seq,
        row_number() over (partition by customer_id order by order_id ) as customer_sales_seq,

        case 
            when (
            rank() over (partition by customer_id order by order_placed_at, order_id) = 1) 
            then 'new'
            else 'return'
        end as nvsr,

        sum(total_amount_paid) over (partition by customer_id order by  order_placed_at, order_id) as customer_lifetime_value,
        first_value(first_order_date) over (partition by customer_id order by order_placed_at, order_id) as fdos   

    from customer_paid_orders 
      
)

select * from final
