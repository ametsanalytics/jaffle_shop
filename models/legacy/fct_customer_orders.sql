with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

order_count as (

    select
     customer_id,
     count(order_id) as order_count
    
    from orders
    group by customer_id

),
final as (

select
    o.order_id,
    o.customer_id,
    first_name || ' ' || last_name as name, 
    last_name as surname,
    first_name as givenname,
    first_order_date,
    order_count,
    lifetime_value as total_lifetime_value,
    round(amount/100.0,2) as order_value_dollars,
    status as order_status,
    payment_status as payment_status
from orders o
inner join customers c using(customer_id)
inner join payments p using (order_id)
inner join order_count using (customer_id)

)

select * from final


/*
join (

    select 
        b.id as customer_id,
        b.name as full_name,
        b.last_name as surname,
        b.first_name as givenname,
        min(order_date) as first_order_date,
        min(case when a.status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when a.status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when a.status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end)/NULLIF(count(case when a.status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct a.id) as order_ids

    from (
      select 
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        *
      from raw.jaffle_shop.orders
    ) a

    join ( 
      select 
        first_name || ' ' || last_name as name, 
        * 
      from raw.jaffle_shop.customers
    ) b
    on a.user_id = b.id

    left outer join raw.stripe.payment c
    on a.id = c.orderid

    where a.status NOT IN ('pending') and c.status != 'fail'

    group by b.id, b.name, b.last_name, b.first_name

) customer_order_history
on orders.user_id = customer_order_history.customer_id

left outer join raw.stripe.payment payments
on orders.id = payments.orderid

where payments.status != 'fail'

and user_id = 1 */