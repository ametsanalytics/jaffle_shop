-- import

with orders as (

    select *,
    row_number() over (partition by customer_id order by order_date, order_id) as user_order_seq
    
    from {{ ref('stg_jaffle_shop__orders') }}
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

    select
        payment_id,
        order_id,
        payment_status,
        amount as payment_amount

    from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'
),

final as (

        select 
            customers.customer_id,
            full_name,
            surname,
            givenname,
            min(valid_order_date) as first_non_returned_order_date,
           
            max(valid_order_date) as most_recent_non_returned_order_date,

            coalesce(count(case when valid_order_date is not null then 1 end),0) as non_returned_order_count,

            sum(case 
                when valid_order_date is not null then payment_amount else 0 
                end)/nullif(count(case when valid_order_date is not null then 1 
                end),0)
            as avg_non_returned_order_value,

            array_agg(distinct orders.order_id) as order_ids,
            min(order_date) as first_order_date,
            coalesce(max(user_order_seq),0) as order_count,

            sum(case 
                    when valid_order_date is not null
                    then payment_amount else 0 
                end) 
            as total_lifetime_value


        from customers
        inner join orders 
        on orders.customer_id = customers.customer_id
        left outer join payments 
        on orders.order_id = payments.order_id

        group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

        

)

select * from final







