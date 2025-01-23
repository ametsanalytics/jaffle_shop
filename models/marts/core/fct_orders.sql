{{
    config(
        materialized='ephemeral'
    )
}}


with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

succesful_payments as (

    select 
        order_id,
        sum(case when payment_status = 'success' then amount end) as amount_paid
    from payments
    group by 1
),

final as (

    select 
        o.order_id,
        o.customer_id,
        o.order_date,
        coalesce(p.amount_paid,0) as amount_paid

        from orders o
        inner join succesful_payments p on o.order_id = p.order_id
        
)

select * from final