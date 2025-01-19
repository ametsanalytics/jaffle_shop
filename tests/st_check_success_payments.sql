-- we expect payments to be greater than zero if successful

with payments as (

    select * from {{ ref('stg_stripe__payments') }}
)

   select 
        order_id,
        sum(amount) as total_amount 
       from payments
       where 
        payment_status = 'payment'
       group by 1
       having 
        total_amount >=0 
       