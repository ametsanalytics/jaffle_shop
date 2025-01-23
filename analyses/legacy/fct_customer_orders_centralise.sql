-- import

with orders as (

   select * from {{ ref('int_orders') }}

),

customers as (

    select 
        customer_id,    
        first_name as givenname,
        last_name as surname,
        full_name 
    from {{ ref('stg_jaffle_shop__customers')}}
),

customer_orders as (

    select 
        orders.*,
        full_name,
        surname,
        givenname,
        count(*) over (partition by orders.customer_id) as order_count,
        min(order_date) over (partition by orders.customer_id) as first_order_date,
        sum(nvl2(valid_order_date,order_value_dollars,0)) over (partition by orders.customer_id) as total_lifetime_value

 /* redundant fields that can be added in for BI later if needed

        min(valid_order_date) over (partition by orders.customer_id) as first_non_returned_order_date,
        max(valid_order_date) over (partition by orders.customer_id) as most_recent_non_returned_order_date,
        count(nvl2(valid_order_date,1,0)) over (partition by orders.customer_id) as non_returned_order_count,
        array_agg(distinct order_id) over (partition by orders.customer_id) as order_ids
 */

    from customers
    inner join orders on orders.customer_id = customers.customer_id

), 

/* redundant fields that can be added in for BI later if needed

avg_order_values as (

    select 

        sum(total_lifetime_value/non_returned_order_count) 
        as avg_non_returned_order_value
    
    from customer_orders
),
*/
final as (

    select 
        order_id,
        customer_id,
        surname,
        givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status
        
    from customer_orders

)

select * from final







