with final as (

    select sum(amount) from {{ ref('stg_stripe__payments') }}
    where payment_status = 'success'
)

select * from final