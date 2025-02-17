{% set payment_methods = ['credit_card','coupon','bank_transfer','gift_card'] %}
with staging as (

    select * from {{ ref('stg_stripe__payments') }}
),

pivoted as (


    select
        order_id,
        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{payment_method}}' then amount else 0 end) as "{{payment_method}}_amount"
        {%- if not loop.last %},{% endif %}
        {% endfor -%}
    from staging
    group by 1
)

select * from pivoted


