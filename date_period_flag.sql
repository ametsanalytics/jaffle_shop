{{
    config(
        materialized='table'
    )
}}

with dates as (

    select * from {{ ref('all_dates') }}
),

enhance as (

    select 
    date_day,
    case 
        when year(date_day) = year(getdate()) and month(date_day) = month(getdate()) then 1 else 0 
    end as current_month,
    case 
        when year(date_day) = year(getdate()) then 1 else 0 
    end as current_year,
    case 
        when 
        (year(date_day) = year(getdate()) and month(date_day) = month(getdate()) )
        and day(date_day) < day(getdate()) 
        then 1 else 0 
    end as MTD,
    case 
        when 
        year(date_day) = year(getdate()) and date_day < getdate()
        then 1 else 0
    end as YTD,
    case 
        when last_day( date_day, 'month' ) = date_day then 1 else 0 end as EOM,
    case 
        when last_day( date_day, 'year' ) = date_day then 1 else 0 end as EOY

    
    
    from dates
)

select * from enhance
where year(date_day) = 2025