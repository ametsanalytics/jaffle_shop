{{ dbt_utils.date_spine(
    datepart="month",
    start_date="cast('2022-01-01' as date)",
    end_date="dateadd('month',12,current_date)"
   )
}}

