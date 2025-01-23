{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

with base as (

    select 

        {{dbt_utils.generate_surrogate_key(['interval_start_time','interval_end_time','query_parameterized_hash','session_id','total_elapsed_time'])}} as id,
        row_number() over (partition by id order by interval_end_time) as rn,
        interval_start_time,
        interval_end_time, 
        query_parameterized_hash, 
        query_text, 
        database_id, 
        database_name, 
        schema_id, 
        schema_name,
        query_type, 
        session_id, 
        user_name, 
        role_name, 
        role_type, 
        warehouse_id,
        warehouse_name, 
        warehouse_size,
        warehouse_type, 
        query_tag,
        is_client_generated_statement, 
        release_version,
        errors, 
        total_elapsed_time, 
        credits_used_cloud_services
        bytes_scanned


from {{ source('snowflake', 'aggregate_query_history') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where interval_end_time > (select max(interval_end_time) from {{ this }}) 
{% endif %}

)


select * from base 

--where id is not null


