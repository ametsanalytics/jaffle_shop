{%- macro cents_to_dollars(column_name,decimals) -%}

round({{column_name}} / 100,{{decimals}})

{%- endmacro-%}