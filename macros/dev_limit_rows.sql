{% macro dev_limit_rows(date_column='created') %}

{% if target.name==dev %}

{% endif %}
where month({{date_column}}) = 1
{% endmacro %}