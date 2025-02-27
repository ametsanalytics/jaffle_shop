-- ensure there are no dupe order ids in fact table

 



{% test dupe_alert(model,column_name) %}

    select 
        {{column_name}}, 
        count({{column_name}}) 
    from {{ model }}
    group by 1
    having count({{column_name}}) >1

{% endtest%}