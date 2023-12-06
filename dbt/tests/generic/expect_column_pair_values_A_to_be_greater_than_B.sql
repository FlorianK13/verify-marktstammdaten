{% test expect_column_pair_values_A_to_be_greater_than_B(model, column_A, column_B, where, factor_column_A=1, or_equal=False) %}

SELECT 
    *
FROM
    {{ model }}
WHERE 
    {% if or_equal %}
            {{ column_B }} > {{ column_A }} * {{ factor_column_A }}
    {% else %}
        {{ column_B }} >= {{ column_A }} * {{ factor_column_A }}
    {% endif %}
    
    {% if where %}
        AND {{ where }}
    {% endif %}

{% endtest %}