{% test expect_regex_to_limit_value(
    model, 
    column_regex,
    regex,
    column_value,
    maximum_value
    ) 
%}

WITH regex_matching_entries AS (
    SELECT * FROM {{ model }}
    WHERE {{ column_regex }} SIMILAR TO '%({{ regex }})%'
)

SELECT * FROM regex_matching_entries WHERE {{ column_value }} >= {{ maximum_value }}
    
{% endtest %}