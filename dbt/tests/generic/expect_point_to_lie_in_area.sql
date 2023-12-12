{% test expect_point_to_lie_in_area(
    model, 
    model_point_column_point_geometry,
    model_point_column_area_id,
    model_point_where_statement,
    model_area,
    model_area_column_id, 
    model_area_column_area_geometry, 
    area_buffer_zone
    ) 
%}
WITH areas AS (
    SELECT 
        {{ model_area_column_id }} as area_column_id,
        ST_BUFFER({{ model_area_column_area_geometry }}, {{ area_buffer_zone }}) AS area_geometry_with_buffer
    FROM {{ model_area }}
),

points AS (
    SELECT *
    FROM {{ model }}
    WHERE {{ model_point_column_point_geometry }} IS NOT NULL
    {% if model_point_where_statement %}
        AND {{ model_point_where_statement }}
    {% endif %}
),

join_areas_on_points AS (
    SELECT 
        * 
    FROM 
        points
    JOIN areas
    ON points.{{ model_point_column_area_id }} = areas.area_column_id
)

SELECT 
    *
FROM
    join_areas_on_points
WHERE 
    NOT ST_WITHIN({{ model_point_column_point_geometry }}, area_geometry_with_buffer)

    
{% endtest %}