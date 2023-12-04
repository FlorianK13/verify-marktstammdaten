{{
    config(
        materialized = 'table',
        indexes=[
            {'columns': ['geometry_array'], 'type': 'gist'}
        ]
    )
}}

with source as (
    select * from {{ source('public', 'municipalities_geoboundaries') }}
),

renamed as (
    select
        "AGS" as municipality_id,
        "GEN" as municipality,
        "NUTS" as nuts,
        "WSK" as legal_effective_date,
        "geometry" as geometry_array
    from source
)

select * from renamed