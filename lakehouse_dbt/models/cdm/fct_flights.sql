{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['dt', 'flight_number', 'tail_number'],
        table_properties={
            'partitioning': "ARRAY['dt']"
        }
    )
}}


WITH source_flights AS (
    SELECT *
    FROM {{ source('postgresql_flights', 'flights') }}
),
airlines AS (
    SELECT *
    FROM {{ source('postgresql_flights', 'airlines') }}
),
airports AS (
    SELECT *
    FROM {{ source('postgresql_flights', 'airports') }}
)
SELECT
    CAST(
        CAST(f."year" AS varchar) || '-' ||
        LPAD(CAST(f."month" AS varchar), 2, '0') || '-' ||
        LPAD(CAST(f."day" AS varchar), 2, '0')
        AS date
    ) AS dt,
    f.flight_number,
    f.tail_number,
    al.airline,
    ap1.airport AS origin_airport,
    ap2.airport AS destination_airport,
    f.cancellation_reason,
    f.cancelled
FROM source_flights f
LEFT JOIN airlines al
    ON f.airline = al.iata_code
LEFT JOIN airports ap1
    ON f.origin_airport = ap1.iata_code
LEFT JOIN airports ap2
    ON f.destination_airport = ap2.iata_code
{% if is_incremental() %}
    WHERE CAST(
        CAST(f."year" AS varchar) || '-' ||
        LPAD(CAST(f."month" AS varchar), 2, '0') || '-' ||
        LPAD(CAST(f."day" AS varchar), 2, '0')
        AS date
    ) > (SELECT MAX(dt) FROM {{ this }})
{% endif %}
