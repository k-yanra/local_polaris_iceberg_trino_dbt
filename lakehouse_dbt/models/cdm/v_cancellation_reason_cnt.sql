{{
    config(
        materialized='view'
    )
}}

WITH cancellation_reason_cnt AS (
    SELECT
        cancellation_reason,
        COUNT(*) FILTER (WHERE cancelled = 1) AS reason_cnt
    FROM {{ ref('fct_flights') }}
    GROUP BY cancellation_reason

)
SELECT 
    t1.cancellation_reason,
    t1.reason_cnt,
    t2.cancellation_description
FROM cancellation_reason_cnt as t1
LEFT JOIN  {{ source('postgresql_flights', 'cancellation_codes') }} as t2
    ON t1.cancellation_reason = t2.cancellation_reason
ORDER BY reason_cnt DESC
