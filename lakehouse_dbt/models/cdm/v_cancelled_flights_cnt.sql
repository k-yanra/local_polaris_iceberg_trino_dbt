{{
    config(materialized='view')
}}

SELECT
    airline,
    COUNT(*) FILTER (WHERE cancelled = 1) AS cancelled_cnt,
    RANK() OVER (
        ORDER BY COUNT(*) FILTER (WHERE cancelled = 1)
    ) AS cancellation_rank
FROM {{ ref('fct_flights') }}
GROUP BY airline
