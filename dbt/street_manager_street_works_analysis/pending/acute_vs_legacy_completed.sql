{% set table_alias = 'acute_vs_legacy_completed_' ~ var('year') ~ '_' ~ var('month') %}

{% set first_day_of_month = "DATE_TRUNC('month', CAST('" ~ var('year') ~ "-" ~ var('month') ~ "-01' AS DATE))" %}

{{ config(materialized='table', alias=table_alias) }}

WITH get_days AS (
    SELECT
        CASE
            WHEN TRY_CAST(actual_start_date_time AS DATE) IS NOT NULL THEN CAST(actual_start_date_time AS DATE)
            ELSE NULL
        END AS startwork,
        highway_authority,
        work_category,
        permit_reference_number
    FROM
        {{ ref('completed_list') }}
),
failed_cast_count AS (
    SELECT
        highway_authority,
        work_category,
        COUNT(*) AS failed_cast
    FROM
        {{ ref('completed_list') }}
    WHERE
        TRY_CAST(actual_start_date_time AS DATE) IS NULL
    GROUP BY
        highway_authority,
        work_category
)
SELECT
    d.highway_authority,
    d.work_category,
    COUNT(CASE WHEN d.startwork >= {{ first_day_of_month }}  THEN d.permit_reference_number END) AS 'Acute Works',
    COUNT(CASE WHEN d.startwork < {{ first_day_of_month }}  THEN d.permit_reference_number END) AS 'Legacy Works',
    f.failed_cast AS 'Data Issue: Invalid Start Date'
FROM
    get_days d
LEFT JOIN
    failed_cast_count f ON d.highway_authority = f.highway_authority AND d.work_category = f.work_category
GROUP BY
    d.highway_authority,
    d.work_category,
    f.failed_cast
