{% set table_alias = 'future_works_by_period_' ~ var('year') ~ '_' ~ var('month') %}

{% set current_month = "DATE_TRUNC('month', CAST('" ~ var('year') ~ "-" ~ var('month') ~ "-01' AS DATE))" %}
{% set month_1_start = "DATE_ADD(" ~ current_month ~ ", INTERVAL 1 MONTH)" %}
{% set month_1_end = "DATE_ADD(" ~ month_1_start ~ ", INTERVAL 1 MONTH - INTERVAL 1 DAY)" %}
{% set month_2_start = "DATE_ADD(" ~ month_1_end ~ ", INTERVAL 1 DAY)" %}
{% set month_2_end = "DATE_ADD(" ~ month_2_start ~ ", INTERVAL 1 MONTH - INTERVAL 1 DAY)" %}
{% set month_3_start = "DATE_ADD(" ~ month_2_end ~ ", INTERVAL 1 DAY)" %}
{% set month_3_end = "DATE_ADD(" ~ month_3_start ~ ", INTERVAL 1 MONTH - INTERVAL 1 DAY)" %}
{% set month_4_start = "DATE_ADD(" ~ month_3_end ~ ", INTERVAL 1 DAY)" %}
{% set month_4_end = "DATE_ADD(" ~ month_4_start ~ ", INTERVAL 1 MONTH - INTERVAL 1 DAY)" %}
{% set beyond_month_4_start = "DATE_ADD(" ~ month_4_end ~ ", INTERVAL 1 DAY)" %}

{{ config(materialized='table', alias=table_alias) }}

WITH get_days AS (
    SELECT
        CASE
            WHEN TRY_CAST(proposed_start_date AS DATE) IS NOT NULL THEN CAST(proposed_start_date AS DATE)
            ELSE NULL
        END AS startwork,
        highway_authority,
        work_category,
        permit_reference_number
    FROM
        {{ ref('future_planned_list') }}
),
failed_cast_count AS (
    SELECT
        highway_authority,
        work_category,
        COUNT(*) AS failed_cast
    FROM
        {{ ref('future_planned_list') }}
    WHERE
        TRY_CAST(proposed_start_date AS DATE) IS NULL
    GROUP BY
        highway_authority,
        work_category
)
SELECT
    d.highway_authority,
    d.work_category,
    COUNT(CASE WHEN d.startwork BETWEEN {{ month_1_start }} AND {{ month_1_end }} THEN d.permit_reference_number END) AS "Next month",
    COUNT(CASE WHEN d.startwork BETWEEN {{ month_2_start }} AND {{ month_2_end }} THEN d.permit_reference_number END) AS "In 2 months",
    COUNT(CASE WHEN d.startwork BETWEEN {{ month_3_start }} AND {{ month_3_end }} THEN d.permit_reference_number END) AS "In 3 months",
    COUNT(CASE WHEN d.startwork BETWEEN {{ month_4_start }} AND {{ month_4_end }} THEN d.permit_reference_number END) AS "In 4 months",
    COUNT(CASE WHEN d.startwork >= {{ beyond_month_4_start }} THEN d.permit_reference_number END) AS "Beyond 4 months",
    COUNT(CASE WHEN d.startwork IS NULL THEN d.permit_reference_number END) AS 'No Proposed Start Date',
    f.failed_cast AS 'Data Issue: Invalid Proposed Start Date'
FROM
    get_days d
LEFT JOIN
    failed_cast_count f ON d.highway_authority = f.highway_authority AND d.work_category = f.work_category
GROUP BY
    d.highway_authority,
    d.work_category,
    f.failed_cast
