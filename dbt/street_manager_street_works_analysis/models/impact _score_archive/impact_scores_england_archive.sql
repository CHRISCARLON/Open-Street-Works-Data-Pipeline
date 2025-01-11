{% set table_alias = 'england_impact_scores_' ~ var('month') ~ '_' ~ var('year') %}

{{ config(
    materialized='table',
    schema='archive',
    alias=table_alias
) }}

SELECT *
FROM {{ source('street_manager', 'impact_scores_england_latest') }}