{% set table_alias = 'wiltshire_example_table_' ~ var('year') ~ '_' ~ var('month') %}
{{ config(materialized='table', alias=table_alias) }}

WITH unioned_data AS (
{% for table in get_tables() %}
    SELECT
        work_status_ref,
        highway_authority,
        event_type,
        event_time,
        permit_reference_number,
        promoter_organisation,
        promoter_swa_code,
        highway_authority_swa_code,
        work_category,
        proposed_start_date,
        proposed_end_date,
        actual_start_date_time,
        actual_end_date_time,
        collaborative_working,
        activity_type,
        is_traffic_sensitive,
        is_ttro_required,
        street_name,
        usrn,
        road_category
        FROM {{ table }}
        {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
)

SELECT
    u.event_type,
    u.event_time,
    u.permit_reference_number,
    u.promoter_organisation,
    u.promoter_swa_code,
    u.highway_authority,
    u.highway_authority_swa_code,
    u.work_category,
    u.proposed_start_date,
    u.proposed_end_date,
    u.actual_start_date_time,
    u.actual_end_date_time,
    u.collaborative_working,
    u.activity_type,
    u.is_traffic_sensitive,
    u.is_ttro_required,
    u.street_name,
    u.usrn,
    u.road_category,
    u.work_status_ref,
    o.geometry
FROM
    unioned_data u
LEFT JOIN os_open_usrns.open_usrns_latest o ON u.usrn = o.usrn
WHERE
    u.work_status_ref = 'completed'
    AND u.highway_authority = 'WILTSHIRE COUNCIL'