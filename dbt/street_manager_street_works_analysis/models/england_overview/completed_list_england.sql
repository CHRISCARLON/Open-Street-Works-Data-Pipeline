{% set table_alias = 'completed_works_list_england_latest' %}
{{ config(materialized='table', alias=table_alias) }}

{% set current_schema = 'raw_data_' ~ var('year') %}
{% set current_table = '"' ~ var('month') ~ '_' ~ var('year') ~ '"' %}

SELECT
    permit_table.event_type,
    permit_table.event_time,
    permit_table.permit_reference_number,
    permit_table.promoter_organisation,
    permit_table.promoter_swa_code,
    permit_table.highway_authority,
    permit_table.highway_authority_swa_code,
    permit_table.work_category,
    permit_table.proposed_start_date,
    permit_table.proposed_end_date,
    permit_table.actual_start_date_time,
    permit_table.actual_end_date_time,
    permit_table.collaborative_working,
    permit_table.activity_type,
    permit_table.is_traffic_sensitive,
    permit_table.is_ttro_required,
    permit_table.street_name,
    permit_table.usrn,
    permit_table.road_category,
    permit_table.work_status_ref,
    open_usrn.geometry,
    geo_place.ofgem_electricity_licence,
    geo_place.ofgem_gas_licence,
    geo_place.ofcom_licence,
    geo_place.ofwat_licence,
    {{ current_timestamp() }} AS date_processed
FROM {{ current_schema }}.{{ current_table }} AS permit_table
LEFT JOIN os_open_usrns.open_usrns_latest AS open_usrn ON permit_table.usrn = open_usrn.usrn
LEFT JOIN geoplace_swa_codes.LATEST_ACTIVE AS geo_place ON CAST(permit_table.promoter_swa_code AS INT) = CAST(geo_place.swa_code AS INT)
WHERE permit_table.work_status_ref = 'completed'
