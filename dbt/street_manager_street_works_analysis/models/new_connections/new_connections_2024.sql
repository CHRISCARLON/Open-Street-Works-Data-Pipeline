{% set table_alias = 'completed_new_connections_2024' %}
{{ config(materialized='table', alias=table_alias) }}

WITH all_months AS (
    {% set tables = get_tables() %}
    {% for table in tables %}
        {% if not loop.first %}UNION ALL{% endif %}
        
        SELECT 
            permit_table.event_type,
            permit_table.event_time,
            permit_table.permit_reference_number,
            permit_table.promoter_organisation,
            permit_table.promoter_swa_code,
            permit_table.highway_authority,
            permit_table.highway_authority_swa_code,
            permit_table.work_category,
            permit_table.works_location_type,
            permit_table.proposed_start_date,
            permit_table.proposed_end_date,
            permit_table.actual_start_date_time,
            permit_table.actual_end_date_time,
            permit_table.collaborative_working,
            permit_table.activity_type,
            permit_table.is_traffic_sensitive,
            permit_table.is_ttro_required,
            permit_table.traffic_management_type_ref,
            permit_table.street_name,
            permit_table.road_category,
            permit_table.usrn,
            permit_table.work_status_ref
        FROM {{ table }} AS permit_table
        WHERE permit_table.work_status_ref = 'completed'
        AND permit_table.event_type = 'WORK_STOP'
        AND permit_table.activity_type = 'New service connection'
    {% endfor %}
)

SELECT DISTINCT ON (main.permit_reference_number)
    main.event_type,
    main.event_time,
    main.permit_reference_number,
    main.promoter_organisation,
    main.promoter_swa_code,
    main.highway_authority,
    main.highway_authority_swa_code,
    main.work_category,
    main.works_location_type,
    main.proposed_start_date,
    main.proposed_end_date,
    main.actual_start_date_time,
    main.actual_end_date_time,
    main.collaborative_working,
    main.activity_type,
    main.is_traffic_sensitive,
    main.is_ttro_required,
    main.traffic_management_type_ref,
    main.street_name,
    main.road_category,
    main.usrn,
    main.work_status_ref,
    open_usrn.geometry,
    geo_place.ofgem_electricity_licence,
    geo_place.ofgem_gas_licence,
    geo_place.ofcom_licence,
    geo_place.ofwat_licence,
    COALESCE(uprn_counts.uprn_count, 0) as uprn_count,
    {{ current_timestamp() }} AS date_processed
FROM all_months AS main
LEFT JOIN os_open_usrns.open_usrns_latest AS open_usrn ON main.usrn = open_usrn.usrn
LEFT JOIN geoplace_swa_codes.LATEST_ACTIVE AS geo_place ON CAST(main.promoter_swa_code AS INT) = CAST(geo_place.swa_code AS INT)
LEFT JOIN {{ ref('uprn_usrn_count') }} as uprn_counts ON main.usrn = uprn_counts.usrn