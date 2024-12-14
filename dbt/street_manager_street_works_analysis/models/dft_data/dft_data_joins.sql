{% set table_alias = 'dft_la_data_latest' %}
{{ config(materialized='table', alias=table_alias) }}

SELECT
    -- GeoPlace table columns
    g.swa_code,
    g.account_name,
    g.prefix,
    g.account_type,

    -- DfT LAs table columns
    d.id,
    d.name,
    d.region_id,
    d.ita_id,
    d.ons_code,

    -- Road Lengths table columns cast to float
    CAST(r.trunk_motorways AS FLOAT) AS trunk_motorways,
    CAST(r.principal_motorways AS FLOAT) AS principal_motorways,
    CAST(r.all_motorways AS FLOAT) AS all_motorways,
    CAST(r.trunk_rural_a_roads AS FLOAT) AS trunk_rural_a_roads,
    CAST(r.trunk_urban_a_roads AS FLOAT) AS trunk_urban_a_roads,
    CAST(r.principal_rural_a_roads AS FLOAT) AS principal_rural_a_roads,
    CAST(r.principal_urban_a_roads AS FLOAT) AS principal_urban_a_roads,
    CAST(r.all_a_roads AS FLOAT) AS all_a_roads,
    CAST(r.major_trunk_roads AS FLOAT) AS major_trunk_roads,
    CAST(r.major_principal_roads AS FLOAT) AS major_principal_roads,
    CAST(r.all_major_roads AS FLOAT) AS all_major_roads,
    CAST(r.rural_b_roads AS FLOAT) AS rural_b_roads,
    CAST(r.urban_b_roads AS FLOAT) AS urban_b_roads,
    CAST(r.rural_c_and_u_roads AS FLOAT) AS rural_c_and_u_roads,
    CAST(r.urban_c_and_u_roads AS FLOAT) AS urban_c_and_u_roads,
    CAST(r.minor_roads AS FLOAT) AS minor_roads,
    CAST(r.total_road_length AS FLOAT) AS total_road_length,

    -- Traffic flows for recent years
    t."2019" AS traffic_flow_2019,
    t."2020" AS traffic_flow_2020,
    t."2021" AS traffic_flow_2021,
    t."2022" AS traffic_flow_2022,
    t."2023" AS traffic_flow_2023,

FROM geoplace_swa_codes.LATEST_ACTIVE g
LEFT JOIN dft_las_gss_code.dft_las_gss_code_latest d
    ON REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(g.account_name)), '\s+', ' '), '\s+$', '') = 
    REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(d.name)), '\s+', ' '), '\s+$', '')
LEFT JOIN dft_road_lengths.dft_road_lengths_latest r
    ON TRIM(d.ons_code) = TRIM(r.ons_area_code)
LEFT JOIN dft_traffic_flows.dft_traffic_flows_latest t
    ON TRIM(d.ons_code) = TRIM(t.local_authority_or_region_code)
WHERE d.name IS NOT NULL
    AND g.account_type != 'Welsh Unitary'
    AND d.ons_code != 'E10000009'