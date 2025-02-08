{% set table_alias = 'impact_scores_england_test' %}
{{ config(
    materialized='table',
    schema='test',
    alias=table_alias)
}}
WITH join_completed_and_in_progress AS (
  SELECT
    usrn,
    street_name,
    highway_authority,
    highway_authority_swa_code,
    actual_start_date_time,
    activity_type,
    work_category,
    is_ttro_required,
    is_traffic_sensitive,
    traffic_management_type_ref,
    uprn_count,
    geometry,
    -- Base impact from work category
    CASE
      WHEN work_category = 'Standard' THEN 2
      WHEN work_category = 'Major' THEN 5
      WHEN work_category = 'Minor' THEN 1
      WHEN work_category = 'HS2 (Highway)' THEN 2
      WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') THEN 3
      ELSE 0
    END
    -- Add TTRO impact
    + CASE WHEN is_ttro_required = 'Yes' THEN 0.5 ELSE 0 END
    -- Add traffic sensitive impact
    + CASE WHEN is_traffic_sensitive = 'Yes' THEN 0.5 ELSE 0 END
    -- Add traffic management impact
    + CASE
        -- High Impact Traffic Management
        WHEN traffic_management_type_ref IN ('road_closure', 'contra_flow', 'lane_closure',
            'convoy_workings', 'multi_way_signals', 'two_way_signals') THEN 2.0
        -- Medium Impact Traffic Management
        WHEN traffic_management_type_ref IN ('give_and_take', 'stop_go_boards', 'priority_working') THEN 1.0
        -- Low Impact Traffic Management
        WHEN traffic_management_type_ref = 'some_carriageway_incursion' THEN 0.5
        -- No Impact
        WHEN traffic_management_type_ref = 'no_carriageway_incursion' THEN 0
        -- Default case for NULL
        WHEN traffic_management_type_ref IS NULL THEN 0.5
        ELSE 0
      END
    -- Add UPRN density impact based on actual distribution
    + CASE
        WHEN uprn_count <= 5 THEN 0.3      -- 33.52% of streets
        WHEN uprn_count <= 10 THEN 0.4     -- 11.04% of streets
        WHEN uprn_count <= 25 THEN 0.5     -- 23.20% of streets
        WHEN uprn_count <= 50 THEN 0.7     -- 16.51% of streets
        WHEN uprn_count <= 100 THEN 0.9    -- 10.11% of streets
        WHEN uprn_count <= 200 THEN 1.1    -- 4.01% of streets
        WHEN uprn_count <= 500 THEN 1.3    -- 1.35% of streets
        ELSE 1.5                           -- 0.26% of streets
      END AS impact_level
  FROM (
    SELECT
      w.usrn,
      w.street_name,
      w.highway_authority,
      w.highway_authority_swa_code,
      w.activity_type,
      w.work_category,
      w.actual_start_date_time,
      w.is_ttro_required,
      w.is_traffic_sensitive,
      w.traffic_management_type_ref,
      w.geometry,
      COALESCE(u.uprn_count, 0) as uprn_count
    FROM {{ source('street_manager', 'in_progress_works_list_england_latest') }} w
    LEFT JOIN {{ source('street_manager', 'uprn_usrn_counts_latest') }} u ON w.usrn = u.usrn
    UNION ALL
    SELECT
      w.usrn,
      w.street_name,
      w.highway_authority,
      w.highway_authority_swa_code,
      w.activity_type,
      w.work_category,
      w.actual_start_date_time,
      w.is_ttro_required,
      w.is_traffic_sensitive,
      w.traffic_management_type_ref,
      w.geometry,
      COALESCE(u.uprn_count, 0) as uprn_count
    FROM {{ source('street_manager', 'completed_works_list_england_latest') }} w
    LEFT JOIN {{ source('street_manager', 'uprn_usrn_counts_latest') }} u ON w.usrn = u.usrn
  ) AS combined_works
),
raw_impacts AS (
    SELECT
        usrn,
        street_name,
        highway_authority,
        LOWER(highway_authority_swa_code) as highway_authority_swa_code,
        uprn_count,
        geometry,
        SUM(impact_level) AS total_impact_level
    FROM join_completed_and_in_progress
    GROUP BY usrn, street_name, highway_authority, highway_authority_swa_code, uprn_count, geometry
)
SELECT
    i.usrn,
    i.street_name,
    i.highway_authority,
    i.uprn_count,
    i.geometry,
    i.total_impact_level,
    la.total_road_length,
    la.traffic_flow_2023,
    i.total_impact_level * (
        LN((SELECT AVG(total_road_length) FROM {{ source('street_manager', 'dft_la_data_latest') }}) /
          NULLIF(la.total_road_length, 0) + 1) + 1
    ) AS normalised_impact_level,
    {{ current_timestamp() }} AS date_processed
FROM raw_impacts i
LEFT JOIN {{ source('street_manager', 'dft_la_data_latest') }} la
    ON i.highway_authority_swa_code = LOWER(la.swa_code)
