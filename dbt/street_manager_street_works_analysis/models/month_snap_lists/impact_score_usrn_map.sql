{% set table_alias = 'impact_scores_usrn_map_' ~ var('year') ~ '_' ~ var('month') %}

{{ config(materialized='table', alias=table_alias) }}

WITH join_tabs AS (
SELECT
    usrn,
    street_name,
    highway_authority,
    actual_start_date_time,
    activity_type,
    work_category,
    is_ttro_required,
    is_traffic_sensitive,
    geometry,
    CASE
    WHEN work_category = 'Standard' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 2 + 0.5 + 0.5
    WHEN work_category = 'Standard' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'No' THEN 2 + 0.5
    WHEN work_category = 'Standard' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'Yes' THEN 2 + 0.5
    WHEN work_category = 'Standard' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'No' THEN 2
    WHEN work_category = 'Standard' THEN 2

    WHEN work_category = 'Major' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 5 + 0.5 + 0.5
    WHEN work_category = 'Major' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'No' THEN 5 + 0.5
    WHEN work_category = 'Major' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'Yes' THEN 5 + 0.5
    WHEN work_category = 'Major' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'No' THEN 5
    WHEN work_category = 'Major' THEN 5

    WHEN work_category = 'Minor' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 + 0.5 + 0.5
    WHEN work_category = 'Minor' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'No' THEN 1 + 0.5
    WHEN work_category = 'Minor' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'Yes' THEN 1 + 0.5
    WHEN work_category = 'Minor' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'No' THEN 1
    WHEN work_category = 'Minor' THEN 1

    WHEN work_category = 'HS2 (Highway)' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 2 + 0.5 + 0.5
    WHEN work_category = 'HS2 (Highway)' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'No' THEN 2 + 0.5
    WHEN work_category = 'HS2 (Highway)' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'Yes' THEN 2 + 0.5
    WHEN work_category = 'HS2 (Highway)' AND is_ttro_required = 'No' AND is_traffic_sensitive = 'No' THEN 2
    WHEN work_category = 'HS2 (Highway)' THEN 2

    WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 3 + 0.5 + 0.5
    WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'No' THEN 3 + 0.5
    WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_ttro_required = 'No' AND is_traffic_sensitive = 'Yes' THEN 3 + 0.5
    WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_ttro_required = 'No' AND is_traffic_sensitive = 'No' THEN 3
    WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') THEN 3

    ELSE 0
    END AS impact_level
  FROM (
    SELECT
      usrn,
      street_name,
      highway_authority,
      activity_type,
      work_category,
      actual_start_date_time,
      is_ttro_required,
      is_traffic_sensitive,
      geometry,
    FROM {{ ref('in_progress_not_complete_list') }}
    
    UNION ALL
    
    SELECT
      usrn,
      street_name,
      highway_authority,
      activity_type,
      work_category,
      actual_start_date_time,
      is_ttro_required,
      is_traffic_sensitive,
      geometry,
    FROM {{ ref('completed_list') }}
  ) AS combined_works
)
SELECT
  usrn,
  street_name,
  highway_authority,
  activity_type,
  work_category,
  is_traffic_sensitive, 
  is_ttro_required,
  geometry,
  SUM(impact_level) AS 'Acute & Legacy Impact Score',
  COUNT(CASE WHEN work_category = 'Major' THEN 1 END) AS 'Major Works',
  COUNT(CASE WHEN work_category = 'Major' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Major Works on Traffic Sensitive Streets',
  COUNT(CASE WHEN work_category = 'Major' AND is_ttro_required = 'Yes' THEN 1 END) AS 'Major Works Needing TTRO',
  COUNT(CASE WHEN work_category = 'Major' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Major Works with TTRO on Traffic Sensitive Streets',
  
  COUNT(CASE WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') THEN 1 END) AS 'Emergency Works',
  COUNT(CASE WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Emergency Works on Traffic Sensitive Streets',
  COUNT(CASE WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_ttro_required = 'Yes' THEN 1 END) AS 'Emergency Works Needing TTRO',
  COUNT(CASE WHEN work_category IN ('Immediate - urgent', 'Immediate - emergency') AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Emergency Works with TTRO on Traffic Sensitive Streets',
  
  COUNT(CASE WHEN work_category = 'Standard' THEN 1 END) AS 'Standard Works',
  COUNT(CASE WHEN work_category = 'Standard' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Standard Works on Traffic Sensitive Streets',
  COUNT(CASE WHEN work_category = 'Standard' AND is_ttro_required = 'Yes' THEN 1 END) AS 'Standard Works Needing TTRO',
  COUNT(CASE WHEN work_category = 'Standard' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Standard Works with TTRO on Traffic Sensitive Streets',
  
  COUNT(CASE WHEN work_category = 'HS2 (Highway)' THEN 1 END) AS 'HS2 Highway Works',
  COUNT(CASE WHEN work_category = 'HS2 (Highway)' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'HS2 Highway Works on Traffic Sensitive Streets',
  COUNT(CASE WHEN work_category = 'HS2 (Highway)' AND is_ttro_required = 'Yes' THEN 1 END) AS 'HS2 Highway Works Needing TTRO',
  COUNT(CASE WHEN work_category = 'HS2 (Highway)' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'HS2 Highway Works with TTRO on Traffic Sensitive Streets',
  
  COUNT(CASE WHEN work_category = 'Minor' THEN 1 END) AS 'Minor Works',
  COUNT(CASE WHEN work_category = 'Minor' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Minor Works on Traffic Sensitive Streets',
  COUNT(CASE WHEN work_category = 'Minor' AND is_ttro_required = 'Yes' THEN 1 END) AS 'Minor Works Needing TTRO',
  COUNT(CASE WHEN work_category = 'Minor' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Minor Works with TTRO on Traffic Sensitive Streets',
  
  COUNT(CASE WHEN work_category = 'Major (PAA)' THEN 1 END) AS 'Major PAAs',
  COUNT(CASE WHEN work_category = 'Major (PAA)' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Major PAAs on Traffic Sensitive Streets',
  COUNT(CASE WHEN work_category = 'Major (PAA)' AND is_ttro_required = 'Yes' THEN 1 END) AS 'Major PAAs Needing TTRO',
  COUNT(CASE WHEN work_category = 'Major (PAA)' AND is_ttro_required = 'Yes' AND is_traffic_sensitive = 'Yes' THEN 1 END) AS 'Major PAAs with TTRO on Traffic Sensitive Streets'
FROM join_tabs
GROUP BY usrn, street_name, highway_authority, activity_type, work_category, is_traffic_sensitive, is_ttro_required, geometry