{% set table_alias = 'in_progress_not_complete_works_list_' ~ var('year') ~ '_' ~ var('month') %}
{{ config(materialized='table', alias=table_alias) }}

{% set current_schema = 'raw_data_' ~ var('year') %}
{% set current_table = '"' ~ var('month') ~ '_' ~ var('year') ~ '"' %}

SELECT
    t1.event_type,
    t1.event_time,
    t1.permit_reference_number,
    t1.promoter_organisation,
    t1.promoter_swa_code,
    t1.highway_authority,
    t1.highway_authority_swa_code,
    t1.work_category,
    t1.proposed_start_date,
    t1.actual_start_date_time,
    t1.collaborative_working,
    t1.activity_type,
    t1.is_traffic_sensitive,
    t1.is_ttro_required,
    t1.street_name,
    t1.usrn,
    t1.road_category,
    t1.work_status_ref,
    u.geometry
FROM {{ current_schema }}.{{ current_table }} AS t1
LEFT JOIN os_open_usrns.open_usrns_latest u ON t1.usrn = u.usrn
WHERE t1.work_status_ref = 'in_progress'
    AND t1.highway_authority IN (
        'LONDON BOROUGH OF BARNET',
        'TRANSPORT FOR LONDON (TFL)',
        'LONDON BOROUGH OF HARROW',
        'LONDON BOROUGH OF BRENT',
        'LONDON BOROUGH OF TOWER HAMLETS',
        'LONDON BOROUGH OF ENFIELD',
        'LONDON BOROUGH OF EALING',
        'LONDON BOROUGH OF MERTON',
        'LONDON BOROUGH OF CROYDON',
        'LONDON BOROUGH OF BARKING AND DAGENHAM',
        'LONDON BOROUGH OF SUTTON',
        'LONDON BOROUGH OF BEXLEY',
        'ROYAL BOROUGH OF KENSINGTON AND CHELSEA',
        'LONDON BOROUGH OF SOUTHWARK',
        'LONDON BOROUGH OF HILLINGDON',
        'LONDON BOROUGH OF CAMDEN',
        'LONDON BOROUGH OF WALTHAM FOREST',
        'LONDON BOROUGH OF REDBRIDGE',
        'CITY OF WESTMINSTER',
        'ROYAL BOROUGH OF GREENWICH',
        'LONDON BOROUGH OF ISLINGTON',
        'LONDON BOROUGH OF HARINGEY',
        'LONDON BOROUGH OF NEWHAM',
        'LONDON BOROUGH OF HACKNEY',
        'LONDON BOROUGH OF HAMMERSMITH & FULHAM',
        'LONDON BOROUGH OF HOUNSLOW',
        'LONDON BOROUGH OF WANDSWORTH',
        'ROYAL BOROUGH OF KINGSTON UPON THAMES',
        'LONDON BOROUGH OF LAMBETH',
        'LONDON BOROUGH OF HAVERING',
        'LONDON BOROUGH OF RICHMOND UPON THAMES',
        'LONDON BOROUGH OF LEWISHAM',
        'CITY OF LONDON CORPORATION',
        'LONDON BOROUGH OF BROMLEY'
    )
    AND t1.permit_reference_number NOT IN (
        SELECT permit_reference_number
        FROM {{ current_schema }}.{{ current_table }}
        WHERE work_status_ref = 'completed'
            AND highway_authority = t1.highway_authority
    )
GROUP BY
    t1.event_type,
    t1.event_time,
    t1.permit_reference_number,
    t1.promoter_organisation,
    t1.promoter_swa_code,
    t1.highway_authority,
    t1.highway_authority_swa_code,
    t1.work_category,
    t1.proposed_start_date,
    t1.actual_start_date_time,
    t1.collaborative_working,
    t1.activity_type,
    t1.is_traffic_sensitive,
    t1.is_ttro_required,
    t1.street_name,
    t1.usrn,
    t1.road_category,
    t1.work_status_ref,
    u.geometry
