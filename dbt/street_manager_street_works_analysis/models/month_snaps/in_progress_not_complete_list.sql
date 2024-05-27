{% set table_alias = 'in_progress_not_complete_list_' ~ var('year') ~ '_' ~ var('month') %}

{{ config(materialized='table', alias=table_alias) }}

{% set current_schema = 'raw_data_' ~ var('year') %}

{% set current_table = '"' ~ var('month') ~ '_' ~ var('year') ~ '"' %}

SELECT
    event_type,
    event_time,
    permit_reference_number,
    promoter_organisation,
    promoter_swa_code,
    highway_authority,
    highway_authority_swa_code,
    work_category,
    proposed_end_date,
    collaborative_working,
    activity_type,
    is_ttro_required,
    street_name,
    usrn,
    road_category,
    work_status_ref
FROM
    {{ current_schema }}.{{ current_table }} AS t1
WHERE
    t1.work_status_ref = 'in_progress'
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
        WHERE
            work_status_ref = 'completed'
            AND highway_authority = t1.highway_authority
    )
GROUP BY
    event_type,
    event_time,
    permit_reference_number,
    promoter_organisation,
    promoter_swa_code,
    highway_authority,
    highway_authority_swa_code,
    work_category,
    proposed_end_date,
    collaborative_working,
    activity_type,
    is_ttro_required,
    street_name,
    usrn,
    road_category,
    work_status_ref,
