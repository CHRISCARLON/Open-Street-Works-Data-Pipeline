{% set table_alias = 'LT_completed_collab_vs_non_collab_count_' ~ var('year') ~ '_' ~ var('month') %}

{{ config(materialized='table', alias=table_alias) }}

WITH unioned_data AS (
{% for table in get_tables() %}
    SELECT
        highway_authority,
        promoter_organisation,
        work_category,
        activity_type,
        is_ttro_required,
        collaborative_working,
        work_status_ref
    FROM {{ table }}
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
)

SELECT
    highway_authority,
    promoter_organisation,
    work_category,
    activity_type,
    is_ttro_required,
    COUNT(CASE WHEN collaborative_working = 'Yes' THEN 1 END) AS collab_works_count,
    COUNT(CASE WHEN collaborative_working = 'No' THEN 1 END) AS non_collab_works_count
FROM
    unioned_data
WHERE
    work_status_ref = 'completed'
    AND highway_authority IN (
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
GROUP BY
    highway_authority,
    promoter_organisation,
    work_category,
    activity_type,
    is_ttro_required
