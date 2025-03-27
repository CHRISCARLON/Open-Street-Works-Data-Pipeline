{% set table_alias = 'emergency_works_overview_england' %}
{{ config(materialized='table', alias=table_alias) }}

WITH combined_works AS (
    -- Get works from in-progress list
    SELECT
        usrn,
        street_name,
        highway_authority,
        highway_authority_swa_code,
        promoter_organisation,
        promoter_swa_code,
        collaborative_working,
        work_category,
        permit_reference_number
    FROM
        {{ ref('in_progress_list_england') }}
    WHERE work_category IN ('Immediate - emergency', 'Immediate - urgent')
    
    UNION ALL
    
    -- Get works from completed list
    SELECT
        usrn,
        street_name,
        highway_authority,
        highway_authority_swa_code,
        promoter_organisation,
        promoter_swa_code,
        collaborative_working,
        work_category,
        permit_reference_number
    FROM
        {{ ref('completed_list_england') }}
    WHERE work_category IN ('Immediate - emergency', 'Immediate - urgent')
),

-- Join with geoplace to get sector information
promoter_sector AS (
    SELECT 
        cw.highway_authority,
        cw.highway_authority_swa_code,
        cw.promoter_organisation,
        cw.promoter_swa_code,
        cw.work_category,
        cw.permit_reference_number,
        CASE 
            WHEN g.ofcom_licence IS NOT NULL THEN 'Telecoms'
            WHEN g.ofgem_electricity_licence IS NOT NULL THEN 'Electricity'
            WHEN g.ofgem_gas_licence IS NOT NULL THEN 'Gas'
            WHEN g.ofwat_licence IS NOT NULL THEN 'Water'
            ELSE 'Other'
        END AS sector
    FROM combined_works cw
    LEFT JOIN geoplace_swa_codes.LATEST_ACTIVE g 
        ON CAST(cw.promoter_swa_code AS INT) = CAST(g.swa_code AS INT)
)

-- Final aggregation with work category counts
SELECT 
    highway_authority,
    sector,
    work_category,
    COUNT(DISTINCT permit_reference_number) as works_count
FROM promoter_sector
GROUP BY 
    highway_authority,
    sector,
    work_category
ORDER BY 
    highway_authority,
    sector,
    work_category