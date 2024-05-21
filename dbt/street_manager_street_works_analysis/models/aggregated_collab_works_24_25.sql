{% set swa_info_table = 'swa_code_info."SWA_CODES_2024_02_13"' %}  

WITH unioned_data AS (
    {% for table in get_collab_tables() %}
    SELECT 
        event_type,
        promoter_organisation, 
        CAST(promoter_swa_code AS VARCHAR) AS promoter_swa_code,  
        highway_authority, 
        work_category, 
        activity_type, 
        work_status_ref,
        collaborative_working
    FROM {{ table }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT 
    ud.event_type,
    ud.promoter_organisation,
    ud.highway_authority,
    ud.work_category,
    ud.activity_type,
    ud.work_status_ref,
    ud.promoter_swa_code,
    COALESCE(swa."Ofgem Gas Licence", 'N/A') AS "Gas Sector",
    COALESCE(swa."Ofgem Electricity Licence", 'N/A') AS "Electricity Sector",
    COALESCE(swa."Ofwat Licence", 'N/A') AS "Water Sector",
    COALESCE(swa."Ofcom Licence", 'N/A') AS "Telco Sector",
    COUNT(*) AS collaborative_working_count
FROM 
    unioned_data ud
LEFT JOIN {{ swa_info_table }} swa
    ON ud.promoter_swa_code = CAST(swa."SWA CODE" AS VARCHAR) 
WHERE 
    ud.collaborative_working = 'Yes'
    AND ud.work_status_ref = 'completed'
GROUP BY 
    ud.event_type,
    ud.promoter_organisation,
    ud.highway_authority,
    ud.work_category,
    ud.activity_type, 
    ud.work_status_ref,
    ud.promoter_swa_code,
    swa."Ofgem Gas Licence",
    swa."Ofgem Electricity Licence",
    swa."Ofwat Licence",
    swa."Ofcom Licence"
