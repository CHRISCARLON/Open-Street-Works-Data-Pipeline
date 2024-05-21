{{ config(materialized='table') }}

SELECT
    COUNT(*) AS total_emergency_works,
    usrn,
    promoter_organisation
FROM
    raw_data_2024."04_2024"
WHERE
    work_status_ref = 'completed'
    AND (work_category = 'Immediate - urgent' OR work_category = 'Immediate - emergency')
GROUP BY
    usrn,
    promoter_organisation
