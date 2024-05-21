with unioned_data as (

    -- Select from raw_data_2024 schema
    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2024."01_2024"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2024."02_2024"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2024."03_2024"

    -- Select from raw_data_2023 schema
    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."04_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."05_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."06_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."07_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."08_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."09_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."10_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."11_2023"

    union all

    select 
        promoter_organisation, 
        highway_authority, 
        work_category, 
        activity_type, 
        collaborative_working
    from raw_data_2023."12_2023"

)

select 
    promoter_organisation,
    highway_authority,
    work_category,
    activity_type,
    count(*) as collaborative_working_count
from unioned_data
where collaborative_working = 'Yes'
group by 
    promoter_organisation,
    highway_authority,
    work_category,
    activity_type
