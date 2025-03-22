import pandas as pd
from src.england_street_manager.extract_load_data import quick_col_rename
from loguru import logger


def test_quick_col_rename():
    # Create a sample DataFrame with nested column names
    data = {
        "event_reference": [529770],
        "event_type": ["work-start"],
        "object_data.work_reference_number": ["TSR1591199404915"],
        "object_data.permit_reference_number": ["TSR1591199404915-01"],
        "object_data.promoter_swa_code": ["STPR"],
        "object_data.promoter_organisation": ["Smoke Test Promoter"],
        "object_data.highway_authority": ["CITY OF WESTMINSTER"],
        "object_data.works_location_coordinates": [
            "LINESTRING(501251.53 222574.64,501305.92 222506.65)"
        ],
        "object_data.street_name": ["HIGH STREET NORTH"],
        "object_data.area_name": ["LONDON"],
        "object_data.work_category": ["Standard"],
        "object_data.traffic_management_type": ["Road closure"],
        "object_data.proposed_start_date": ["2020-06-10T00:00:00.000Z"],
        "object_data.proposed_start_time": ["2020-06-10T13:50:00.000Z"],
        "object_data.proposed_end_date": ["2020-06-12T00:00:00.000Z"],
        "object_data.proposed_end_time": [None],
        "object_data.actual_start_date_time": ["2020-06-11T10:11:00.000Z"],
        "object_data.actual_end_date_time": [None],
        "object_data.work_status": ["Works in progress"],
        "object_data.usrn": ["8401426"],
        "object_data.highway_authority_swa_code": ["5990"],
        "object_data.work_category_ref": ["standard"],
        "object_data.traffic_management_type_ref": ["road_closure"],
        "object_data.work_status_ref": ["in_progress"],
        "object_data.activity_type": ["Remedial works"],
        "object_data.is_ttro_required": ["No"],
        "object_data.is_covid_19_response": ["No"],
        "object_data.works_location_type": ["Cycleway, Footpath"],
        "object_data.permit_conditions": ["NCT01a, NCT01b, NCT11a"],
        "object_data.road_category": ["3"],
        "object_data.is_traffic_sensitive": ["Yes"],
        "object_data.is_deemed": ["No"],
        "object_data.permit_status": ["permit_modification_request"],
        "object_data.town": ["LONDON"],
        "object_data.collaborative_working": ["Yes"],
        "object_data.collaboration_type": ["Other"],
        "object_data.collaboration_type_ref": ["other"],
        "object_data.close_footway": ["Yes, a pedestrian walkway will be provided"],
        "object_data.close_footway_ref": ["yes_provide_pedestrian_walkway"],
        "object_data.current_traffic_management_type": ["Multi-way signals"],
        "object_data.current_traffic_management_type_ref": ["multi_way_signals"],
        "object_data.current_traffic_management_update_date": [
            "2020-06-11T10:11:00.000Z"
        ],
        "event_time": ["2020-06-04T08:00:00.000Z"],
        "object_type": ["PERMIT"],
        "object_reference": ["TSR1591199404915-01"],
        "version": [1],
    }
    df = pd.DataFrame(data)
    logger.info(f"Df Unchanged: {df['object_data.area_name']}")

    # Call the quick_col_rename function
    renamed_df = quick_col_rename(df)

    # Assert that the "object_data." prefix is removed from the column names
    assert not any(col.startswith("object_data.") for col in renamed_df.columns)

    # Assert that the column names without the "object_data." prefix are present
    expected_columns = [
        "event_reference",
        "event_type",
        "work_reference_number",
        "permit_reference_number",
        "promoter_swa_code",
        "promoter_organisation",
        "highway_authority",
        "works_location_coordinates",
        "street_name",
        "area_name",
        "work_category",
        "traffic_management_type",
        "proposed_start_date",
        "proposed_start_time",
        "proposed_end_date",
        "proposed_end_time",
        "actual_start_date_time",
        "actual_end_date_time",
        "work_status",
        "usrn",
        "highway_authority_swa_code",
        "work_category_ref",
        "traffic_management_type_ref",
        "work_status_ref",
        "activity_type",
        "is_ttro_required",
        "is_covid_19_response",
        "works_location_type",
        "permit_conditions",
        "road_category",
        "is_traffic_sensitive",
        "is_deemed",
        "permit_status",
        "town",
        "collaborative_working",
        "collaboration_type",
        "collaboration_type_ref",
        "close_footway",
        "close_footway_ref",
        "current_traffic_management_type",
        "current_traffic_management_type_ref",
        "current_traffic_management_update_date",
        "event_time",
        "object_type",
        "object_reference",
        "version",
    ]
    assert all(col in renamed_df.columns for col in expected_columns)

    # Assert that the data in the DataFrame remains unchanged
    assert renamed_df.equals(
        df.rename(columns=lambda col: col.replace("object_data.", ""))
    )
    logger.info(f"Df Renamed: {renamed_df['area_name']}")
