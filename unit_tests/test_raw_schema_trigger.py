from datetime import datetime
from unittest.mock import patch
from src.street_manager_permit_functions.schema_name_trigger import get_raw_data_year

@patch('src.street_manager_permit_functions.schema_name_trigger.datetime')
def test_get_raw_data_year_january(mock_datetime):
    # Test case when the current month is January 24 near the end of the month
    mock_datetime.now.return_value = datetime(2024, 1, 1)
    expected_output = "raw_data_2023"
    assert get_raw_data_year() == expected_output

@patch('src.street_manager_permit_functions.schema_name_trigger.datetime')
def test_get_raw_data_year_not_january(mock_datetime):
    # Test case when the current month is not January
    mock_datetime.now.return_value = datetime(2024, 5, 28)
    expected_output = "raw_data_2024"
    assert get_raw_data_year() == expected_output
    

@patch('src.street_manager_permit_functions.schema_name_trigger.datetime')
def test_get_raw_data_year_january_later_in_month(mock_datetime):
    # Test case when the current month is January 25 near the end of the month 
    mock_datetime.now.return_value = datetime(2025, 1, 28)
    expected_output = "raw_data_2024"
    assert get_raw_data_year() == expected_output
