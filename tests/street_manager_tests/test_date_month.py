import re

from src.street_manager.date_month import last_month, date_for_table, month_to_abbrev

def test_last_month():
    month = last_month()
    assert len(month) == 2
    assert isinstance(month, list)

def test_date_for_table():
    result = date_for_table()
    
    # Define the regex pattern
    pattern = r'^\d{2}_\d{4}$'
    
    # Assert that the result matches the pattern
    assert re.match(pattern, result), f"Result '{result}' does not match the expected format 'MM_YYYY'"
    
    # Additional check to ensure the month is between 01 and 12
    month, year = result.split('_')
    assert 1 <= int(month) <= 12, f"Month '{month}' is not between 01 and 12"

def test_month_to_abbrev():
    result = month_to_abbrev(3)
    assert result == "Mar"
    
    result_2 = month_to_abbrev(10)
    assert result_2 == "Oct"
