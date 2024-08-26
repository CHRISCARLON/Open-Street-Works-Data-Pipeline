import re

from src.street_manager.generate_dl_link import generate_dl_link
from src.street_manager.historic_main_links import generate_monthly_download_links

def test_generate_dl_link():
    """
    check YYY_MM patter is in the link correctly.
    """
    result = generate_dl_link()
    expected_pattern = r"^https://opendata\.manage-roadworks\.service\.gov\.uk/permit/\d{4}/\d{2}\.zip$"
    assert re.match(expected_pattern, result), f"Generated URL '{result}' does not match expected pattern '{expected_pattern}'"
    

def test_generate_monthly_download_links():
    """
    check YYY_MM patter is in the link correctly.
    """
    result = generate_monthly_download_links(2023, 1, 13)
    expected_pattern = r"^https://opendata\.manage-roadworks\.service\.gov\.uk/permit/\d{4}/\d{2}\.zip$"
    
    assert len(result) == 12
    
    for link in result:
        assert re.match(expected_pattern, link), f"Generated URL '{result}' does not match expected pattern '{expected_pattern}'"

