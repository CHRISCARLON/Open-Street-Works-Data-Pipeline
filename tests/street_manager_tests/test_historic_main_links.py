import pytest
from src.street_manager.historic_main_links import generate_monthly_download_links

def test_generate_links_for_all_months_of_2023():
    links_2023 = generate_monthly_download_links(2023, 1, 13)
    assert len(links_2023) == 12
    assert links_2023[0] == "https://opendata.manage-roadworks.service.gov.uk/permit/2023/01.zip"
    assert links_2023[-1] == "https://opendata.manage-roadworks.service.gov.uk/permit/2023/12.zip"

def test_generate_links_for_specific_range_of_months():
    links_range = generate_monthly_download_links(2022, 7, 10)
    assert len(links_range) == 3
    assert links_range[0] == "https://opendata.manage-roadworks.service.gov.uk/permit/2022/07.zip"
    assert links_range[-1] == "https://opendata.manage-roadworks.service.gov.uk/permit/2022/09.zip"

def test_generate_links_for_single_month():
    links_single = generate_monthly_download_links(2024, 3, 4)
    assert len(links_single) == 1
    assert links_single[0] == "https://opendata.manage-roadworks.service.gov.uk/permit/2024/03.zip"

if __name__ == "__main__":
    pytest.main(["-v", __file__])
