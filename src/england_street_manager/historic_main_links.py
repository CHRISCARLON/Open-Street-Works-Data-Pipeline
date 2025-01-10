from loguru import logger


def generate_monthly_download_links(year: int, start_month_index: int, end_month_index: int) -> list:
    """
    Generate donwload links based on a base url, year, and end month index number (1 to 13)
    Should return a list of download links base on input
    If you input 2023, 13 then you should get download links for all months of 2023
    
    """
    base_url = "https://opendata.manage-roadworks.service.gov.uk/permit"
    
    links = []
    for month in range(start_month_index, end_month_index):
        month_formatted = f"{month:02}"  
        url = f"{base_url}/{year}/{month_formatted}.zip"
        links.append(url)
    logger.success("Download Links Generated")
    return links
