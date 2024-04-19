from loguru import logger

# Generate donwload links based on a base url, year, and end month index number (1 to 13) 
def generate_monthly_download_links(year, end_month_index):
    base_url = "https://opendata.manage-roadworks.service.gov.uk/permit"
    
    links = []
    for month in range(1, end_month_index):
        month_formatted = f"{month:02}"  
        url = f"{base_url}/{year}/{month_formatted}.zip"
        links.append(url)
    logger.success("Download Links Generated")
    return links
