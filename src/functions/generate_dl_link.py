from .date_month import current_year_month

def generate_dl_link() -> str:
    """
    Function to generate download link. 
    Returns the url required to download the street manager data. 
    """
    year_month = current_year_month()
    year, month = str(year_month[0]), str(year_month[1])
    url = f"https://opendata.manage-roadworks.service.gov.uk/permit/{year}/{month}.zip"
    return url
