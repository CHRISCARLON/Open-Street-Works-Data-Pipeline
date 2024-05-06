from .date_month import last_month

def generate_dl_link() -> str:
    """
    Function to generate download link. 
    
    Returns the url required to download the Street Manager data. 
    
    """
    year_month = last_month()
    year, month = str(year_month[0]), str(year_month[1])
    url = f"https://opendata.manage-roadworks.service.gov.uk/permit/{year}/{month}.zip"
    return url
