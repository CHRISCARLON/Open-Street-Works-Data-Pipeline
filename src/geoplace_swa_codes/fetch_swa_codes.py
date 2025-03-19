import requests
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup, Tag
from io import BytesIO
from loguru import logger
from msoffcrypto import OfficeFile
from typing import Optional

def get_link() -> Optional[str]:
    """
    Scrape download link from website
    Returns:
        str: Download link as a string
    Raises:
        ValueError: If no download link is found or any error occurs
    """
    url = "https://www.geoplace.co.uk/local-authority-resources/street-works-managers/view-swa-codes"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        download_link = soup.find('a', class_='download-item__download-link')
        if download_link and isinstance(download_link, Tag):
            href = download_link.get('href')
            logger.success("Link Found")
            return str(href)
        else:
            raise ValueError("No valid download link found on the page")
    except Exception as e:
        logger.error(f"Error in get_link: {e}")
        raise ValueError(f"Failed to get download link: {e}")

def clean_name_geoplace(x: str):
    """
    Used to clean names columns ready for left join in the future

    # usage: df.loc[:, "account_name"] = df.loc[:, "account_name"].apply(clean_name)
    """
    x = x.replace("LONDON BOROUGH OF", "").strip()
    x = x.replace("COUNTY COUNCIL", "").strip()
    x = x.replace("BOROUGH COUNCIL", "").strip()
    x = x.replace("CITY COUNCIL", "").strip()
    x = x.replace("COUNCIL", "").strip()
    x = x.replace("ROYAL BOROUGH OF", "").strip()
    x = x.replace("COUNCIL OF THE", "").strip()
    x = x.replace("CITY OF", "").strip()
    x = x.replace("COUNTY", "").strip()
    x = x.replace("BOROUGH", "").strip()
    x = x.replace("CITY", "").strip()
    x = x.replace("METROPOLITAN", "").strip()
    x = x.replace("DISTRICT", "").strip()
    x = x.replace("CORPORATION", "").strip()
    x = x.replace("OF", "").strip()
    x = str(x).lower()
    return x

def fetch_swa_codes() -> Optional[pd.DataFrame]:
    """
    Use download link to fetch data.
    Data is an old xls file and needs extra steps to create dataframe.
    Calls the get_link function.

    Returns:
        Optional[pd.DataFrame]: DataFrame containing the SWA codes data, or None if an error occurs.
    """
    try:
        url = get_link()
        # Explicit type assertion
        assert isinstance(url, str), "URL must be a string"
    except (ValueError, AssertionError) as e:
        logger.error(f"Error getting download link: {e}")
        return None

    try:
        response = requests.get(url)
        response.raise_for_status()

        result = BytesIO(response.content)
        office_file = OfficeFile(result)
        office_file.load_key('VelvetSweatshop')

        decrypted_file = BytesIO()
        office_file.decrypt(decrypted_file)
        decrypted_file.seek(0)

        # Read in and do some basic renames and transformation
        df = pd.read_excel(decrypted_file, header=1, engine='xlrd')
        df = df.astype(str).replace('nan', None)
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_')
        df.loc[:, "account_name"] = df.loc[:, "account_name"].apply(clean_name_geoplace)
        df.loc[df["account_name"] == "peter", "account_name"] = "peterborough"
        df.loc[df["account_name"] == "bournemouth, christchurch and poole", "account_name"] = "bournemouth christchurch and poole"
        df.loc[df["account_name"] == "brighton & hove", "account_name"] = "brighton and hove"
        df.loc[df["account_name"] == "telford & wrekin", "account_name"] = "telford and wrekin"
        df.loc[df["account_name"] == "hammersmith & fulham", "account_name"] = "hammersmith and fulham"
        df.loc[df["account_name"] == "cheshire east", "account_name"] = "east cheshire"
        df.loc[df["account_name"] == "cheshire west and chester", "account_name"] = "west cheshire"
        df.loc[df["account_name"] == "east riding  yorkshire", "account_name"] = "eastridingyorkshire"

        # Add date time processed column
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['date_time_processed'] = current_time
        return df

    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_swa_codes: {e}")

    return None

