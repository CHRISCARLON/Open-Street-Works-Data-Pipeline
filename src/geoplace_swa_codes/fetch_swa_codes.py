import requests
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup, Tag
from io import BytesIO
from loguru import logger
from msoffcrypto import OfficeFile
from pydantic import ValidationError
from typing import List, Tuple, Optional

from pydantic_model.swa_codes_model import SWACodeModel

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

        # Add date time processed column
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['date_time_processed'] = current_time

        logger.success(f"DataFrame created successfully: {df.head(10)}")

        return df

    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_swa_codes: {e}")

    return None

def validate_data_model() -> Optional[Tuple[List[SWACodeModel], List[str], pd.DataFrame]]:
    """
    Fetches SWA codes data, validates it against the SWACode Pydantic model,
    and returns a list of validated SWACode objects.

    Returns:
        A list of validated SWACodeModel objects.
    """

    try:
        # Fetch the SWA codes data
        df = fetch_swa_codes()
        # Explicit type assertion
        assert isinstance(df, pd.DataFrame), "DataFrame must be present"
    except (ValueError, AssertionError) as e:
        logger.error(f"Error getting download link: {e}")
        return None

    # Convert DataFrame to a list of dictionaries
    data_dicts = df.to_dict(orient='records')

    # Set variables up for validation process
    validated_data = []
    errors = []

    # Iterate through and validate
    for idx, item in enumerate(data_dicts):
        try:
            # Validate the item
            validated_item = SWACodeModel.model_validate(item)
            validated_data.append(validated_item)
        except ValidationError as e:
            errors.append(f"Error in record {idx}: {str(e)}")
    logger.success(f"Successfully validated {len(validated_data)} out of {len(data_dicts)} records.")
    logger.info(f"There were {len(errors)} errors")
    return validated_data, errors, df
