import requests
import pandas as pd

from bs4 import BeautifulSoup
from io import BytesIO
from loguru import logger
from msoffcrypto import OfficeFile
from pydantic import ValidationError
from typing import List

from ..pydantic_model.swa_codes_model import SWACodeModel

def get_link():
    """
    Scrape download link from website
    """
    
    url = "https://www.geoplace.co.uk/local-authority-resources/street-works-managers/view-swa-codes"
    
    try: 
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        download_link = soup.find('a', class_='download-item__download-link')

        if download_link:
            href = download_link.get('href')
            return href

    except Exception as e:
        raise e

def fetch_swa_codes():
    """
    Use dowload link to fetch data.
    
    Data is an old xls file and needs extra steps to create dataframe. 
    """
    url = get_link()
    response = requests.get(url)
    result = BytesIO(response.content)
    # print(result.getvalue()[:100]) # check the file signature as the xls file provided is very old?
    
    try:
        # Create an OfficeFile object
        office_file = OfficeFile(result)

        # Load the decryption key (password)
        office_file.load_key('VelvetSweatshop')

        # Create a BytesIO object for the decrypted file
        decrypted_file = BytesIO()
        
        # Decrypt the file
        office_file.decrypt(decrypted_file)

        # Reset the BytesIO object to the beginning
        decrypted_file.seek(0)
        
        # Load into excel
        df = pd.read_excel(decrypted_file, header=1, engine='xlrd')
        df = df.astype(str).replace('nan', None)

        return df
    except Exception as e:
        raise e

def validate_data_model() -> List[SWACodeModel]:
    """
    Fetches SWA codes data, validates it against the SWACode Pydantic model,
    and returns a list of validated SWACode objects.
    
    Returns:
        A list of validated SWACodeModel objects.
    """
    # Fetch the SWA codes data
    df = fetch_swa_codes()

    # Convert DataFrame to a list of dictionaries
    data_dicts = df.to_dict(orient='records')
    
    validated_data = []
    errors = []

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


if __name__ == "__main__":
    v = validate_data_model()
    print(v)
