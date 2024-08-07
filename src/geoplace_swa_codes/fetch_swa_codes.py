import requests
import pandas as pd

from bs4 import BeautifulSoup
from io import BytesIO
from msoffcrypto import OfficeFile

def get_link():
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
    url = get_link()
    response = requests.get(url)
    result = BytesIO(response.content)
    # print(result.getvalue()[:100]) # check the file signature as the xls file provide is very old?
    
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
        return df
    except Exception as e:
        raise e

# if __name__ == "__main__":
#     v = fetch_swa_codes()
#     print(type(v))
