import requests
import pandas as pd

from io import BytesIO
from stream_unzip import stream_unzip
from loguru import logger


def fetch_presigned_url(api_url) -> str:
    """
    Fetch the pre-signed URL from the initial API endpoint.

    Args:
        api_url (str): The API URL returning the JSON with the pre-signed URL.
    
    Returns:
        str: Pre-signed URL for the ZIP file.
    """
    try: 
        response = requests.get(api_url)
        response.raise_for_status()  
        data = response.json()
        logger.success(f'URL generated: {data}')
        return data['url']
    
    except Exception as e:
        logger.error(f'There has been an error: {e}')
        raise  


def fetch_data(dl_url):
    """
    Stream data from a URL where the CSV data is zipped.

    Args: 
        dl_url (str): The URL for downloading the zipped CSV file.
    
    Returns:
        Iterable of streamed zipped chunks.
    """
    with requests.get(dl_url, stream=True, timeout=30) as response:
        response.raise_for_status()  
        yield from response.iter_content(chunk_size=65536)  


def quick_col_rename(df) -> pd.DataFrame:
    """
    Rename columns...
    
    """
    df = df.rename(columns={'02': "Table Number"})
    return df


def load_csv_from_zip(zipped_chunks, nrows=10):
    """
    Load the first nrows rows from the single CSV file within streamed zipped chunks.

    Args:
        zipped_chunks (Iterable): Streamed zipped chunks.
        nrows (int): Number of rows to load from the CSV.

    Returns:
        DataFrame: A Pandas DataFrame containing the first nrows rows from the CSV.
    """
    found_csv = False  # Flag to check if CSV file is found

    for file_name, file_size, file_chunks in stream_unzip(zipped_chunks):
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8')

        if file_name.endswith('.csv'):
            found_csv = True
            logger.success("CSV file found.")
            try:
                csv_content = BytesIO()
                for chunk in file_chunks:
                    csv_content.write(chunk)
                csv_content.seek(0)
                df = pd.read_csv(csv_content, nrows=nrows)
                df = quick_col_rename(df)
                return df
            except Exception as e:
                logger.error(f'There has been an error: {e}')
                raise  

    if not found_csv:
        logger.error("No CSV file found.")
        raise FileNotFoundError("No CSV file found.")



url = "https://downloads.srwr.scot/export/api/v1/file/01.zip"
fetcher = fetch_presigned_url(url)
get_data = fetch_data(fetcher)
df = load_csv_from_zip(get_data)
print(df.columns)
