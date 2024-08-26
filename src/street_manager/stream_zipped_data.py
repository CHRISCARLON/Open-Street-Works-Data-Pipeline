import requests


def fetch_data(dl_url):
    """
    Stream data from DfT website for the Street Manager data you want

    Args: 
        takes the url for the street manager data for exmaple:
        "https://opendata.manage-roadworks.service.gov.uk/permit/2024/03.zip"
    
    It should return chunks of the data to be processed further.
    """
    with requests.get(dl_url, stream=True, timeout=30) as chunk:
        yield from chunk.iter_content(chunk_size=1048576)
