import requests
from loguru import logger

def fetch_redirect_url() -> str:
    url = "https://api.os.uk/downloads/v1/products/OpenUSRN/downloads?area=GB&format=GeoPackage&redirect"
    try:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        logger.success(f"The Redirect URL is: {redirect_url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error retrieving the redirect URL: {e}")
        raise
    return redirect_url

if __name__=="__main__":
    test = fetch_redirect_url()
    print(test)
