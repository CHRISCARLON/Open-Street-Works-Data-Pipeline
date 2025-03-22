import requests
from loguru import logger


def fetch_redirect_url() -> str:
    """
    Call the redirect url and then fetch the actual download url.
    This is suboptimal and will change in future versions.
    """
    url = "https://api.os.uk/downloads/v1/products/OpenUSRN/downloads?area=GB&format=GeoPackage&redirect"
    try:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        logger.success(f"The Redirect URL is: {redirect_url}")
    except (requests.exceptions.RequestException, ValueError, Exception) as e:
        logger.error(f"An error retrieving the redirect URL: {e}")
        raise
    return redirect_url
