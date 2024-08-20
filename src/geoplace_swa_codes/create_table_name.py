import re
from loguru import logger

def get_table_name(url):
    """
    Create table table from part of url using a Regex expression
    """

    url_link = url

    pattern = r"SWA_CODES_\d{4}-\d{2}-\d{2}"

    match = re.search(pattern, url_link)
    if match:
        logger.success("Table name created")
        result = match.group()
        return result
    else:
        logger.error("The required pattern was not found")
