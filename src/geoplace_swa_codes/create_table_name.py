import re

def get_table_name(url):
    """
    Create table table from part of url
    """
    
    url_link = url
    
    pattern = r"SWA_CODES_\d{4}-\d{2}-\d{2}"
    
    match = re.search(pattern, url_link)
    if match:
        result = match.group()
        return result
