import psutil
import os

from memory_profiler import profile
from loguru import logger

from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_credentials import get_secrets
from general_functions.creds import secret_name

from os_open_linked_ids_uprns_usrns.os_open_linked_ids_processor import load_csv_data
from os_open_linked_ids_uprns_usrns.get_redirect_url import fetch_redirect_url
from os_open_linked_ids_uprns_usrns.create_motherduck_table import create_table

@profile
def main(batch_limit: int):
    """
    OS open usrns main will process the latest OS linked open identifiers.
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    # Fetch secrets
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]

    conn = connect_to_motherduck(token, database)

    logger.success("OS OPEN USRN DATA STARTED")
    create_table(conn, schema="os_open_linked_identifiers", name="os_open_linked_identifiers_uprn_usrn_latest")
    url = fetch_redirect_url(url="https://api.os.uk/downloads/v1/products/LIDS/downloads?area=GB&format=CSV&fileName=lids-2024-11_csv_BLPU-UPRN-Street-USRN-11.zip&redirect")
    load_csv_data(url, conn, batch_limit, schema="os_open_linked_identifiers", name="os_open_linked_identifiers_uprn_usrn_latest")
    logger.success("OS OPEN USRN DATA PROCESSED")

    logger.success("OS ROAD TOID USRN DATA STARTED")
    create_table(conn, schema = "os_open_linked_identifiers", name="os_open_linked_identifiers_toid_usrn_road_latest")
    url_2 = fetch_redirect_url(url="https://api.os.uk/downloads/v1/products/LIDS/downloads?area=GB&format=CSV&fileName=lids-2024-11_csv_Road-TOID-Street-USRN-10.zip&redirect")
    load_csv_data(url_2, conn, batch_limit, schema = "os_open_linked_identifiers", name="os_open_linked_identifiers_toid_usrn_road_latest")
    logger.success("OS ROAD TOID USRN DATA PROCESSED")

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)
