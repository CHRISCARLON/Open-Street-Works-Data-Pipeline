import psutil
import os

from memory_profiler import profile
from loguru import logger

from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_credentials import get_secrets
from general_functions.creds import secret_name
from os_open_usrn.get_redirect_url import fetch_redirect_url
from os_open_usrn.os_open_usrns_processor import load_geopackage_open_usrns
from os_open_usrn.create_motherduck_table import create_table

@profile
def main(batch_limit: int):
    """
    OS open usrns main will process the latest OS open usrn data.

    USRN = Unique Street Reference Number
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    # Fetch secrets
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]

    # Connect to MotherDuck
    conn = connect_to_motherduck(token, database)

    # Create table
    create_table(conn)

    # Fetch data
    url = fetch_redirect_url()

    # Process data
    load_geopackage_open_usrns(url, conn, batch_limit)

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    logger.success("OS OPEN USRN DATA PROCESSED")
