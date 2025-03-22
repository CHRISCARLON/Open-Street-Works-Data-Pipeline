import psutil
import os

from memory_profiler import profile
from loguru import logger

from general_functions.create_motherduck_connection import connect_to_motherduck
from src.auth.get_credentials import get_secrets
from src.auth.creds import secret_name
from scottish_road_works_register.generate_dl_links import dl_link_creator
from scottish_road_works_register.experimental_extract_load_data import (
    fetch_presigned_url,
    fetch_data,
    process_batches,
)


@profile
def main(schema_name, batch_limit):
    """
    This will process all Scottish Road Works Register data from a specified range

    Args:
        Schema name
        Batch limit - set this to specify the chunk size for processing (e.g. process in chunks of 100,000 files)
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    # Credentials for MotherDuck
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = secrets[schema_name]

    # Set up connection and process data
    conn = connect_to_motherduck(token, database)
    url = dl_link_creator("15")
    dl_url = fetch_presigned_url(url)
    dl_data = fetch_data(dl_url)
    process_batches(dl_data, batch_limit, conn, schema)

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    logger.success("HISTORIC SCOTTISH ROAD WORKS REGISTER DATA PROCESSED")
