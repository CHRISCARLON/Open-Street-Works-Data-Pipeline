import psutil
import os

from memory_profiler import profile
from loguru import logger

from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_credentials import get_secrets
from general_functions.creds import secret_name

from os_open_linked_ids.os_open_linked_ids_processor import load_csv_data
from os_open_linked_ids.get_redirect_url import fetch_redirect_url
from os_open_linked_ids.create_motherduck_table import create_table_1

@profile
def main(batch_limit: int):
    """
    This will process the latest OS linked open identifiers.
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    # Fetch secrets
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]

    # # 1
    conn_usrn_uprn = connect_to_motherduck(token, database)

    logger.success("OS USRN to URPN DATA STARTED")
    create_table_1(conn_usrn_uprn, schema="os_open_linked_identifiers", name="os_open_linked_identifiers_uprn_usrn_latest")
    url = fetch_redirect_url(url="https://api.os.uk/downloads/v1/products/LIDS/downloads?area=GB&format=CSV&fileName=lids-2024-12_csv_BLPU-UPRN-Street-USRN-11.zip&redirect")
    load_csv_data(url, conn_usrn_uprn, batch_limit, schema="os_open_linked_identifiers", name="os_open_linked_identifiers_uprn_usrn_latest")
    logger.success("OS USRN to UPRN DATA PROCESSED")

    if conn_usrn_uprn:
        conn_usrn_uprn.close()

    # 2
    # conn_road_toid_usrn = connect_to_motherduck(token, database)

    # logger.success("OS ROAD TOID USRN DATA STARTED")
    # create_table_2(conn_road_toid_usrn, schema = "os_open_linked_identifiers", name="os_open_linked_identifiers_toid_usrn_road_latest")
    # url_2 = fetch_redirect_url(url="https://api.os.uk/downloads/v1/products/LIDS/downloads?area=GB&format=CSV&fileName=lids-2024-12_csv_Road-TOID-Street-USRN-10.zip&redirect")
    # load_csv_data(url_2, conn_road_toid_usrn, batch_limit, schema = "os_open_linked_identifiers", name="os_open_linked_identifiers_toid_usrn_road_latest")
    # logger.success("OS ROAD TOID USRN DATA PROCESSED")

    # if conn_road_toid_usrn:
    #     conn_road_toid_usrn.close()

    # 3
    # conn_usrn_topo = connect_to_motherduck(token, database)

    # logger.success("OS USRN TOPO TOID DATA STARTED")
    # create_table_3(conn=conn_usrn_topo, schema = "os_open_linked_identifiers", name="os_open_linked_identifiers_usrn_topo_area_toid_latest")
    # url_3 = fetch_redirect_url(url="https://api.os.uk/downloads/v1/products/LIDS/downloads?area=GB&format=CSV&fileName=lids-2024-12_csv_Street-USRN-TopographicArea-TOID-4.zip&redirect")
    # load_csv_data(url_3, conn_usrn_topo, batch_limit, schema = "os_open_linked_identifiers", name="os_open_linked_identifiers_usrn_topo_area_toid_latest")
    # logger.success("OS USRN TOPO TOID DATA PROCESSED")

    # if conn_usrn_topo:
    #     conn_usrn_topo.close()

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)
