import psutil
import os
from memory_profiler import profile

from general_functions.create_motherduck_connection import connect_to_motherduck
from general_functions.get_creds import get_secrets
from general_functions.creds import secret_name
from os_open_usrn_functions.get_redirect_url import fetch_redirect_url
from os_open_usrn_functions.os_open_usrns_processor import load_geopackage_open_usrns
from os_open_usrn_functions.create_motherduck_table import create_table

@profile
def main():
    """
    OS open usrns main will process the latest OS open usrn data. 
    
    USRN = Unique Street Reference Number
    """
    
    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    conn = connect_to_motherduck(token, database)
    create_table(conn)
    url = fetch_redirect_url()
    load_geopackage_open_usrns(url, conn)
    
    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    # Calculate the memory usage difference
    memory_usage = final_memory - initial_memory

    # Convert memory usage to megabytes
    memory_usage_mb = memory_usage / (1024 * 1024)
    print(f"Memory usage: {memory_usage_mb:.2f} MB")
    print("OS OPEN USRNS MAIN COMPLETE")
