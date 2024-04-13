import psutil
import os
from memory_profiler import profile

from functions.generate_dl_link import generate_dl_link
from functions.motherduck_connection import connect_to_motherduck
from functions.get_creds import get_secrets
from functions.extract_load_data import fetch_data, process_batch_and_insert_to_duckdb


@profile
def main():
    """
    Main function to control flow and log memory usage.
    """
    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss

    # AWS credentials, S3 bucket, and MotherDuck details
    secrets = get_secrets("streetmanagerpipeline")
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = secrets["schema"]
    table = secrets["table"]

    conn = connect_to_motherduck(token, database)

    link = generate_dl_link()
    data = fetch_data(link)
    process_batch_and_insert_to_duckdb(data, conn, schema, table)

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    # Calculate the memory usage difference
    memory_usage = final_memory - initial_memory

    # Convert memory usage to megabytes
    memory_usage_mb = memory_usage / (1024 * 1024)

    print(f"Memory usage: {memory_usage_mb:.2f} MB")


if __name__ =="__main__":
    main()
