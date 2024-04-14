import psutil
import os
from memory_profiler import profile

from functions.generate_dl_link import generate_dl_link
from functions.date_month import date_for_table
from functions.motherduck_connection import connect_to_motherduck
from functions.get_creds import get_secrets
from functions.extract_load_data import fetch_data, process_batch_and_insert_to_duckdb
from functions.motherduck_create_table import motherduck_create_table


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
    schema = secrets["schema_24"]
    
    # Create table variable
    table = date_for_table()

    # Initiate motherduck connection and table 
    conn = connect_to_motherduck(token, database)
    
    motherduck_create_table(conn, schema, table)

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
