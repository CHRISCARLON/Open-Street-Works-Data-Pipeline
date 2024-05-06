import psutil
import os
from memory_profiler import profile

from permit_functions.generate_dl_link import generate_dl_link
from permit_functions.date_month import date_for_table
from permit_functions.motherduck_create_table import motherduck_create_table
from permit_functions.motherduck_connection import connect_to_motherduck
from permit_functions.get_creds import get_secrets
from permit_functions.creds import secret_name
from permit_functions.extract_load_data import (
    fetch_data, 
    process_batch_and_insert_to_duckdb, 
    check_data_schema, 
    quick_col_rename
    )
from pydantic_model.street_manager_model import (
    StreetManagerPermitModel,
    validate_dataframe_sample,
    handle_validation_errors
)


@profile
def main(schema_name):
    
    """
    Monthly Permit Main will process the latest Street Manager Permit data. 
    
    If you run this in April then you will generate the D/L link for March's data.
    
    """
    
    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss

    # Quick, basic check of the Street Manager schema before continuing!
    test_link = generate_dl_link()
    test_data = fetch_data(test_link)
    test_df = check_data_schema(test_data)
    test_df = quick_col_rename(test_df)
    validate = validate_dataframe_sample(test_df, StreetManagerPermitModel)
    handle_validation_errors(validate)
    print(test_df.dtypes)

    # Credentials for MotherDuck
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = secrets[schema_name]
    
    # Create MotherDuck table date
    table = date_for_table()

    # Initiate motherduck connection and table 
    conn = connect_to_motherduck(token, database)
    motherduck_create_table(conn, schema, table)

    # Start data processing
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
    # Define schema for the current year
    main("schema_24")

