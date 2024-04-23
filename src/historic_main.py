import psutil
import os
from memory_profiler import profile
from loguru import logger

from functions.historic_main_links import generate_monthly_download_links
from functions.motherduck_create_table import motherduck_create_table
from functions.motherduck_connection import connect_to_motherduck
from functions.get_creds import get_secrets
from functions.extract_load_data import (
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
def main():
    
    """
    Historic Main will process a batch of months.
    
    Specify a time frame and process 1 to 12 months of data for a particular year.  
    
    generate_monthly_download_links(2023, 7, 13) = data for July 2023 to December 2023
    
    """
    
    # Get initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss

    # Generate links
    # Pick a year, pick a starting month (e.g. 2 would be Feb), pick and end month (e.g. 13 would be Dec)
    # 13 = December due to Python indexing 
    links = generate_monthly_download_links(2023, 7, 13)

    # Credentials for MotherDuck
    secrets = get_secrets("streetmanagerpipeline")
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = secrets["schema_23"]
    
    # Initiate motherduck connection
    conn = connect_to_motherduck(token, database)

    for link in links:
        # Extract month and year from link for MotherDuck table
        parts = link.split('/')
        year = parts[-2]
        month = parts[-1].replace('.zip', '')
        table = f"{month}_{year}"  

        # Create MotherDuck table
        motherduck_create_table(conn, schema, table)

        # Quick, basic check of permit data schema
        test_data = fetch_data(link)
        test_df = check_data_schema(test_data)
        test_df = quick_col_rename(test_df)
        validate = validate_dataframe_sample(test_df, StreetManagerPermitModel)
        handle_validation_errors(validate)
        print(test_df.dtypes)

        # If checks pass, then process permit data
        permit_data = fetch_data(link)
        process_batch_and_insert_to_duckdb(permit_data, conn, schema, table)
        logger.success(f"Data for {table} has been processed!")

    # Get final, high level memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    memory_usage = final_memory - initial_memory
    memory_usage_mb = memory_usage / (1024 * 1024)
    logger.success("Data for all links have been processed!")
    print(f"Memory usage: {memory_usage_mb:.2f} MB")

if __name__ =="__main__":
    main()
