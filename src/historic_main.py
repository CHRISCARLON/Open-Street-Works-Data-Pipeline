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
    
    # Get initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss

    # Generate links
    links = generate_monthly_download_links(2023, 13)

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

        # Process permit data
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
