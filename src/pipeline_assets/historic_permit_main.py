import psutil
import os

from memory_profiler import profile
from loguru import logger

from ..street_manager.historic_main_links import generate_monthly_download_links
from ..street_manager.motherduck_create_table import motherduck_create_table
from ..general_functions.create_motherduck_connection import connect_to_motherduck
from ..general_functions.get_credentials import get_secrets
from ..general_functions.creds import secret_name
from ..street_manager.extract_load_data import (
    process_batch_and_insert_to_motherduck,
    quick_col_rename
    )
from ..street_manager.stream_zipped_data import fetch_data
from ..street_manager.validate_data_model import check_data_schema
from ..pydantic_model.street_manager_model import (
    StreetManagerPermitModel,
    validate_dataframe_sample,
    handle_validation_errors
)

@profile
def main(schema_name, limit_number, year_int, start_month_int, end_month_int):

    """
    Historic Permit Main will process batches of historic data.

    Specify a time period and process 1 to 12 months of data for a particular year and/or years.

    Example usgae:

    main("schema_23", 2023, 1, 13) - will process all data from 2023

    main("schema_21", 2021, 7, 13) - will only process data from July 2021 to December 2021
    """

    # Get initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    # Generate links
    # Pick a year, pick a starting month (e.g. 2 would be Feb), pick and end month (e.g. 13 would be Dec)
    # 13 = December due to Python indexing
    links = generate_monthly_download_links(year_int, start_month_int, end_month_int)

    # Credentials for MotherDuck
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = secrets[schema_name]

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
        process_batch_and_insert_to_motherduck(permit_data, limit_number, conn, schema, table)
        logger.success(f"Data for {table} has been processed!")

    # Get final, high level memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)
    logger.success("HISTORIC DATA HAS BEEN PROCESSED")
