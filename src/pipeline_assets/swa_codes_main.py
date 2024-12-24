import psutil
import os

from memory_profiler import profile
from loguru import logger

from geoplace_swa_codes.create_table_name import table_name_latest, get_table_name
from geoplace_swa_codes.fetch_swa_codes import (
    get_link,
    fetch_swa_codes,
    validate_data_model,
)
from general_functions.create_motherduck_connection import MotherDuckConnector
from general_functions.creds import secret_name
from general_functions.get_credentials import get_secrets


@ profile
def main():
    """
    This will process the most recent Geoplace SWA Code list data
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(f"Initial Memory: {initial_memory}")

    logger.success("SWA DATA STARTED")

    # Validate that we are getting the data we expect
    validate_data_model()
    logger.info("VALIDATION PASSED")

    # Fetch secrete variables
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = "geoplace_swa_codes"

    # Get table name for whole dataframe entry
    link = get_link()
    table_name = get_table_name(link)

    # Table name for table filtered for Active only
    table_name_active_latest = table_name_latest()

    # Creare swa code dataframe
    df = fetch_swa_codes()

    # Create table and insert data into MotherDuck
    with MotherDuckConnector(token, database) as conn:
        # Create table with snake_case column names
        create_table_query = f"""
        CREATE OR REPLACE TABLE "{schema}"."{table_name}" (
            swa_code VARCHAR,
            account_name VARCHAR,
            prefix VARCHAR,
            account_type VARCHAR,
            registered_for_street_manager VARCHAR,
            account_status VARCHAR,
            companies_house_number VARCHAR,
            previous_company_names VARCHAR,
            linked_parent_company VARCHAR,
            website VARCHAR,
            plant_enquiries VARCHAR,
            ofgem_electricity_licence VARCHAR,
            ofgem_gas_licence VARCHAR,
            ofcom_licence VARCHAR,
            ofwat_licence VARCHAR,
            company_subsumed_by VARCHAR,
            swa_code_of_new_company VARCHAR,
            date_time_processed VARCHAR
        );
        """
        conn.execute_query(create_table_query)

        # Insert data
        insert_query = f"""
        INSERT INTO "{schema}"."{table_name}"
        SELECT * FROM df
        """
        conn.execute_query(insert_query)

        # Create table for ACTIVE ENTRIES ONLY with snake_case column names
        create_table_query = f"""
        CREATE OR REPLACE TABLE "{schema}"."{table_name_active_latest}" (
            swa_code VARCHAR,
            account_name VARCHAR,
            prefix VARCHAR,
            account_type VARCHAR,
            registered_for_street_manager VARCHAR,
            account_status VARCHAR,
            companies_house_number VARCHAR,
            previous_company_names VARCHAR,
            linked_parent_company VARCHAR,
            website VARCHAR,
            plant_enquiries VARCHAR,
            ofgem_electricity_licence VARCHAR,
            ofgem_gas_licence VARCHAR,
            ofcom_licence VARCHAR,
            ofwat_licence VARCHAR,
            company_subsumed_by VARCHAR,
            swa_code_of_new_company VARCHAR,
            date_time_processed VARCHAR
        );
        """
        conn.execute_query(create_table_query)

        # Insert data with filter for active accounts
        insert_query = f"""
        INSERT INTO "{schema}"."{table_name_active_latest}"
        SELECT * FROM df
        WHERE account_status = 'Active'
        """
        conn.execute_query(insert_query)


    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)

    logger.success("SWA DATA PROCESSED")
