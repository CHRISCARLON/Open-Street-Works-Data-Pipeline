import psutil
import os

from memory_profiler import profile
from loguru import logger

from ..geoplace_swa_codes.create_table_name import get_table_name
from ..geoplace_swa_codes.fetch_swa_codes import (
    get_link,
    fetch_swa_codes,
    validate_data_model,
)
from ..general_functions.create_motherduck_connection import MotherDuckConnector
from ..general_functions.creds import secret_name
from ..general_functions.get_credentials import get_secrets


@ profile
def main():
    """
    This will process the most recent Geoplace SWA Code list data
    """

    # Get the initial memory usage
    initial_memory = psutil.Process(os.getpid()).memory_info().rss
    print(initial_memory)

    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = secrets["motherdb"]
    schema = "geoplace_swa_codes"
    link = get_link()
    table_name = get_table_name(link)

    validate_data_model()

    df = fetch_swa_codes()

    with MotherDuckConnector(token, database) as conn:
        (
            conn.execute_query(f"""CREATE OR REPLACE TABLE "{schema}"."{table_name}" (
                    "SWA Code" VARCHAR,
                    "Account Name" VARCHAR,
                    "Prefix" VARCHAR,
                    "Account Type" VARCHAR,
                    "Registered for Street Manager" VARCHAR,
                    "Account Status" VARCHAR,
                    "Companies House Number" VARCHAR,
                    "Previous Company Names" VARCHAR,
                    "Linked/Parent Company" VARCHAR,
                    "Website" VARCHAR,
                    "Plant Enquiries" VARCHAR,
                    "Ofgem Electricity Licence" VARCHAR,
                    "Ofgem Gas Licence" VARCHAR,
                    "Ofcom Licence" VARCHAR,
                    "Ofwat Licence" VARCHAR,
                    "Company Subsumed By" VARCHAR,
                    "SWA Code of New Company" VARCHAR
                );"""),
        )
        conn.execute_query(
            f"""INSERT INTO "{schema}"."{table_name}" SELECT * FROM df"""
        )

    # Get the final memory usage
    final_memory = psutil.Process(os.getpid()).memory_info().rss
    print(final_memory)
    logger.success("HISTORIC SCOTTISH ROAD WORKS REGISTER DATA PROCESSED")
