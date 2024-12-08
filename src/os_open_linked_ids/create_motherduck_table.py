import duckdb
from loguru import logger

def create_table(conn):
    """
    Creates a new table for the latest open lined identifiers data every month.
    This will replace the table that is already there.

    Args:
        Connection object
    """
    schema = "os_open_linked_identifiers"

    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."os_open_linked_identifiers_latest" (
            CORRELATION_ID VARCHAR,
            IDENTIFIER_1 BIGINT,
            VERSION_NUMBER_1 VARCHAR,
            VERSION_DATE_1 BIGINT,
            IDENTIFIER_2 BIGINT,
            VERSION_NUMBER_2 VARCHAR,
            VERSION_DATE_2 BIGINT,
            CONFIDENCE VARCHAR
            );"""
            conn.execute(table_command)
            logger.success("MotherDuck Table created successfully.")
        except (duckdb.ConnectionException, duckdb.DataError, duckdb.Error, Exception) as e:
            logger.error(f"An error occurred: {e}")
            raise
