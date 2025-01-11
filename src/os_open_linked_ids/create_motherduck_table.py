import duckdb
from loguru import logger

# TODO this needs rolled up into a more abstract, reuseable function
# This is just done quickly to make sure the pipeline works

def create_table_1(conn, schema, name):
    """
    Creates a new table for the latest open linked identifiers data every month.
    This will replace the table that is already there.

    Args:
        Connection object
        schema
        table name
    """
    
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."{name}" (
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

def create_table_2(conn, schema, name):
    """
    Creates a new table for the latest open linked identifiers data every month.
    This will replace the table that is already there.

    Args:
        Connection object
        schema
        table name
    """
    
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."{name}" (
            CORRELATION_ID VARCHAR,
            IDENTIFIER_1 VARCHAR,
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

def create_table_3(conn, schema, name):
    """
    Creates a new table for the latest open linked identifiers data every month.
    This will replace the table that is already there.

    Args:
        Connection object
        schema
        table name
    """
    
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."{name}" (
            CORRELATION_ID VARCHAR,
            IDENTIFIER_1 BIGINT,
            VERSION_NUMBER_1 VARCHAR,
            VERSION_DATE_1 BIGINT,
            IDENTIFIER_2 VARCHAR,
            VERSION_NUMBER_2 BIGINT,
            VERSION_DATE_2 BIGINT,
            CONFIDENCE VARCHAR
            );"""
            conn.execute(table_command)
            logger.success("MotherDuck Table created successfully.")
        except (duckdb.ConnectionException, duckdb.DataError, duckdb.Error, Exception) as e:
            logger.error(f"An error occurred: {e}")
            raise
