import duckdb
from loguru import logger

def create_table(conn): 
    """
    Creates a new table for the latest open usrn data every month.
    This will replace the table that is already there.
    Takes a connection object. 
    """
    schema = "os_open_usrns"
    
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."open_usrns_latest" (
                geometry VARCHAR,
                street_type VARCHAR, 
                usrn BIGINT
            );"""
            conn.execute(table_command)
            logger.success("MotherDuck Table created successfully.")
        except (duckdb.ConnectionException, duckdb.DataError, duckdb.Error, Exception) as e:
            logger.error(f"An error occurred: {e}")
            raise