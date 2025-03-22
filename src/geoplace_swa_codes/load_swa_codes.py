import duckdb
from loguru import logger

# This is deprecated and has been replaced by
# src/general_functions/create_motherduck_connection.py
# MotherDuck Connector


def create_table_swa_codes_motherduck(conn, table_name):
    """
    Creates a new table for the latest open usrn data every month.
    This will replace the table that is already there.

    Args:
        Connection object
        Table name
    """
    schema = "geoplace_swa_codes"
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."{table_name}" (
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
                "SWA Code of New Company" INT
            );"""
            conn.execute(table_command)
            logger.success("MotherDuck Table created successfully.")
        except (
            duckdb.ConnectionException,
            duckdb.DataError,
            duckdb.Error,
            Exception,
        ) as e:
            logger.error(f"An error occurred: {e}")
            raise


def load_swa_code_data_motherduck(df, conn, table):
    """
    Inserts a DataFrame into a DuckDB table.

    This function also ensures that the order of the columns is always the same.

    Args:
        df: The DataFrame to be inserted.
        conn: The MotherDuck connection.
        schema: The schema of the table.
        table: The name of the table.
    """
    schema = "geoplace_swa_codes"
    try:
        insert_sql = f"""INSERT INTO "{schema}"."{table}" SELECT * FROM df"""
        conn.execute(insert_sql)
    except (duckdb.DataError, duckdb.Error, Exception) as e:
        logger.error(f"Error inserting DataFrame into DuckDB: {e}")
        raise
