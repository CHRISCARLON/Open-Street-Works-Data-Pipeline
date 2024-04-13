import duckdb
from loguru import logger

def connect_to_motherduck(token: str, database: str):
    """
    Create connection object to MotherDuck. 
    Requires a token and database name. 
    
    Args:
        token
        database

    """
    if token is None:
        raise ValueError("MotherDuck token not found in environment variables")

    connection_string = f'md:{database}?motherduck_token={token}'
    try:
        con = duckdb.connect(connection_string)
        logger.success("Connection Made")
    except Exception as e:
        logger.warning(f"An error occured {e}")
        raise
    return con
