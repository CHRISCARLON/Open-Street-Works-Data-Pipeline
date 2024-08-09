import duckdb
from loguru import logger

def connect_to_motherduck(token: str, database: str) -> duckdb.DuckDBPyConnection:
    """
    Creates a connection object to MotherDuck. 
    
    Requires a token and database name. 
    
    Args:
        token
        database

    """
    if token:
        connection_string = f'md:{database}?motherduck_token={token}'
        try:
            con = duckdb.connect(connection_string)
            logger.success("MotherDuck Connection Made")
        except Exception as e:
            logger.warning(f"An error occured with MotherDuck {e}")
            raise
        return con
    else:
        return
