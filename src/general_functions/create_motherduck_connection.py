import duckdb
from loguru import logger

def connect_to_motherduck(token: str, database: str) -> duckdb.DuckDBPyConnection:
    """
    Creates a connection object to MotherDuck. 
    
    Requires a token and database name. 
    
    Args:
        token
        database
        
    Returns:
        duckdb.DuckDBPyConnection
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

class MotherDuckConnector:
    """
    Experimental connector to make future code more reusable.
    """
    def __init__(self, token: str, databse: str):
        self.token = token
        self.database = databse
        self.connection = None

    def motherduck_connect(self) -> duckdb.DuckDBPyConnection:
        try:
            connection_string = f'md:{self.database}?motherduck_token={self.token}'
            self.connection = duckdb.connect(connection_string)
            logger.success("Connection Made")
            return self.connection
        except (duckdb.ConnectionException, duckdb.Error, Exception) as e:
            raise e 
        
    def execute_query(self, query: str):
        if self.connection is None:
            self.connect()
        return self.connection.execute(query)

    def fetch_data(self, query: str):
        if self.connection is None:
            self.connect()
        return self.connection.execute(query).fetchall()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("MotherDuck Connection Closed")

    def __enter__(self):
        self.motherduck_connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()