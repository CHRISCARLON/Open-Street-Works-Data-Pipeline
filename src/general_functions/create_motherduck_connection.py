import duckdb
from loguru import logger


def connect_to_motherduck(token: str, database: str):
    """
    Creates a connection object to MotherDuck.

    Requires a token and database name.

    Args:
        token
        database

    Returns:
        MotherDuckd connection
    """
    if token:
        connection_string = f"md:{database}?motherduck_token={token}"
        try:
            con = duckdb.connect(connection_string)
            logger.success("MotherDuck Connection Made")
            return con
        except Exception as e:
            logger.warning(f"An error occured with MotherDuck {e}")
            raise e


class MotherDuckConnector:
    """
    Experimental connector to make future code more reusable.

    The plan is to have everything call this connection instead of the connect to motherduck function above.
    """

    def __init__(self, token: str, database: str):
        """
        Define initial variables.

        Args:
            Motherduck Token
            Database name
        """
        self.token = token
        self.database = database
        self.connection = None

    def motherduck_connect(self) -> duckdb.DuckDBPyConnection:
        try:
            connection_string = f"md:{self.database}?motherduck_token={self.token}"
            self.connection = duckdb.connect(connection_string)
            logger.success("Connection Made")
            return self.connection
        except (duckdb.ConnectionException, duckdb.Error, Exception) as e:
            raise e

    def execute_query(self, query: str):
        if self.connection is None:
            self.motherduck_connect()

        if self.connection:
            return self.connection.execute(query)

    def fetch_data(self, query: str):
        if self.connection is None:
            self.motherduck_connect()

        if self.connection:
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
