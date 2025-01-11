import duckdb
from loguru import logger

def create_table(conn):
    """
    Creates a new table for the latest open roads data every month.
    This will replace the table that is already there.
    Args:
        Connection object
    """
    schema = "os_open_roads"
    if conn:
        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."open_roads_latest" (
                id VARCHAR,
                fictitious VARCHAR,
                road_classification VARCHAR,
                road_function VARCHAR,
                form_of_way VARCHAR,
                road_classification_number VARCHAR,
                name_1 VARCHAR,
                name_1_lang VARCHAR,
                name_2 VARCHAR,
                name_2_lang VARCHAR,
                road_structure VARCHAR,
                length VARCHAR,          
                length_uom VARCHAR,
                loop VARCHAR,
                primary_route VARCHAR,  
                trunk_road VARCHAR,      
                start_node VARCHAR,
                end_node VARCHAR,
                road_number_toid VARCHAR,
                road_name_toid VARCHAR,
                geometry VARCHAR
            );"""
            conn.execute(table_command)
            logger.success("MotherDuck Table created successfully.")
        except (duckdb.ConnectionException, duckdb.DataError, duckdb.Error, Exception) as e:
            logger.error(f"An error occurred: {e}")
            raise