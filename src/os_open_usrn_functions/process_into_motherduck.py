from loguru import logger
from general_functions.creds import open_usrn_schema

def process_chunk(df, conn):
    
    schema = open_usrn_schema

    if conn:
        try:
            insert_sql = f"""INSERT INTO "{schema}"."open_usrns_latest" SELECT * FROM df"""
            conn.execute(insert_sql)
        except Exception as e:
            logger.error(f"Error inserting DataFrame into DuckDB: {e}")
            raise
    return None 