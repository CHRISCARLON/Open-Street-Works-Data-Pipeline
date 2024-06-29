from loguru import logger

def process_chunk(df, conn):
    
    schema = "os_open_usrns"

    if conn:
        try:
            insert_sql = f"""INSERT INTO "{schema}"."open_usrns_latest" SELECT * FROM df"""
            conn.execute(insert_sql)
        except Exception as e:
            logger.error(f"Error inserting DataFrame into DuckDB: {e}")
            raise
    return None 