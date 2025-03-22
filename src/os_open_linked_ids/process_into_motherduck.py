from loguru import logger
import time


def process_chunk(df, conn, schema, name):
    """
    Takes a connection object and a dataframe
    Processes dataframe into MotherDuck table with retry logic

    Args:
        df: Dataframe to process
        conn: Connection object
        schema: Database schema
        name: Table name
        max_retries: Maximum number of retry attempts
        base_delay: Base delay between retries (will be exponentially increased)
    """

    max_retries = 3
    base_delay = 3

    if not conn:
        logger.error("No connection provided")
        return None

    for attempt in range(max_retries):
        try:
            insert_sql = f"""INSERT INTO "{schema}"."{name}" SELECT * FROM df"""
            conn.execute(insert_sql)
            if attempt > 0:  # Log if it succeeded after retries
                logger.success(f"Successfully inserted data on attempt {attempt + 1}")
            return True

        except Exception as e:
            if attempt < max_retries - 1:  # Don't wait on the last attempt
                wait_time = (2**attempt) * base_delay
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed. Final error: {e}")
                raise
