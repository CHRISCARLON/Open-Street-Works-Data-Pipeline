import json
import pandas as pd

from .flatten_street_manager_data import flatten_json
from stream_unzip import stream_unzip
from loguru import logger
from tqdm import tqdm


def quick_col_rename(df) -> pd.DataFrame:
    """
    Need to rename the columns following the flatten json function output

    This is because a lot of data was initially nested within "object_data"

    This will remove "object data." from the column names.
    """
    df.columns = [col.replace("object_data.", "") if "object_data." in col else col for col in df.columns]
    return df


def insert_dataframe_to_motherduck(df, conn, schema, table):
    """
    Inserts a DataFrame into a MotherDuck table.

    This function also ensures that the order of the columns is always the same.

    Args:
        df: The DataFrame to be inserted.
        conn: The MotherDuck connection.
        schema: The schema of the table.
        table: The name of the table.
    """
    try:
        column_names = df.columns.tolist()
        columns_sql = ', '.join(column_names)
        placeholders = ', '.join([f"df.{name}" for name in column_names])
        insert_sql = f"""INSERT INTO "{schema}"."{table}" ({columns_sql}) SELECT {placeholders} FROM df"""
        conn.execute(insert_sql)
    except Exception as e:
        logger.error(f"Error inserting DataFrame into DuckDB: {e}")
        raise


def process_batch_and_insert_to_motherduck(zipped_chunks, limit_numbner, conn, schema, table):
    """
    Streams data from DfT into MotherDuck.
    Process data in batches of [whatever you decide].
    Usually around 1 million jsons to proccess per month.

    Args:
        data to be streamed
        connection to md
        schema
        table
    """
    batch_limit = limit_numbner
    batch_count = 0
    flattened_data = []
    current_file = None
    current_item = None

    for file, size, unzipped_chunks in tqdm(stream_unzip(zipped_chunks)):
        current_file = file
        if isinstance(current_file, bytes):
            current_file = current_file.decode('utf-8')

        try:
            bytes_obj = b''.join(unzipped_chunks)
            json_data = json.loads(bytes_obj.decode('utf-8'))
            current_item = flatten_json(json_data)
            flattened_data.append(current_item)
            batch_count += 1

            # Process and insert data in batches
            if batch_count >= batch_limit:
                df = pd.DataFrame(flattened_data)
                df = df.fillna('NULL')
                df = quick_col_rename(df)
                # Insert the batch into MothertDuck
                insert_dataframe_to_motherduck(df, conn, schema, table)
                logger.success("Batch processed!")
                # Reset the batch for the next iteration
                flattened_data.clear()
                batch_count = 0
                current_item = None 

        except Exception as e:
            logger.error(f"{e}")
            logger.error(f"Error processing file: {current_file}")
            if current_item:
                logger.error("Last processed item:")
                debug_df = pd.DataFrame([current_item]) 
                print(debug_df)
                print(debug_df.dtypes)
            raise

    # Process the remaining data
    try:
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            df = df.fillna('NULL')
            df = quick_col_rename(df)
            # Insert the batch into MothertDuck
            insert_dataframe_to_motherduck(df, conn, schema, table)
            logger.success("Batch processed!")
        logger.success("Data processing complete - all batches have been processed")

    except Exception as e:
        logger.error(f"{e}")
        logger.error("Error processing final batch")
        if flattened_data:
            logger.error(f"Number of items in final batch: {len(flattened_data)}")
            last_item = flattened_data[-1] if flattened_data else None
            if last_item:
                debug_df = pd.DataFrame([last_item])
                print(debug_df)
                print(debug_df.dtypes)
        raise
