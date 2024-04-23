import json
import requests
import pandas as pd

from stream_unzip import stream_unzip
from loguru import logger
from tqdm import tqdm


def fetch_data(dl_url):
    """
    Stream data from DfT website for the street manager data you want

    Args: 
        takes the url for the street manager data for exmaple:
        "https://opendata.manage-roadworks.service.gov.uk/permit/2024/03.zip"
    
    It should return chunks of the data to be processed further
    
    """
    with requests.get(dl_url, stream=True, timeout=30) as chunk:
        yield from chunk.iter_content(chunk_size=6500)


def flatten_json(json_data) -> dict:
    """
    Street manager archived open data comes in nested json files
    This function flattens the structure
    
    Args:
        json_data to flatten
    
    """
    flattened_data = {}

    def flatten(data, prefix=''):
        if isinstance(data, dict):
            for key in data:
                flatten(data[key], f'{prefix}{key}.')
        else:
            flattened_data[prefix[:-1]] = data

    flatten(json_data)
    return flattened_data


def quick_col_rename(df) -> pd.DataFrame:
    """
    Need to rename the columns following the flatten json function output
    
    This is because a lot of data was initially nested within "object_data"
    
    This will remove "object data." from the column names
    
    """
    df.columns = [col.replace("object_data.", "") if "object_data." in col else col for col in df.columns]
    return df


def check_data_schema(zipped_chunks):
    """
    Reads 50 JSON files from zipped chunks and returns a Pandas DataFrame.
    
    This is so you can assess the data structure and become familiar with it. 
    
    Args:
        zipped_chunks: Iterable of zipped chunks containing JSON files.
    
    """
    max_files = 50
    file_count = 0
    data_list = []

    for file, size, unzipped_chunks in tqdm(stream_unzip(zipped_chunks)):
        if file_count >= max_files:
            break

        if isinstance(file, bytes):
            file = file.decode('utf-8')

        try:
            # Decode bytes to string and load into JSON
            bytes_obj = b''.join(unzipped_chunks)
            json_data = json.loads(bytes_obj.decode('utf-8'))
            # Flatten the JSON data if necessary and add to list
            flattened_data = flatten_json(json_data)
            data_list.append(flattened_data)
            file_count += 1

        except Exception as e:
            print(f"Error processing {file}: {e}")
            raise

    # Create DataFrame from the collected data
    df = pd.DataFrame(data_list)
    return df


def process_batch_and_insert_to_duckdb(zipped_chunks, conn, schema, table):
    """
    Streams data from DfT into MotherDuck. 
    Process data in batches of 50,000.
    Usually around 1 million jsons to proccess per month. 
    
    Args:
        data to be streamed 
        connection to md
        schema 
        table
    
    """
    batch_limit = 50000
    batch_count = 0
    flattened_data = []

    for file, size, unzipped_chunks in tqdm(stream_unzip(zipped_chunks)):
        if isinstance(file, bytes):
            file = file.decode('utf-8')

        try:
            bytes_obj = b''.join(unzipped_chunks)
            json_data = json.loads(bytes_obj.decode('utf-8'))
            flattened_item = flatten_json(json_data)
            flattened_data.append(flattened_item)
            batch_count += 1

            # Process and insert data in batches
            if batch_count >= batch_limit:
                df = pd.DataFrame(flattened_data)
                df = df.fillna('NULL')
                df = quick_col_rename(df)
                # Insert the batch into DuckDB
                column_names = df.columns.tolist()
                columns_sql = ', '.join(column_names)
                placeholders = ', '.join([f"df.{name}" for name in column_names])
                insert_sql = f"""INSERT INTO "{schema}"."{table}" ({columns_sql}) SELECT {placeholders} FROM df"""
                conn.execute(insert_sql)
                logger.success("Batch processed!")
                # Reset the batch for the next iteration
                flattened_data = []
                batch_count = 0

        except Exception as e:
            logger.error(f"{e}")
            logger.error(f"Error processing {file} with data: {flattened_item}")
            debug_df = pd.DataFrame(flattened_item)
            print(debug_df)
            print(debug_df.dtypes)
            raise

    try:
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            df = df.fillna('NULL')
            df = quick_col_rename(df)
            # Insert the remaining data into DuckDB
            column_names = df.columns.tolist()
            columns_sql = ', '.join(column_names)
            placeholders = ', '.join([f"df.{name}" for name in column_names])
            insert_sql = f"""INSERT INTO "{schema}"."{table}" ({columns_sql}) SELECT {placeholders} FROM df"""
            conn.execute(insert_sql)
            logger.success("Batch processed!")
        logger.success("Data processing complete - all batches have been processed")

    except Exception as e:
        logger.error(f"{e}")
        logger.error(f"Error processing {file} with data: {flattened_item}")
        debug_df = pd.DataFrame(flattened_item)
        print(debug_df)
        print(debug_df.dtypes)
        raise
