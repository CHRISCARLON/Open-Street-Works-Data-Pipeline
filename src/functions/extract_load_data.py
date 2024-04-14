import json
import requests
import pandas as pd

from stream_unzip import stream_unzip
from loguru import logger
from tqdm import tqdm


def fetch_data(dl_url):
    """
    Stream unzip data from DfT website for the street manager data you want

    Args: 
        takes the url for the street manager data for exmaple:
        "https://opendata.manage-roadworks.service.gov.uk/permit/2024/03.zip"
        
    """
    with requests.get(dl_url, stream=True, timeout=30) as chunk:
        yield from chunk.iter_content(chunk_size=6500)


def flatten_json(json_data):
    """
    Street manager archived open data comes in nested json files.
    This function flattens the structure.
    
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


def check_data_schema(zipped_chunks):
    """
    Reads up to 10 JSON files from zipped chunks and returns a Pandas DataFrame.
    This is so you can assess the data structure. 
    
    Args:
        zipped_chunks: Iterable of zipped chunks containing JSON files.
    
    """
    max_files = 10
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
    Streams data from DfT into mother duck. 
    Process in batches of 50,000.
    Usually around 1 million jsons to proccess. 
    
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
                df = df.fillna('No Value')
                # Insert the batch into DuckDB
                conn.execute(f"""INSERT INTO "{schema}"."{table}" SELECT * FROM df""")
                logger.success("Batch processed!")
                # Reset the batch for the next iteration
                flattened_data = []
                batch_count = 0

        except Exception as e:
            logger.warning(f"Error processing {file}: {e}")
            raise

    # Insert any remaining data after exiting the loop
    if flattened_data:
        df = pd.DataFrame(flattened_data)
        df = df.fillna('No Value')
        # Insert the remaining data into DuckDB
        conn.execute(f"""INSERT INTO "{schema}"."{table}" SELECT * FROM df""")
        logger.success("Final batch processed!")

    logger.success("Data processing complete - all batches processed")
