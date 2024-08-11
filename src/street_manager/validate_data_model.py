import json
import pandas as pd

from .flatten_street_manager_data import flatten_json

from stream_unzip import stream_unzip
from loguru import logger
from tqdm import tqdm

def check_data_schema(zipped_chunks):
    """
    Reads 50 JSON files from zipped chunks and returns a Pandas DataFrame.
    
    This is so you can assess the data structure and validate part of it against the Pydantic model. 
    
    Args:
        zipped_chunks: Iterable of zipped chunks containing JSON files.
    """
    max_files = 500
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
            logger.error(f"Error processing {file}: {e}")
            raise

    # Create DataFrame from the collected data
    df = pd.DataFrame(data_list)
    return df
