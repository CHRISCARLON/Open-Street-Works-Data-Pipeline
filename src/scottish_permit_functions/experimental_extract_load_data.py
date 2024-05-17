import requests
import pandas as pd
import csv

from io import BytesIO
from stream_unzip import stream_unzip
from loguru import logger
from tqdm import tqdm


def fetch_presigned_url(api_url) -> str:
    """
    Fetch the pre-signed URL from the initial API endpoint.

    Args:
        api_url (str): The API URL returning the JSON with the pre-signed URL.
    
    Returns:
        str: Pre-signed URL for the ZIP file.
    """
    try: 
        response = requests.get(api_url)
        response.raise_for_status()  
        data = response.json()
        logger.success(f'URL generated: {data}')
        return data['url']
    
    except Exception as e:
        logger.error(f'There has been an error: {e}')
        raise  


def fetch_data(dl_url):
    """
    Stream data from a URL where the CSV data is zipped.

    Args: 
        dl_url (str): The URL for downloading the zipped CSV file.
    
    Returns:
        Iterable of streamed zipped chunks.
    """
    with requests.get(dl_url, stream=True, timeout=30) as response:
        response.raise_for_status()  
        yield from response.iter_content(chunk_size=60000)


def quick_col_rename(df) -> pd.DataFrame:
    """
    Rename columns the...
    
    """
    df = df.rename(columns={'02': "Table Number"})
    return df


def check_data_schema(zipped_chunks, nrows=10):
    """
    Load the first nrows rows from the single CSV file within streamed zipped chunks.

    Args:
        zipped_chunks (Iterable): Streamed zipped chunks.
        nrows (int): Number of rows to load from the CSV.

    Returns:
        DataFrame: A Pandas DataFrame containing the first nrows rows from the CSV.
    """
    found_csv = False  

    for file_name, file_size, file_chunks in stream_unzip(zipped_chunks):
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8')

        if file_name.endswith('.csv'):
            found_csv = True
            logger.success("CSV file found.")
            try:
                csv_content = BytesIO()
                for chunk in file_chunks:
                    csv_content.write(chunk)
                csv_content.seek(0)
                df = pd.read_csv(csv_content, nrows=nrows)
                df = quick_col_rename(df)
                return df
            except Exception as e:
                logger.error(f'There has been an error: {e}')
                raise  

    if not found_csv:
        logger.error("No CSV file found.")
        raise FileNotFoundError("No CSV file found.")


def process_data_rows(collected_rows, data_frames):
    for key, rows in collected_rows.items():
        for row in rows:
            if key in data_frames:
                data_frames[key].loc[len(data_frames[key])] = row
            else:
                data_frames[key] = pd.DataFrame([row])

        logger.info(f"Processed data for key {key}")


def load_data_to_db(df, conn, schema, table_name):
    try:
        full_table_name = f'"{schema}"."{table_name}"'
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} AS SELECT * FROM df LIMIT 0")
        conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM df")
        logger.info(f"Data inserted into {full_table_name}")
    except Exception as e:
        logger.error(f"Error inserting data into {full_table_name}: {e}")
        raise


def process_batches(zipped_chunks, limit_number, conn, schema):
    """
    Streams data from Scottish Road Works Register into MotherDuck. 
    Process data in batches - I'd recommend between 50,000 to 100,000 rows.
    
    Args:
        data to be streamed 
        connection to md
        schema 
        table
    
    """
    batch_limit = limit_number
    total_rows = 0
    collected_rows = {}
    data_frames = {}
    batch_counter = 0
    desired_keys = ["000", "001", "002", "003", 
                    "004", "006", "007", "008", 
                    "009", "010", "036", "041",
                    "098", "099"]
    
    found_csv = False

    for file_name, file_size, unzipped_chunks in tqdm(stream_unzip(zipped_chunks)):
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8')

        if file_name.endswith('.csv'):
            found_csv = True
            logger.info("Processing CSV file:", file_name)
            csv_content = BytesIO()
            
            try:
                for chunk in unzipped_chunks:
                    csv_content.write(chunk)
                csv_content.seek(0)

                reader = csv.reader(csv_content.read().decode('utf-8').splitlines())
                for row in reader:
                    key = row[1]
                    if key in desired_keys:
                        if key not in collected_rows:
                            collected_rows[key] = []
                        collected_rows[key].append(row)
                        total_rows +=1

                        if total_rows >= batch_limit:
                            process_data_rows(collected_rows, data_frames)
                            collected_rows.clear()  
                            # ADD TO MDUCKDB HERE
                            for key, df in data_frames.items():
                                table_name = key  
                                load_data_to_db(df, conn, schema, table_name)
                            data_frames.clear()
                            total_rows = 0  
                            logger.info(f"Processed Batch Number: {batch_counter}")
                            batch_counter+=1

            except Exception as e:
                logger.error(f"Error processing CSV file, {file_name}: {e}")
                raise

    if not found_csv:
        logger.error("No CSV file found.")
        raise FileNotFoundError("No CSV file found.")

    # Process any remaining data
    if collected_rows:
        process_data_rows(collected_rows, data_frames)
        # ADD TO MDUCKDB HERE
        for key, df in data_frames.items():
            table_name = key  
            load_data_to_db(df, conn, schema, table_name)
        logger.success(f"All batches processed. {batch_counter} batches processed.")

    logger.info("Finished processing CSV files into separate data frames based on keys.")
