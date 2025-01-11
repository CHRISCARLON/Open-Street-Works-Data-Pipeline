import requests
import csv
import json

from io import BytesIO
from stream_unzip import stream_unzip
from loguru import logger


def fetch_presigned_url(api_url) -> str:
    """
    Fetch the pre-signed URL from the initial API endpoint.

    Args:
        api_url (str): The API URL returning the JSON with the pre-signed URL.
    
    Returns:
        str: Pre-signed URL for the ZIP file. This is the url required to actually 
        download the data. 
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


def load_data_to_db(collected_rows, conn, schema, table_name):
    """
    Load data into motherduck...
    
    Should be loaded in like this...
    
    col1     col2
    001      list of lists containing values
    001      list of lists containing values 
    003      list of lists containing values
    005      list of lists containing values 
    
    Use begin transaction and commit as you're loading in batches of data within a loop. 
    
    We can use DBT later to create the necessary tables after the initial load. 
    """
    full_table_name = f'"{schema}"."{table_name}"'

    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} (table_id VARCHAR, column_values JSON)")
        
        conn.execute("BEGIN TRANSACTION")
        
        for table_id, rows in collected_rows.items():
            values = []
            for row in rows:
                escaped_row = [str(value).replace("'", "''") for value in row]
                values.append(f"('{table_id}', '{json.dumps(escaped_row)}')")
            
            if values:
                conn.execute(f"INSERT INTO {full_table_name} (table_id, column_values) VALUES {', '.join(values)}")
        
        conn.execute("COMMIT")
        
        logger.info(f"Data inserted into {full_table_name}")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Error inserting data into {full_table_name}: {e}")
        raise


def process_batches(zipped_chunks, limit_number, conn, schema):
    """
    Streams data from Scottish Road Works Register into MotherDuck. 
    Process data in batches - I'd recommend between 50,000 to 100,000 rows.
    This depends on what period of permit data you ingesting though. 
    
    Args:
        data to be streamed 
        conn to motherduck
        schema 
        table
    
    """
    batch_limit = limit_number
    total_rows = 0
    collected_rows = {}
    batch_counter = 0
    desired_keys = ["000", "001", "002", "003", 
                    "004", "006", "007", "008", 
                    "009", "010", "036", "041",
                    "098", "099"]
    
    found_csv = False
    current_csv_name = None 

    for file_name, file_size, unzipped_chunks in stream_unzip(zipped_chunks):
        if isinstance(file_name, bytes):
            file_name = file_name.decode('utf-8')

        if file_name.endswith('.csv'):
            found_csv = True
            current_csv_name = file_name
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
                            table_name = file_name
                            load_data_to_db(collected_rows, conn, schema, table_name)
                            collected_rows.clear()
                            total_rows = 0
                            logger.info(f"Processed Batch Number: {batch_counter}")
                            batch_counter += 1

            except Exception as e:
                logger.error(f"Error processing CSV file, {file_name}: {e}")
                raise

    if not found_csv:
        logger.error("No CSV file found.")
        raise FileNotFoundError("No CSV file found.")

    if collected_rows:
        table_name = current_csv_name
        load_data_to_db(collected_rows, conn, schema, table_name)
        collected_rows.clear()
        total_rows = 0
        batch_counter += 1
        logger.success(f"All batches processed. {batch_counter} batches processed.")

    logger.info("Finished processing CSV file.")
