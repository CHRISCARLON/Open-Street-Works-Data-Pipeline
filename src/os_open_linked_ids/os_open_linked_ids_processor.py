import os
import tempfile
import zipfile
import requests
import csv
import io
import pandas as pd
from loguru import logger
from .process_into_motherduck import process_chunk

def load_csv_data(url: str, conn, batch_limit: int):
    """
    Function to stream and process CSV data in batches.

    Args:
        url (str): URL of the zipped CSV file
        conn: DuckDB connection object
        batch_limit (int): Number of rows to process in each batch
    """
    fieldnames = [
        'CORRELATION_ID',
        'IDENTIFIER_1',
        'VERSION_NUMBER_1',
        'VERSION_DATE_1',
        'IDENTIFIER_2',
        'VERSION_NUMBER_2',
        'VERSION_DATE_2',
        'CONFIDENCE'
    ]

    errors = []
    current_batch = []
    total_rows_processed = 0

    try:
        # Get url
        response = requests.get(url)
        response.raise_for_status()

        # Create the temp dir as the OS download has several files in it
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, 'temp.zip')
            with open(zip_path, 'wb') as zip_file:
                zip_file.write(response.content)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Get the csv file that has the data in it
            csv_file = next(
                (os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.csv')),
                None
            )

            if not csv_file:
                error_msg = "No CSV file found in the zip archive"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            with open(csv_file, 'r', newline='') as file:
                # Create a single CSV reader for the entire file
                reader = csv.DictReader(file, fieldnames=fieldnames)
                next(reader)  # Skip header

                for i, row in enumerate(reader, 1):
                    try:
                        current_batch.append(row)

                        if len(current_batch) >= batch_limit:
                            df_chunk = pd.DataFrame(current_batch)
                            process_chunk(df_chunk, conn)

                            total_rows_processed += len(current_batch)
                            logger.info(f"Processed rows {total_rows_processed-len(current_batch)+1} to {total_rows_processed}")

                            current_batch = []

                    except Exception as e:
                        error_msg = f"Error processing row {i}: {e}"
                        logger.warning(error_msg)
                        errors.append(error_msg)
                        continue

                # Process remaining rows
                if current_batch:
                    try:
                        df_chunk = pd.DataFrame(current_batch)
                        process_chunk(df_chunk, conn)
                        total_rows_processed += len(current_batch)
                        logger.info(f"Processed final batch: rows {total_rows_processed-len(current_batch)+1} to {total_rows_processed}")

                    except Exception as e:
                        error_msg = f"Error processing final batch: {e}"
                        logger.error(error_msg)
                        errors.append(error_msg)

    except Exception as e:
        error_msg = f"Error processing the zip file: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        raise

    finally:
        if errors:
            logger.error(f"Total errors encountered: {len(errors)}")
            for error in errors:
                print(error)

        logger.info(f"Completed processing. Total rows processed: {total_rows_processed}")

    return None
