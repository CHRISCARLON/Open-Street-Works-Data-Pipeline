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
        limit (int): Number of rows to process in each batch, defaults to 150,000
    """

    # Define field names
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

    limit = batch_limit

    # List to store errors
    errors = []

    # List to accumulate rows for batch processing
    current_batch = []

    # Counter for total rows processed
    total_rows_processed = 0

    try:
        # Download the zip file
        response = requests.get(url)
        response.raise_for_status()

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write the zip file to the temporary directory
            zip_path = os.path.join(temp_dir, 'temp.zip')
            with open(zip_path, 'wb') as zip_file:
                zip_file.write(response.content)

            # Extract the contents of the zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Find the CSV file
            csv_file = None
            for file_name in os.listdir(temp_dir):
                if file_name.endswith('.csv'):
                    csv_file = os.path.join(temp_dir, file_name)
                    break

            if csv_file:
                try:
                    with open(csv_file, 'rb') as file:
                        # Create StringIO buffer to handle line-by-line processing
                        row_buffer = io.StringIO()

                        # Iterate through lines directly from the file
                        lines = (line.decode('utf-8') for line in file)

                        # Skip header
                        next(lines)

                        # Process each line
                        for i, line in enumerate(lines, 1):
                            try:
                                # Write the line to StringIO buffer
                                row_buffer.write(line)

                                # Move cursor to start of buffer
                                row_buffer.seek(0)

                                # Parse the single line with CSV reader
                                reader = csv.DictReader(
                                    [row_buffer.getvalue()],
                                    fieldnames=fieldnames
                                )

                                # Get the parsed row
                                row = next(reader)

                                # Append to current batch
                                current_batch.append(row)

                                # When batch size is reached, process the batch
                                if len(current_batch) >= limit:
                                    # Convert batch to DataFrame
                                    df_chunk = pd.DataFrame(current_batch)

                                    # Process the chunk
                                    process_chunk(df_chunk, conn)

                                    # Update total rows processed
                                    total_rows_processed += len(current_batch)
                                    logger.info(f"Processed rows {total_rows_processed-len(current_batch)+1} to {total_rows_processed}")

                                    # Clear the batch list
                                    current_batch = []

                                # Clear the StringIO buffer
                                row_buffer.truncate(0)
                                row_buffer.seek(0)

                            except Exception as e:
                                error_msg = f"Error processing row {i}: {e}"
                                logger.warning(error_msg)
                                errors.append(error_msg)
                                continue

                        # Process any remaining rows in the final batch
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
                    error_msg = f"Error processing CSV file: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    raise
            else:
                error_msg = "No CSV file found in the zip archive"
                logger.error(error_msg)
                errors.append(error_msg)
                raise FileNotFoundError(error_msg)

    except Exception as e:
        error_msg = f"Error processing the zip file: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        raise

    finally:
        # Print all errors + total amount of errors
        if errors:
            logger.error(f"Total errors encountered: {len(errors)}")
            for error in errors:
                print(error)

        # Log completion
        logger.info(f"Completed processing. Total rows processed: {total_rows_processed}")

    return None
