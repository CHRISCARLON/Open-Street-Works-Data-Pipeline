import requests
import os
import tempfile
import zipfile
import pandas as pd
import fiona
from shapely import wkt
from shapely.geometry import shape
from loguru import logger
from tqdm import tqdm

def insert_into_motherduck(df, conn, schema: str, table: str):
    """
    Processes dataframe into MotherDuck table

    Args:
        Dataframe
        Connection object
        Schema name
        Table name
    """

    if conn:
        try:
            insert_sql = (
                f"""INSERT INTO "{schema}"."{table}" SELECT * FROM df"""
            )
            conn.execute(insert_sql)
            logger.success(f"Inserted {len(df)} rows into {schema}.{table}")
        except Exception as e:
            logger.error(f"Error inserting DataFrame into DuckDB: {e}")
            raise
    return None

def fetch_redirect_url(url: str) -> str:
    """
    Call the redirect url and then fetch the actual download url.
    This is suboptimal and will change in future versions.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        logger.success(f"The Redirect URL is: {redirect_url}")
    except (requests.exceptions.RequestException, ValueError, Exception) as e:
        logger.error(f"An error retrieving the redirect URL: {e}")
        raise
    return redirect_url

def load_geopackage_open_usrns(url: str, conn, batch_size: int, schema: str, table: str):
    """
    Function to load OS open usrn data in batches of 50,000 rows.

    It taskes a duckdb connection object and the download url required.

    Args:
        Url for data
        Connection object
    """

    # This can be changed based on how much memory you want to use overall
    chunk_size = batch_size

    # List to store errors
    errors = []

    try:
        # Download the zip file
        logger.info("Downloading zip file...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write the zip file to the temporary directory
            zip_path = os.path.join(temp_dir, "temp.zip")
            with open(zip_path, "wb") as zip_file:
                zip_file.write(response.content)

            logger.info("Extracting zip file...")
            # Extract the contents of the zip file
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            # Find the GeoPackage file
            gpkg_file = None
            for file_name in os.listdir(temp_dir):
                if file_name.endswith(".gpkg"):
                    gpkg_file = os.path.join(temp_dir, file_name)
                    logger.success(f"The GeoPackage file is: {gpkg_file}")
                    break

            if gpkg_file:
                try:
                    # Open the GeoPackage file
                    with fiona.open(gpkg_file, "r") as src:
                        # Print some of the metadata to check everything is OK
                        crs = src.crs
                        schema = src.schema

                        logger.info(f"The CRS is: {crs}")
                        logger.info(f"The Schema is: {schema}")
                        
                        # Get total number of features for the progress bar
                        total_features = len(src)
                        logger.info(f"Total features to process: {total_features}")

                        # List to store extracted features for DataFrame processing
                        features = []

                        # Use tqdm for progress tracking
                        for i, feature in enumerate(tqdm(src, total=total_features, desc="Processing features")):
                            try:
                                # Convert geometry to WKT string
                                geom = shape(feature["geometry"])
                                feature["properties"]["geometry"] = wkt.dumps(geom)
                            except Exception as e:
                                # If there's an error converting the geometry, set it to None
                                feature["properties"]["geometry"] = None
                                error_msg = (
                                    f"Error converting geometry for feature {i}: {e}"
                                )
                                logger.warning(error_msg)
                                errors.append(error_msg)

                            # Append each feature to the list
                            features.append(feature["properties"])

                            # When the list hits the limit size - e.g. 75,000
                            # Process list into DataFrame
                            if len(features) == chunk_size:
                                # Process the chunk
                                df_chunk = pd.DataFrame(features)
                                insert_into_motherduck(df_chunk, conn, schema, table)
                                logger.info(
                                    f"Processed features {i-chunk_size+1} to {i}"
                                )

                                # Empty the list
                                features = []

                        # Process any remaining features outside the loop
                        if features:
                            df_chunk = pd.DataFrame(features)
                            insert_into_motherduck(df_chunk, conn, schema, table)
                            logger.info(f"Processed remaining features: {len(features)}")
                            # Empty the list
                            features = []

                except Exception as e:
                    error_msg = f"Error processing GeoPackage: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    raise
            else:
                error_msg = "No GeoPackage file found in the zip archive"
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
    return None

def process_data(url: str, conn, batch_size: int, schema: str, table: str):
    """
    Process the data from the url and insert it into the motherduck table.
    """
    logger.info(f"Starting data stream processing from {url} with batch size {batch_size}")
    redirect_url = fetch_redirect_url(url)
    load_geopackage_open_usrns(redirect_url, conn, batch_size, schema, table)
