from pydantic import BaseModel, ValidationError, Field
from typing import Optional
import pandas as pd
from loguru import logger
import json
from stream_unzip import stream_unzip
from tqdm import tqdm

# Core elements of the Street Manager permit data
class StreetManagerPermitModel(BaseModel):
    event_reference: Optional[int]
    event_type: Optional[str]
    work_reference_number: Optional[str]
    permit_reference_number: Optional[str]
    promoter_swa_code: str
    promoter_organisation: str
    highway_authority: str
    works_location_coordinates: Optional[str]
    street_name: Optional[str]
    area_name: Optional[str]
    work_category: str
    traffic_management_type: Optional[str]
    proposed_start_date: Optional[str]
    proposed_start_time: Optional[str]
    proposed_end_date: Optional[str]
    proposed_end_time: Optional[str]
    actual_start_date_time: Optional[str]
    actual_end_date_time: Optional[str]
    work_status: str
    usrn: Optional[str]
    highway_authority_swa_code: Optional[str]
    work_category_ref: str
    traffic_management_type_ref: Optional[str]
    work_status_ref: str
    activity_type: str
    is_ttro_required: Optional[str]
    is_covid_19_response: Optional[str]
    works_location_type: Optional[str]
    permit_conditions: str
    road_category: Optional[str]
    is_traffic_sensitive: Optional[str]
    is_deemed: Optional[str]
    permit_status: str
    town: Optional[str]
    collaborative_working: Optional[str]
    close_footway: Optional[str]
    close_footway_ref: Optional[str]
    current_traffic_management_type: Optional[str]
    current_traffic_management_type_ref: Optional[str]
    current_traffic_management_update_date: Optional[str]
    event_time: Optional[str]
    object_type: Optional[str]
    object_reference: Optional[str]
    version: Optional[int]
    collaboration_type: Optional[str] = Field(default=None)
    collaboration_type_ref: Optional[str] = Field(default=None)

# Validate a small sample against the model
def validate_dataframe_sample(
    df: pd.DataFrame, model: type[StreetManagerPermitModel], sample_size: int = 500
) -> list:
    errors = []
    # Ensure sample size is not larger than the DataFrame
    sample_size = min(sample_size, len(df))
    # Sample rows
    sample_df = df.sample(n=sample_size)
    # Need to fill nan values - otherwise the model fails
    sample_df = sample_df.fillna("None")
    for index, row in sample_df.iterrows():
        try:
            model.model_validate(row.to_dict())
        except ValidationError as e:
            # Record validation errors with the index from the original DataFrame
            errors.append({"index": index, "errors": e.errors()})
    return errors

# Raise a ValueError if the list returned contains something - if empty continue as normal as no error
def handle_validation_errors(validation_errors: list):
    if validation_errors:
        # Log each validation error
        for error in validation_errors:
            logger.error(
                f"Validation error at index {error['index']}: {error['errors']}"
            )
        # Raise an exception to indicate validation failure
        raise ValueError("Validation errors detected in sample of data.")
    else:
        logger.success("The Pydantic model has been passed! Allez!")

# Functions to test a sample of 500 files to ensure schema is valid
def flatten_json(json_data) -> dict:
    """
    Street manager archived open data comes in nested json files
    This function flattens the structure
    
    Args:
        json_data to flatten
        
    Returns:
        flattened data
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
    Reads 500 JSON files from zipped chunks and returns a Pandas DataFrame.

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