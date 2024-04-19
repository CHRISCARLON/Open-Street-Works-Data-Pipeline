from pydantic import BaseModel, ValidationError
from typing import Optional
import pandas as pd
from loguru import logger

class StreetManagerPermitModel(BaseModel):
    event_reference: Optional[int]
    event_type: Optional[str]
    work_reference_number: Optional[str]
    permit_reference_number: Optional[str]
    promoter_swa_code: Optional[str]
    promoter_organisation: Optional[str]
    highway_authority: Optional[str]
    highway_authority_swa_code: Optional[str]
    event_time: Optional[str]
    object_type: Optional[str]
    object_reference: Optional[str]
    version: Optional[int]

# Validate a small sample against the model
def validate_dataframe_sample(df: pd.DataFrame, model: BaseModel, sample_size: int = 50):
    errors = []
    # Ensure sample size is not larger than the DataFrame
    sample_size = min(sample_size, len(df))
    # Sample rows
    sample_df = df.sample(n=sample_size)
    for index, row in sample_df.iterrows():
        try:
            model(**row.to_dict())
        except ValidationError as e:
            # Record validation errors with the index from the original DataFrame
            errors.append({'index': index, 'errors': e.errors()})
    return errors

# Raise a ValueError if the list returned contains something - if empty continue as normal as no error
def handle_validation_errors(validation_errors):
    if validation_errors:
        # Log each validation error
        for error in validation_errors:
            logger.error(f"Validation error at index {error['index']}: {error['errors']}")
        # Raise an exception to indicate validation failure
        raise ValueError("Validation errors detected in sample of data.")
    else:
        logger.success("The Pydantic model has been passed! Allez!")
