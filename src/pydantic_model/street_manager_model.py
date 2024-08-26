from pydantic import BaseModel, ValidationError, Field
from typing import Optional
import pandas as pd
from loguru import logger


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
    df: pd.DataFrame, model: StreetManagerPermitModel, sample_size: int = 500
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
