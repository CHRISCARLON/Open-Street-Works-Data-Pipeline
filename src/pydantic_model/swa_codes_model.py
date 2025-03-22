import pandas as pd

from src.geoplace_swa_codes.fetch_swa_codes import fetch_swa_codes

from loguru import logger
from pydantic import BaseModel, Field, ConfigDict, ValidationError
from typing import List, Tuple, Optional


class SWACodeModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    swa_code: Optional[str] = Field(None, alias="SWA Code")
    account_name: Optional[str] = Field(None, alias="Account Name")
    prefix: Optional[str] = Field(None, alias="Prefix")
    account_type: Optional[str] = Field(None, alias="Account Type")
    registered_for_street_manager: Optional[str] = Field(
        None, alias="Registered for Street Manager"
    )
    account_status: Optional[str] = Field(None, alias="Account Status")
    companies_house_number: Optional[str] = Field(None, alias="Companies House Number")
    previous_company_names: Optional[str] = Field(None, alias="Previous Company Names")
    linked_parent_company: Optional[str] = Field(None, alias="Linked/Parent Company")
    website: Optional[str] = Field(None, alias="Website")
    plant_enquiries: Optional[str] = Field(None, alias="Plant Enquiries")
    ofgem_electricity_licence: Optional[str] = Field(
        None, alias="Ofgem Electricity Licence"
    )
    ofgem_gas_licence: Optional[str] = Field(None, alias="Ofgem Gas Licence")
    ofcom_licence: Optional[str] = Field(None, alias="Ofcom Licence")
    ofwat_licence: Optional[str] = Field(None, alias="Ofwat Licence")
    company_subsumed_by: Optional[str] = Field(None, alias="Company Subsumed By")
    swa_code_of_new_company: Optional[str] = Field(
        None, alias="SWA Code of New Company"
    )


def validate_data_model() -> (
    Optional[Tuple[List[SWACodeModel], List[str], pd.DataFrame]]
):
    """
    Fetches SWA codes data, validates it against the SWA Code Pydantic model,
    and returns a list of validated SWACode objects.

    Returns:
        A list of validated SWACodeModel objects.
    """

    try:
        # Fetch the SWA codes data
        df = fetch_swa_codes()
        # Explicit type assertion
        assert isinstance(df, pd.DataFrame), "DataFrame must be present"
    except (ValueError, AssertionError) as e:
        logger.error(f"Error getting download link: {e}")
        return None

    # Convert DataFrame to a list of dictionaries
    data_dicts = df.to_dict(orient="records")

    # Set variables up for validation process
    validated_data = []
    errors = []

    # Iterate through and validate
    for idx, item in enumerate(data_dicts):
        try:
            # Validate the item
            validated_item = SWACodeModel.model_validate(item)
            validated_data.append(validated_item)
        except ValidationError as e:
            errors.append(f"Error in record {idx}: {str(e)}")
    logger.success(
        f"Successfully validated {len(validated_data)} out of {len(data_dicts)} records."
    )
    logger.info(f"There were {len(errors)} errors")
    return validated_data, errors, df
