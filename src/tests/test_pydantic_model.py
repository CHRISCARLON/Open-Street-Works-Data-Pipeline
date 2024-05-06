from ..permit_functions.extract_load_data import fetch_data, check_data_schema, quick_col_rename
from ..pydantic_model.street_manager_model import (
    StreetManagerPermitModel,
    validate_dataframe_sample,
    handle_validation_errors
)

def check_permit_model():
    """
    
    
    """
    
    link = "https://opendata.manage-roadworks.service.gov.uk/permit/2024/01.zip"
    data = fetch_data(link)
    df = check_data_schema(data)
    df = quick_col_rename(df)
    validate = validate_dataframe_sample(df, StreetManagerPermitModel)
    handle_validation_errors(validate)
    print(df.dtypes)
    print(df.head(5))
    return None


if __name__=="__main__":
    check_permit_model()