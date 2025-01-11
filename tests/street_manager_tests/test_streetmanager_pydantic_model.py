import pytest
import pandas as pd

from src.england_street_manager.extract_load_data import quick_col_rename
from src.england_street_manager.stream_zipped_data import fetch_data
from src.pydantic_model.street_manager_model import (
    StreetManagerPermitModel,
    validate_dataframe_sample,
    handle_validation_errors,
    check_data_schema
)

@pytest.fixture
def sample_data():
    link = "https://opendata.manage-roadworks.service.gov.uk/permit/2024/12.zip"
    data = fetch_data(link)
    return data

def test_check_data_schema(sample_data):
    df = check_data_schema(sample_data)
    print(df.head(10))
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 500

def test_quick_col_rename(sample_data):
    df = check_data_schema(sample_data)
    renamed_df = quick_col_rename(df)
    assert "object_data." not in renamed_df.columns

def test_validate_dataframe_sample(sample_data):
    df = check_data_schema(sample_data)
    df = quick_col_rename(df)
    validate = validate_dataframe_sample(df, StreetManagerPermitModel)
    assert not validate

def test_handle_validation_errors(sample_data):
    df = check_data_schema(sample_data)
    df = quick_col_rename(df)
    validate = validate_dataframe_sample(df, StreetManagerPermitModel)
    errors = handle_validation_errors(validate)
    assert not errors
