from src.pydantic_model.swa_codes_model import validate_data_model
from loguru import logger


def test_validate_data_model():
    # Fetch data
    result = validate_data_model()
    assert result is not None, "Returned None - Error Fecthing Data"
    validated_data, errors, df = result

    # Check that we have the correct number of validated records
    assert len(validated_data) + len(errors) == len(df)
    # Check that there are no errors
    assert len(errors) == 0, f"Validation errors occurred: {errors}"
    logger.success("No Schema Errors Found")
    logger.success(f"Sample Df: {df}")
