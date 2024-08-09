from src.geoplace_swa_codes.fetch_swa_codes import validate_data_model

def test_validate_data_model():
    validated_data, errors, df = validate_data_model()
    
    # Check that we have the correct number of validated records
    assert len(validated_data) + len(errors) == len(df)
    
    # Check that there are no errors
    assert len(errors) == 0, f"Validation errors occurred: {errors}"
