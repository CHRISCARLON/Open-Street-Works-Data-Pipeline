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
