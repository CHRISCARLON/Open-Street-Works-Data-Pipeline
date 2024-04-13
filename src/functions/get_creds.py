import json
import boto3
from loguru import logger


def get_secrets(secret_name, region_name="eu-west-2") -> json:
    """
    Create a Secrets Manager client. 
    Returns a JSON with environment variables. 

    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',
                            region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.error(f"Unable to retrieve secret {secret_name}: {str(e)}")
        raise e
    else:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
