import json
import boto3
import boto3.session

from loguru import logger


def get_secrets(secret_name, region_name="eu-west-2") -> dict:
    """
    Create an AWS Secrets Manager client.

    Returns a JSON with environment variables.

    Args:
        Secret name (value used to retreive secret from aws secrets manager)
        Region name
    """

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        logger.success("Secrets Retrieved")
    except Exception as e:
        logger.error(f"Unable to retrieve secret {secret_name}: {str(e)}")
        raise e
    else:
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
