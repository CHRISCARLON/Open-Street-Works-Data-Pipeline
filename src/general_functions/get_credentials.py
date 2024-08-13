import json
import boto3
import boto3.session

from loguru import logger
# from creds import secret_name


def get_secrets(secret_name, region_name="eu-west-2") -> dict:
    """
    Create an AWS Secrets Manager client.

    Returns a JSON with environment variables.

    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.error(f"Unable to retrieve secret {secret_name}: {str(e)}")
        raise e
    else:
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)


# if __name__ == "__main__":
#     v = get_secrets(secret_name)
#     print(v)
