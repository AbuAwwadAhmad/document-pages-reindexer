import boto3
import json


def get_secret(secret_name, secret_key):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret_value = json.loads(response['SecretString'])
    return secret_value[str(secret_key)]
