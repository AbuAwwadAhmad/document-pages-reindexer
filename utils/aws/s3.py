import boto3
from utils import file_utils, constantsTest as constants
# from utils import file_utils, constants
from botocore.exceptions import NoCredentialsError


def download_file_from_s3(s3_file_url, file_id, local_file_path):
    s3 = boto3.client('s3')
    index = s3_file_url.find(constants.S3_BUCKET + '/')

    if index != -1:
        s3_key = s3_file_url[index + len(constants.S3_BUCKET + '/'):]
    else:
        raise Exception(f"Invalid S3 file URL: {s3_file_url} is not valid for file id: {file_id}")

    try:
        s3.download_file(constants.S3_BUCKET, s3_key, local_file_path)
    except NoCredentialsError:
        raise Exception("AWS credentials not available. Make sure your credentials are configured.")
    return local_file_path
