import functools
import io
import typing

import boto3
import botocore.client
import botocore.exceptions

from kvstore.exceptions import S3FileNotFoundError

# We have removed all the sensitive information from our code, 
# so everyone who is interested can simlpy use their own credentials info

ACCESS_KEY_ID = ''
ACCESS_SECRET_KEY = ''
BUCKET_NAME = ''


@functools.lru_cache()
def _get_resource():
    """Initialize the resource for the bucket.

    Note:
        This is an internal function.
    """
    return boto3.resource(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        config=botocore.client.Config(signature_version='s3v4')
    )


@functools.lru_cache()
def _get_bucket():
    """Initialize the bucket.

    Note:
        This is an internal function.
    """
    return _get_resource().Bucket(BUCKET_NAME)

S3_BUCKET = _get_bucket()


def upload_file(filename: str, data: bytes, bucket=S3_BUCKET, save: bool = True):
    """Upload a file to the a bucket.

    Args:
        filename (str): The filename to upload under.
        data (bytes): The content of the file.
        bucket: The bucket to upload to. This is a default bucket.
    """
    return bucket.upload_fileobj(io.BytesIO(data), filename)


def delete_file(filename: str, bucket=S3_BUCKET, saved: bool = True) -> bool:
    """Delete a file from a bucket.

    Args:
        filename (str): The filename to delete.
        bucket: The bucket to use. This is a default bucket.

    Returns:
        bool: True if deletion was succesful.
    """
    response = bucket.delete_objects(Delete={'Objects': [{'Key': filename}]})
    response = response['Deleted']
    if len(response) == 0 or response[0]['Key'] != filename:
        raise Exception('Could not delete file.')
    return True


def download_file(filename: str, bucket=S3_BUCKET) -> bytes:
    """Download a certain file from a bucket.

    Args:
        filename (str): The filename to download.
        bucket: The bucket to download from. This is a default bucket.

    Returns:
        bytes: The contents of the file.
    """
    with io.BytesIO() as f:
        try:
            bucket.download_fileobj(filename, f)
        except botocore.exceptions.ClientError:
            raise S3FileNotFoundError('File {} not found.'.format(filename))
        f.seek(0)
        return f.read()


def list_files(bucket=S3_BUCKET) -> typing.List[str]:
    """List the current files on S3.

    Args:
        bucket: The bucket to get the list from. This is a default bucket.

    Returns:
        list of str: The list of files in the bucket.
    """
    return [o.key for o in bucket.objects.all()]

