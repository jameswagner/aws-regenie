"""
S3 utility functions shared across Lambda functions
"""
import re
import boto3
from botocore.exceptions import ClientError

# Import shared constants
from .constants import S3Constants


def ensure_trailing_slash(path: str) -> str:
    """Ensure the S3 path ends with a trailing slash"""
    if not path.endswith(S3Constants.PATH_SEPARATOR):
        return f"{path}{S3Constants.PATH_SEPARATOR}"
    return path


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """
    Parse an S3 URI into bucket and key components
    
    Args:
        s3_uri: S3 URI in format s3://bucket/key
        
    Returns:
        Tuple of (bucket, key) or (None, None) if invalid
    """
    match = re.match(S3Constants.S3_URI_PATTERN, s3_uri)
    if match:
        bucket = match.group(1)
        key = match.group(2)
        return bucket, key
    return None, None


def check_file_exists(s3_client, bucket: str, key: str) -> bool:
    """
    Check if a file exists in S3
    
    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        True if file exists, False otherwise
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response[S3Constants.ERROR_KEY][S3Constants.CODE_KEY] == S3Constants.NOT_FOUND_CODE:
            return False
        else:
            raise 