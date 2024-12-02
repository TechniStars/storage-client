import boto3
import os
from .s3_base import S3BaseStorage
from typing import Optional


class S3Storage(S3BaseStorage):
    """AWS S3 storage implementation."""

    def __init__(self, access_key: Optional[str] = None,
                 secret_access_key: Optional[str] = None,
                 region: Optional[str] = None):

        # Try to get credentials from parameters first, then environment variables
        self.access_key = access_key or os.environ.get('S3_ACCESS_KEY_ID')
        self.secret_key = secret_access_key or os.environ.get('S3_SECRET_ACCESS_KEY')

        # Check if we have all required credentials
        missing_params = []
        if not self.access_key:
            missing_params.append('access_key')
        if not self.secret_key:
            missing_params.append('secret_access_key')

        if missing_params:
            raise ValueError(
                f"Hey! You forgot to provide these parameters: {', '.join(missing_params)}! "
                "Either pass them directly or set these environment variables: "
                "S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY"
            )

        client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name='auto' if region is None else region
        )
        super().__init__(client)

    def _get_storage_url(self, bucket_name: str, fpath_in_bucket: str) -> str:
        """
        Generate an S3 URL for the given bucket and file path.

        Args:
           bucket_name: Name of the bucket containing the file.
           fpath_in_bucket: Path/key of the file.

        Returns:
           S3 URL in format 's3://bucket-name/file-path'.
        """
        return f's3://{bucket_name}/{fpath_in_bucket}'
