import boto3
import os
from .s3_base import S3BaseStorage
from typing import Optional


class R2Storage(S3BaseStorage):
    """Cloudflare R2 storage implementation."""

    def __init__(self, access_key: Optional[str] = None,
                 secret_access_key: Optional[str] = None,
                 account_id: Optional[str] = None,
                 region: Optional[str] = None):

        # Try to get credentials from parameters first, then environment variables
        self.access_key = access_key or os.environ.get('R2_ACCESS_KEY_ID')
        self.secret_key = secret_access_key or os.environ.get('R2_SECRET_ACCESS_KEY')
        self.account_id = account_id or os.environ.get('R2_ACCOUNT_ID')

        # Check if we have all required credentials
        missing_params = []
        if not self.access_key:
            missing_params.append('access_key')
        if not self.secret_key:
            missing_params.append('secret_access_key')
        if not self.account_id:
            missing_params.append('account_id')

        if missing_params:
            raise ValueError(
                f"Hey! You forgot to provide these parameters: {', '.join(missing_params)}! "
                "Either pass them directly or set these environment variables: "
                "R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID"
            )

        client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=f'https://{self.account_id}.r2.cloudflarestorage.com',
            region_name='auto' if region is None else region
        )
        super().__init__(client)

    def _get_storage_url(self, bucket_name: str, fpath_in_bucket: str) -> str:
        """
        Generate an R2 URL for the given bucket and file path.

        Args:
           bucket_name: Name of the bucket containing the file.
           fpath_in_bucket: Path/key of the file.

        Returns:
           R2 URL in format 'r2://bucket-name/file-path'.
        """
        return f'r2://{bucket_name}/{fpath_in_bucket}'
