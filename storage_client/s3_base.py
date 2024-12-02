import os.path
from typing import List, Optional, Dict

from botocore.exceptions import ClientError

from storage_interface import StorageInterface


class S3BaseStorage(StorageInterface):
    """Base class for S3-compatible storage implementations."""

    def __init__(self, client):
        """
        Initialize the storage interface with an S3 client.

        Args:
            client: A configured boto3 S3 client instance.
        """
        self.client = client

    def upload_file(self, local_fpath: str, bucket_name: str, fpath_in_bucket: Optional[str] = None) -> tuple[str, str]:
        """
        Upload a local file to S3-based storage.

        Args:
            local_fpath: Path to the local file to upload.
            bucket_name: Name of the destination bucket.
            fpath_in_bucket: Optional; Path/key for the file in the bucket.
                           If not provided, uses the local file's basename.

        Returns:
            tuple: (file path in bucket, storage URL)

        Raises:
            Exception: If the upload fails due to client errors.
        """
        if fpath_in_bucket is None:
            fpath_in_bucket = os.path.basename(local_fpath)

        try:
            self.client.upload_file(local_fpath, bucket_name, fpath_in_bucket)
            return fpath_in_bucket, self._get_storage_url(bucket_name, fpath_in_bucket)
        except ClientError as e:
            raise Exception(f"Upload failed: {str(e)}")

    def download_file(self, bucket_name: str, fpath_in_bucket: str, local_fpath: Optional[str] = None) -> str:
        """
        Download a file from S3-based storage to local filesystem.

        Creates necessary local directories if they don't exist.

        Args:
            bucket_name: Source bucket name.
            fpath_in_bucket: Path/key of the file in the bucket.
            local_fpath: Optional; Local destination path.
                        If not provided, uses the bucket file's basename.

        Returns:
            str: Path where the file was downloaded locally.

        Raises:
            Exception: If the download fails due to client errors.
        """
        if local_fpath is None:
            local_fpath = os.path.basename(fpath_in_bucket)

        directory = os.path.dirname(local_fpath)
        if directory:
            os.makedirs(directory, exist_ok=True)

        try:
            self.client.download_file(bucket_name, fpath_in_bucket, local_fpath)
            return local_fpath
        except ClientError as e:
            raise Exception(f"Download failed: {str(e)}")

    def list_files(self, bucket_name: str, prefix: Optional[str] = None) -> List[Dict]:
        """
        List files in an S3-based bucket, optionally filtered by prefix.

        Args:
            bucket_name: Name of the bucket to list.
            prefix: Optional; Filter results to files starting with this prefix.

        Returns:
            List[Dict]: List of dictionaries containing file information:
                       - key: File path/key in bucket
                       - size: File size in bytes
                       - last_modified: Timestamp of last modification

        Raises:
            Exception: If the listing operation fails.
        """
        try:
            if prefix:
                response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            else:
                response = self.client.list_objects_v2(Bucket=bucket_name)

            return [{'key': obj['Key'],
                     'size': obj['Size'],
                     'last_modified': obj['LastModified']}
                    for obj in response.get('Contents', [])]
        except ClientError as e:
            raise Exception(f"Listing failed: {str(e)}")

    def delete_file(self, bucket_name: str, fpath_in_bucket: str) -> bool:
        """
        Delete a file from S3-based storage.

        Args:
            bucket_name: Name of the bucket containing the file.
            fpath_in_bucket: Path/key of the file to delete.

        Returns:
            bool: True if deletion was successful.

        Raises:
            Exception: If the deletion fails due to client errors.
        """
        try:
            self.client.delete_object(Bucket=bucket_name, Key=fpath_in_bucket)
            return True
        except ClientError as e:
            raise Exception(f"Deletion failed: {str(e)}")

    def file_exists(self, bucket_name: str, fpath_in_bucket: str) -> bool:
        """
        Check if a file exists in S3-based storage.

        Args:
            bucket_name: Name of the bucket to check.
            fpath_in_bucket: Path/key of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            self.client.head_object(Bucket=bucket_name, Key=fpath_in_bucket)
            return True
        except ClientError:
            return False

    def get_file_metadata(self, bucket_name: str, fpath_in_bucket: str) -> Dict:
        """
        Retrieve metadata for a file in S3-based storage.

        Args:
            bucket_name: Name of the bucket containing the file.
            fpath_in_bucket: Path/key of the file.

        Returns:
            File metadata containing file size, MIME type, last modification timestamp,
            and custom metadata dictionary.

        Raises:
            Exception: If metadata retrieval fails.
        """
        try:
            response = self.client.head_object(Bucket=bucket_name, Key=fpath_in_bucket)
            return {
                'content_length': response['ContentLength'],
                'content_type': response.get('ContentType'),
                'last_modified': response['LastModified'],
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            raise Exception(f"Failed to get metadata: {str(e)}")

    def _get_storage_url(self, bucket_name: str, fpath_in_bucket: str) -> str:
        """
        Get storage URL for the file. Should be overridden by subclasses.

        Args:
            bucket_name: Name of the bucket containing the file.
            fpath_in_bucket: Path/key of the file.

        Returns:
            str: URL to access the file.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError
