from abc import ABC, abstractmethod
from typing import List, Optional, Dict


class StorageInterface(ABC):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @abstractmethod
    def upload_file(self, fpath: str, bucket_name: str, fpath_in_bucket: Optional[str] = None) -> tuple[str, str]:
        pass

    @abstractmethod
    def download_file(self, bucket_name: str, fpath_in_bucket: str, local_fpath: Optional[str] = None) -> str:
        pass

    @abstractmethod
    def list_files(self, bucket_name: str, prefix: Optional[str] = None) -> List[Dict]:
        pass

    @abstractmethod
    def delete_file(self, bucket_name: str, fpath_in_bucket: str) -> bool:
        pass

    @abstractmethod
    def file_exists(self, bucket_name: str, fpath_in_bucket: str) -> bool:
        pass

    @abstractmethod
    def get_file_metadata(self, bucket_name: str, fpath_in_bucket: str) -> Dict:
        pass
