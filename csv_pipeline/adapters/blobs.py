import os
from abc import ABC, abstractmethod

import boto3

from csv_pipeline.config import Config


class BlobAdapter(ABC):

    @abstractmethod
    def __init__(self, config: Config):
        self.config = config
        self.storage_type = config.STORAGE_TYPE

    @abstractmethod
    def read_data(self, file_name_with_path: str) -> str:
        pass

    @abstractmethod
    def delete_data(self, file_name_with_path: str) -> None:
        pass

    @abstractmethod
    def save_data(self, file_name: str, data: bytes) -> None:
        pass


class S3BlobAdapter(BlobAdapter):
    def __init__(self, config: Config):
        super().__init__(config=config)
        self.s3 = boto3.client("s3")

    def read_data(self, file_name_with_path: str):
        response = self.s3.get_object(
            Bucket=self.config.S3_BUCKET, Key=file_name_with_path)
        return response["Body"].read().decode("utf-8")

    def delete_data(self, file_name_with_path) -> None:
        self.s3.delete_object(Bucket=self.config.S3_BUCKET,
                              Key=file_name_with_path)

    def save_data(self, file_name: str, data: bytes) -> None:
        # Save data to S3
        self.s3.put_object(Bucket=self.config.S3_BUCKET,
                           Key=file_name, Body=data)


class FileSystemBlobAdapter(BlobAdapter):
    def __init__(self, config: Config):
        super().__init__(config=config)

    def read_data(self, file_name_with_path: str):
        with open(os.path.join(self.config.LOCAL_STORAGE_PATH, file_name_with_path), "r") as file:
            return file.read()

    def delete_data(self, file_name_with_path: str):
        os.remove(os.path.join(
            self.config.LOCAL_STORAGE_PATH, file_name_with_path))

    def save_data(self, file_name: str, data: bytes) -> None:
        # Save data to a local file
        file_path = os.path.join(self.config.STORAGE_BASE_PATH, file_name)
        with open(file_path, "wb") as f:
            f.write(data)
