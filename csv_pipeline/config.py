# External Imports
import os


class Config:
    # Default to filesystem
    STORAGE_TYPE = os.getenv("STORAGE_TYPE", "filesystem")
    STORAGE_BASE_PATH = os.getenv("STORAGE_BASE_PATH", "./")

    # Default to in-memory queue
    QUEUE_TYPE = os.getenv("QUEUE_TYPE", "in_memory")
    DB_TYPE = os.getenv("DB_TYPE", "sqlite")  # Default to SQLite
    S3_BUCKET = os.getenv("S3_BUCKET", "your-bucket-name")
    LARGE_FILE_S3_KEY = os.getenv("LARGE_FILE_S3_KEY", "large.csv")
    LOCAL_STORAGE_PATH = os.getenv(
        "LOCAL_STORAGE_PATH", "/path/to/local/storage")
    # Change this for PostgreSQL
    DB_URI = os.getenv("DB_URI", "sqlite:///sales_data.db")


if __name__ == "__main__":
    pass
