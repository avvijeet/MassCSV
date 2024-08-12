# External Imports
import os
import signal
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from config import Config
from extractor import read_and_save_csv_in_chunks
from loader import DataLoader
from loguru import logger
from transformer import cleanse_and_validate

# Internal Imports
from adapters import (
    BlobAdapter,
    DBAdapter,
    FileSystemBlobAdapter,
    InMemQueueAdapter,
    PostgreSQLAdapter,
    QueueAdapter,
    RabbitMQAdapter,
    S3BlobAdapter,
    SQLiteAdapter,
)

QUEUE_ADAPTER_MAP = {"in_memory": InMemQueueAdapter,
                     "rabbitmq": RabbitMQAdapter}
BLOB_ADAPTER_MAP = {"filesystem": FileSystemBlobAdapter, "s3": S3BlobAdapter}
DB_ADAPTER_MAP = {"sqlite": SQLiteAdapter, "postgres": PostgreSQLAdapter}


def get_queue_adapter(config: Config) -> QueueAdapter:
    return QUEUE_ADAPTER_MAP[config.QUEUE_TYPE](config=config)


def get_blob_adapter(config: Config) -> BlobAdapter:
    return BLOB_ADAPTER_MAP[config.STORAGE_TYPE](config=config)


def get_db_adapter(config: Config) -> DBAdapter:
    return DB_ADAPTER_MAP[config.DB_TYPE](config=config)


def handle_extraction(queue_adapter: QueueAdapter, config: Config):
    logger.debug("Starting Async Extractor")
    for extracted_chunk_path in read_and_save_csv_in_chunks(
        bucket_name=config.S3_BUCKET, s3_key=config.LARGE_FILE_S3_KEY
    ):
        logger.debug("Publishing extracted_chunk_path to transform_queue")
        queue_adapter.publish(extracted_chunk_path, "transform_queue")


def handle_transformation(queue_adapter: QueueAdapter):
    logger.debug("Starting Transformation Consumer")
    while True:
        logger.debug("Waiting to consume from transform_queue")
        extracted_file_path = queue_adapter.consume("transform_queue")
        if extracted_file_path is None:
            logger.debug("No file path received, continue waiting...")
            continue

        logger.debug(
            f"Received file path: {extracted_file_path} from transform_queue")
        transformed_file_path = f"transformed_{extracted_file_path}"
        transformed_file_path = cleanse_and_validate(
            input_file_path=extracted_file_path, cleansed_processed_output_file_path=transformed_file_path
        )
        if transformed_file_path:
            logger.debug(f"Publishing {transformed_file_path} to loader_queue")
            queue_adapter.publish(transformed_file_path, "loader_queue")


def handle_loading(queue_adapter: QueueAdapter, loader: DataLoader):
    logger.debug("Starting Loader Consumer")
    while True:
        try:
            logger.debug("Waiting to consume from loader_queue")
            transformed_file_path = queue_adapter.consume("loader_queue")
            if transformed_file_path is None:
                logger.debug("No file path received, continue waiting...")
                continue

            logger.debug(
                f"Received file path: {transformed_file_path} from loader_queue")
            logger.debug("Loading transformed_file_path")
            loader.process_file(file_name_with_path=transformed_file_path)
            logger.success("Processed File")
        except Exception as e:
            logger.error(f"Error in handle_loading: {e}")


def main():
    config = Config()

    queue_adapter = get_queue_adapter(config=config)
    storage_adapter = get_blob_adapter(config=config)
    db_adapter = get_db_adapter(config=config)
    loader = DataLoader(storage_adapter=storage_adapter, db_adapter=db_adapter)

    db_adapter.create_tables()

    # Create necessary queues
    queue_adapter.create_queue("transform_queue")
    queue_adapter.create_queue("loader_queue")

    # Set up executors
    with (
        ThreadPoolExecutor(max_workers=3) as thread_executor,
        ProcessPoolExecutor(max_workers=os.cpu_count()) as process_executor,
    ):

        # Submit the tasks to the respective executors
        thread_executor.submit(
            handle_extraction, queue_adapter, config)  # I/O Bound
        process_executor.submit(handle_transformation,
                                queue_adapter)  # CPU Intensive
        thread_executor.submit(
            handle_loading, queue_adapter, loader)  # I/O Bound

        logger.success(
            "Started Extraction Task and consumer for Transformers and Loaders")

        # Handle shutdown gracefully
        def shutdown():
            logger.info("Shutting down...")
            process_executor.shutdown(wait=True)
            thread_executor.shutdown(wait=True)
            queue_adapter.close()

        signal.signal(signal.SIGINT, lambda s, f: shutdown())
        signal.signal(signal.SIGTERM, lambda s, f: shutdown())

        try:
            # Wait for all tasks to complete
            process_executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()
