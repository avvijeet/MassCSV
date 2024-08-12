from enum import StrEnum, auto

from adapters import BlobAdapter, DBAdapter, QueueAdapter
from config import Config
from loader import DataLoader


class QueueTypes(StrEnum):
    RABBITMQ = auto()
    KAFKA = auto()
    SQS = auto()
    CELERY = auto()
    REDIS = auto()
    ZOOKEEPER = auto()
    IN_MEMORY = auto()


def main():
    config = Config()

    queue = QueueAdapter(config)
    storage_adapter = BlobAdapter(config)
    db_adapter = DBAdapter(config)
    db_adapter.create_tables()
    loader = DataLoader(storage_adapter, db_adapter)

    # Listen for new data
    while True:
        file_name = queue.consume()  # Block until a new message arrives
        loader.process_file(file_name)


if __name__ == "__main__":
    main()
