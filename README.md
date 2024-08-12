# CSV Pipeline Project

## Overview

The CSV Pipeline Project is designed to handle the extraction, transformation, and loading (ETL) of CSV data. It leverages a combination of thread and process pools to efficiently manage I/O-bound and CPU-intensive tasks. The project is highly configurable, supporting various storage backends, queue systems, and databases.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
- [Usage](#usage)
- [Components](#components)
  - [Main](#main)
  - [Config](#config)
  - [DataLoader](#dataloader)
  - [DBAdapter](#dbadapter)
  - [Adapters](#adapters)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [License](#license)
- [Contributing](#contributing)

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/avvijeet/csv-pipeline.git && cd csv-pipeline
   ```

2. **Create a virtual environment and activate it:**

    ```bash
    python3.11 -m venv venv && source venv/bin/activate
    ```

3. Install the required dependencies:

    ```bash
    source venv/bin/activate && pip install -r requirements.txt
    ```

## Configuration

Configuration is managed through environment variables. You can set these variables in your environment or create a .env file in the root directory of the project.

## Environment Variables

- STORAGE_TYPE: Type of storage backend (default: filesystem)
- STORAGE_BASE_PATH: Base path for filesystem storage (default: ./)
- QUEUE_TYPE: Type of queue system (default: in_memory)
- DB_TYPE: Type of database (default: sqlite)
- S3_BUCKET: S3 bucket name (default: your-bucket-name)
- LARGE_FILE_S3_KEY: S3 key for the large file (default: large.csv)
- LOCAL_STORAGE_PATH: Path to local storage (default: /path/to/local/storage)
- DB_URI: Database URI (default: sqlite:///sales_data.db)

## Usage

Ensure all necessary environment variables are set.

## Run the main script

```bash
python csv_pipeline/main.py
```

## Components

### Main

The main.py script initializes the configuration, adapters, and executors. It sets up the necessary queues and submits tasks to the appropriate executors. It also handles graceful shutdown on receiving termination signals.

### Config

The config.py file defines the Config class, which reads configuration values from environment variables.

### DataLoader

The DataLoader class in loader.py is responsible for reading data from the storage backend, processing it, and inserting it into the database. It also handles error logging and deletion of processed files.

### DBAdapter

The DBAdapter class in adapters/databases.py is an abstract base class for database operations. It defines methods for executing queries, creating tables, and inserting data.

### Adapters

Adapters are used to abstract the interaction with different storage backends, queue systems, and databases. The following adapters are available:

- Queue Adapters: InMemQueueAdapter, RabbitMQAdapter
- Blob Adapters: FileSystemBlobAdapter, S3BlobAdapter
- DB Adapters: SQLiteAdapter, PostgreSQLAdapter

### Example Usage

To get a queue adapter:

```python
from config import Config
from main import get_queue_adapter

config = Config()
queue_adapter = get_queue_adapter(config)
```

### Logging

The project uses the loguru library for logging. Logs are categorized into different levels such as info, success, and error.

Example Logging

```python
from loguru import logger

logger.info("This is an info message")
logger.success("This is a success message")
logger.error("This is an error message")
```

### Error Handling

Errors encountered during data processing are logged and saved to an error log file. The DataLoader class maintains a list of error rows and writes them to a CSV file if any errors occur.

Example Error Handling

```python
def process_order(self, row):
    try:
        # Process the order
    except Exception as e:
        self.log_error(row.to_dict(), str(e))
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please read the CONTRIBUTING file for guidelines on how to contribute to this project.

For any questions or issues, please open an issue on the GitHub repository.
