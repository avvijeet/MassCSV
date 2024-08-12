# External Imports
from abc import ABC, abstractmethod
from queue import Queue

import pika  # RabbitMQ
from config import Config


class QueueAdapter(ABC):
    def __init__(self, config: Config):
        self.config = config
        self.queue_type = config.QUEUE_TYPE

    @abstractmethod
    def create_queue(self, queue_name: str):
        pass

    @abstractmethod
    def publish(self, message, queue_name: str):
        pass

    @abstractmethod
    def consume(self, queue_name: str):
        pass

    @abstractmethod
    def close(self):
        pass


class InMemQueueAdapter(QueueAdapter):
    def __init__(self, config: Config):
        super().__init__(config=config)
        self.queues = {}  # Dictionary to hold multiple in-memory queues

    def create_queue(self, queue_name: str):
        if queue_name not in self.queues:
            self.queues[queue_name] = Queue()

    def publish(self, message, queue_name: str):
        if queue_name not in self.queues:
            self.create_queue(queue_name)
        self.queues[queue_name].put(message)

    def consume(self, queue_name: str):
        if queue_name in self.queues:
            return self.queues[queue_name].get()
        return None

    def close(self):
        pass


class RabbitMQAdapter(QueueAdapter):
    def __init__(self, config: Config):
        super().__init__(config=config)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost"))
        self.channels = {}

    def create_queue(self, queue_name: str):
        if queue_name not in self.channels:
            channel = self.connection.channel()
            channel.queue_declare(queue=queue_name)  # Ensures the queue exists
            self.channels[queue_name] = channel

    def publish(self, message, queue_name: str):
        if queue_name not in self.channels:
            self.create_queue(queue_name)
        self.channels[queue_name].basic_publish(
            exchange="", routing_key=queue_name, body=message)

    def consume(self, queue_name: str):
        if queue_name not in self.channels:
            self.create_queue(queue_name)
        for method_frame, properties, body in self.channels[queue_name].consume(queue_name):
            self.channels[queue_name].basic_ack(method_frame.delivery_tag)
            return body.decode()

    def close(self):
        for channel in self.channels.values():
            channel.close()
        self.connection.close()


if __name__ == "__main__":
    pass
