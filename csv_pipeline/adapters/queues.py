from abc import ABC, abstractmethod
from queue import Queue

import pika  # RabbitMQ
from kafka import KafkaConsumer, KafkaProducer

from csv_pipeline.config import Config


class QueueAdapter(ABC):
    @abstractmethod
    def __init__(self, config: Config):
        self.config = config
        self.queue_type = config.QUEUE_TYPE

    @abstractmethod
    def publish(self, message):
        pass

    @abstractmethod
    def consume(self):
        pass

    @abstractmethod
    def close(self):
        pass


class InMemQueueAdapter(ABC):
    def __init__(self, config: Config):
        super().__init__(config=config)
        self.queue = Queue()

    def publish(self, message):
        self.queue.put(message)

    def consume(self):
        return self.queue.get()

    def close(self):
        pass


class KafkaAdapter(ABC):
    def __init__(self, config: Config):
        super().__init__(config=config)
        self.producer = KafkaProducer(bootstrap_servers="localhost:9092")
        self.consumer = KafkaConsumer(
            "data_topic", bootstrap_servers="localhost:9092")

    def publish(self, message):

        self.producer.send("data_topic", value=message.encode("utf-8"))

    def consume(self):

        for message in self.consumer:
            return message.value.decode()

    def close(self):
        self.producer.close()
        self.consumer.close()


class RabbitMQAdapter(ABC):

    def __init__(self, config: Config):
        super().__init__(config=config)

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="data_queue")

    def publish(self, message):

        self.channel.basic_publish(
            exchange="", routing_key="data_queue", body=message)

    def consume(self):
        for method_frame, properties, body in self.channel.consume("data_queue"):
            return body.decode()

    def close(self):
        self.connection.close()
