import time
from kafka import KafkaConsumer, KafkaProducer
import json


class KafkaConsumerStream:
    def __init__(self, org_id: str):
        """Initialize Kafka consumer for consuming raw logs."""
        self.org_id = org_id
        self.topic = f"logs.raw.{org_id}"
        while True:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers="kafka:29092",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    group_id="normalizer-group"
                )
                print(f"✅ Connected to Kafka topic: {self.topic}")
                break
            except Exception as e:
                print(f"Error connecting to Kafka: {e}")
                time.sleep(5)  # Retry after delay

    def consume_messages(self):
        """Consume messages from Kafka topic."""
        for message in self.consumer:
            yield message.value

    def close(self):
        """Close the consumer connection."""
        self.consumer.close()


class KafkaProducerStream:
    def __init__(self, org_id: str):
        """Initialize Kafka producer for publishing normalized logs."""
        self.org_id = org_id
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers="kafka:29092",
                    value_serializer=lambda m: json.dumps(m).encode("utf-8")
                )
                print(f"✅ Connected to Kafka for producing to topic: logs.normalized.{self.org_id}")
                break
            except Exception as e:
                print(f"Error connecting to Kafka: {e}")
                time.sleep(5)  # Retry after delay

    def get_topic(self):
        return f"logs.normalized.{self.org_id}"
    
    def publish_message(self, message: dict):
        """Publish a message to Kafka topic."""
        topic = self.get_topic()
        future = self.producer.send(topic, message)
        return future.get(timeout=10)

    def flush(self):
        """Flush pending messages."""
        self.producer.flush()

    def close(self):
        """Close the producer connection."""
        self.producer.close()


class KafkaDLQProducer(KafkaProducerStream):
    def __init__(self, org_id: str):
        super().__init__(org_id)

    def get_topic(self):
        return f"logs.dlq.{self.org_id}"
    