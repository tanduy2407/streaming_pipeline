import datetime

from kafka import KafkaConsumer, KafkaProducer
import json


class KafkaConsumerStream:
    def __init__(self, org_id: str):
        """Initialize Kafka consumer for consuming raw logs."""
        self.org_id = org_id
        self.topic = f"logs.raw.{org_id}"
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers="kafka:29092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="normalizer-group"
        )

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
        self.producer = KafkaProducer(
            bootstrap_servers="kafka:29092",
            value_serializer=lambda m: json.dumps(m).encode("utf-8")
        )

    def publish_message(self, message: dict):
        """Publish a message to Kafka topic."""
        topic = f"logs.normalized.{self.org_id}"
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
        self.topic = f"logs.dlq.{org_id}"

def build_dlq_event(raw_message: dict, error: Exception, org_id: str):
    return {
        "org_id": org_id,
        "error": str(error),
        "error_type": type(error).__name__,
        "raw_event": raw_message,
        "failed_at": datetime.utcnow().isoformat() + "Z"
    }