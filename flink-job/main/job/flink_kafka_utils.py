from pyflink.common.serialization import SimpleStringSchema, SerializationSchema
import json
from typing import Dict, Any
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import OutputTag


class OCSFEventDeserializer(SimpleStringSchema):
    """Deserializer for OCSF events coming from Kafka (JSON -> Python dict)."""

    def deserialize(self, message: bytes) -> Dict[str, Any]:
        # Convert raw Kafka bytes into a Python dictionary
        try:
            return json.loads(message.decode("utf-8"))
        except Exception as e:
            # Handle malformed messages gracefully to avoid job failure
            print(f"Error deserializing message: {e}")
            return {}  # Return empty dict as fallback


class AuthEventSerializer(SerializationSchema):
    """Serializer for sending processed/aggregated events back to Kafka."""

    def serialize(self, element: Dict[str, Any]) -> bytes:
        # Convert Python dict into JSON bytes for Kafka producer
        return json.dumps(element).encode("utf-8")


def create_env(parallelism: int = 1) -> StreamExecutionEnvironment:
    # Initialize Flink streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism (number of parallel operator instances)
    env.set_parallelism(parallelism)
    
    return env


def create_kafka_source(topic: str, group_id: str):
    # Kafka broker configuration
    kafka_brokers = "localhost:9092"

    # Consumer configuration
    consumer_properties = {
        "bootstrap.servers": kafka_brokers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",  # Start from earliest if no offset
        "enable.auto.commit": "true"      # Kafka manages offset commits
    }

    # Create Kafka consumer source
    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=OCSFEventDeserializer(),  # Custom JSON deserializer
        properties=consumer_properties
    )


def create_kafka_sink(topic: str):
    # Kafka broker configuration
    kafka_brokers = "localhost:9092"

    # Producer configuration
    producer_properties = {
        "bootstrap.servers": kafka_brokers,
        "acks": "all",                 # Strong durability guarantee
        "compression.type": "snappy"   # Improve throughput and reduce size
    }

    # Create Kafka producer sink
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=AuthEventSerializer(),  # Custom JSON serializer
        producer_config=producer_properties
    )


def create_late_tag():
    # Side output tag for handling late events (e.g., late arrivals in event-time processing)
    return OutputTag("late-events")