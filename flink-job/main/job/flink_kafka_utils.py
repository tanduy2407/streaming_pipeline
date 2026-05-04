from pyflink.common.serialization import SimpleStringSchema, DeserializationSchema, SerializationSchema
import json
from typing import Dict, Any
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, KafkaSource
from pyflink.datastream import StreamExecutionEnvironment, OutputTag


class OCSFEventDeserializer(DeserializationSchema):
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
    return KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics(topic) \
        .set_group_id(group_id) \
        .set_value_only_deserializer(OCSFEventDeserializer()) \
        .build()


def create_kafka_sink(topic: str):
    return KafkaSink.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
        .build()


def create_late_tag():
    # Side output tag for handling late events (e.g., late arrivals in event-time processing)
    return OutputTag("late-events")
