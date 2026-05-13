from kafka_stream import KafkaConsumerStream, KafkaProducerStream, KafkaDLQProducer
from registry import NormalizerRegistry
from model.ocsf_authentication_event import OcsfAuthenticationEvent, Endpoint, User, Metadata
from datetime import datetime
from abc import ABC, abstractmethod
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    pass


class Normalizer(ABC):

    @abstractmethod
    def normalize(self, raw_json: str, org_id: str) -> OcsfAuthenticationEvent:
        """
        Convert raw source-specific event into standardized OCSF event.
        Must be implemented by each source-specific normalizer.
        """
        pass


@NormalizerRegistry.register("wazuh")
class WazuhNormalizer(Normalizer):
    def normalize(self, raw_json: dict, org_id: str) -> OcsfAuthenticationEvent:
        try:
            data = raw_json
        except Exception as e:
            raise NormalizationError("Invalid JSON") from e

        try:
            unmapped = {}

            # --- TIME ---
            # Wazuh: "timestamp": ISO8601 → convert to epoch millis
            time = int(
                datetime.fromisoformat(
                    data["timestamp"].replace("Z", "+00:00"))
                .timestamp() * 1000
            )

            # --- STATUS ---
            # Mapping logic:
            # - data.result == "success" → 1
            # - data.result == "failure" OR description contains "failed" → 2
            result = data.get("data", {}).get("result", "").lower()
            description = data.get("rule", {}).get("description", "").lower()

            if result == "success":
                status_id = 1
            elif result == "failure":
                status_id = 2

            # --- USER ---
            # OCSF decision:
            # - dstuser = authenticated account → user.name
            user_name = data.get("data", {}).get("dstuser")

            # --- SRC ENDPOINT ---
            src_ip = data.get("data", {}).get("srcip")

            # --- DST ENDPOINT ---
            # Using agent.name as destination host
            hostname = data.get("agent", {}).get("name")

            # --- SEVERITY ---
            severity = data.get("rule", {}).get("level")

            # --- UNMAPPED ---
            for key in data:
                if key not in {"timestamp", "data", "agent", "rule"}:
                    unmapped[key] = data[key]

            return OcsfAuthenticationEvent(
                time=time,
                status_id=status_id,
                severity_id=severity,
                src_endpoint=Endpoint(ip=src_ip),
                dst_endpoint=Endpoint(hostname=hostname),
                user=User(name=user_name),
                metadata=Metadata(
                    product="wazuh",
                    version=None,
                    org_id=org_id
                ),
                unmapped=unmapped
            )
        except Exception as e:
            raise NormalizationError("Failed to normalize Wazuh event") from e


@NormalizerRegistry.register("cloudtrail")
class CloudTrailNormalizer(Normalizer):
    def normalize(self, raw_json: dict, org_id: str) -> OcsfAuthenticationEvent:
        try:
            data = raw_json
        except Exception as e:
            raise NormalizationError("Invalid JSON") from e
        try:
            unmapped = {}

            # --- TIME ---
            # CloudTrail: eventTime
            time = int(
                datetime.fromisoformat(
                    data["eventTime"].replace("Z", "+00:00"))
                .timestamp() * 1000
            )

            # --- STATUS ---
            # Mapping:
            # responseElements.ConsoleLogin = Success / Failure
            login_status = (
                data.get("responseElements", {})
                .get("ConsoleLogin", "")
                .lower()
            )

            if login_status == "success":
                status_id = 1
            elif login_status == "failure":
                status_id = 2

            # --- USER ---
            user_name = data.get("userIdentity", {}).get("userName")

            # --- SRC ENDPOINT ---
            src_ip = data.get("sourceIPAddress")

            # --- UNMAPPED ---
            for key in data:
                if key not in {
                    "eventTime",
                    "responseElements",
                    "userIdentity",
                    "sourceIPAddress"
                }:
                    unmapped[key] = data[key]

            return OcsfAuthenticationEvent(
                time=time,
                status_id=status_id,
                src_endpoint=Endpoint(ip=src_ip),
                user=User(name=user_name),
                metadata=Metadata(
                    product="cloudtrail",
                    version=None,
                    org_id=org_id
                ),
                unmapped=unmapped
            )
        except Exception as e:
            raise NormalizationError(
                "Failed to normalize CloudTrail event") from e


def detect_source_type(raw_json: dict):
    """
    Detect source system based on presence of key fields.
    This is a simple heuristic (can be replaced with schema registry in production).
    """
    if all(k in raw_json for k in ["agent", "rule"]):
        return "wazuh"
    if all(k in raw_json for k in ["awsRegion", "userIdentity"]):
        return "cloudtrail"
    raise ValueError("Unknown source type")


def normalize_event(raw_json: str, org_id: str):
    """
    Detect source type and apply corresponding normalizer.
    """
    source_type = detect_source_type(raw_json)
    print(f"Detected source type: {source_type}")

    # Retrieve correct normalizer from registry
    normalizer = NormalizerRegistry.get(source_type)
    return normalizer.normalize(raw_json, org_id)


def build_dlq_event(raw_message: dict, error: Exception, org_id: str):
    return {
        "org_id": org_id,
        "error": str(error),
        "error_type": type(error).__name__,
        "raw_event": raw_message,
        "failed_at": datetime.utcnow().isoformat() + "Z"
    }


def stream_pipeline(org_id: str):
    """
    End-to-end streaming pipeline:
    - Consume raw events from Kafka topic logs.raw.{org_id}
    - Normalize to OCSF format
    - Publish to logs.normalized.{org_id}
    """
    consumer = None
    producer = None
    dlq_producer = None
    try:
        consumer = KafkaConsumerStream(org_id, f"logs.raw.{org_id}", f"normalizer_group")
        producer = KafkaProducerStream(org_id)
        dlq_producer = KafkaDLQProducer(org_id)
        logger.info(f"Connected to topic: logs.raw.{org_id}")
        logger.info(f"Publishing to topic: logs.normalized.{org_id}")
        logger.info("Waiting for messages...")

        message_count = 0
        for message in consumer.consume_messages():
            try:
                message_count += 1
                logger.info(f"Received message #{message_count}")

                # Normalize the event
                normalized_event = normalize_event(message, org_id)
                print(normalized_event)
                logger.info(
                    f"Normalized event: {normalized_event.metadata.product}")

                # Publish to normalized topic
                producer.publish_message(normalized_event.to_dict())
                logger.info(f"Published normalized event #{message_count}")

            except NormalizationError as e:
                # Handle known normalization issues (bad schema, missing fields, etc.)
                logger.exception("Normalization error")
                # Send to DLQ
                dlq_event = build_dlq_event(message, e, org_id)
                dlq_producer.publish_message(dlq_event)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Producer closed")
        if consumer:
            consumer.close()
            logger.info("Consumer closed")


if __name__ == "__main__":
    org_id = "test_org"
    stream_pipeline(org_id)
