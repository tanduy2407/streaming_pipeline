from kafka_stream import KafkaConsumerStream
import logging
from opensearchpy import OpenSearch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_opensearch():
    os_client = OpenSearch(
        hosts=[{"host": "opensearch", "port": 9200}],
        use_ssl=False,
        verify_certs=False,
    )
    logger.info("✅ Connected to OpenSearch")
    return os_client


def main(org_id: str):
    topic = f"metrics.auth.{org_id}"
    index_name = f"metrics.auth.{org_id}"
    os_client = connect_to_opensearch()
    consumer = KafkaConsumerStream(org_id, topic, "opensearch_group")
    logger.info(f"Connected to topic: {topic}")
    for message in consumer.consume_messages():
        try:
            logger.info(f"Received: {message}")
            # insert into OpenSearch
            response = os_client.index(
                index=index_name,
                body=message
            )
            logger.info(f"Indexed: {response['_id']}")
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")

if __name__ == "__main__":
    org_id = "test_org"
    main(org_id)

    