from kafka_stream import KafkaConsumerStream, KafkaProducerStream, KafkaDLQProducer
import logging
from opensearchpy import OpenSearch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_opensearch():
    # ----------------------------
    # OpenSearch client
    # ----------------------------
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
    consumer = KafkaConsumerStream(org_id, topic)
    logger.info(f"Connected to topic: {topic}")
    for message in consumer.consume_messages():
        event = message.value

        print("Received:", event)

        # insert into OpenSearch
        response = os_client.index(
            index=index_name,
            body=event
        )

        print("Indexed:", response["_id"])

if __name__ == "__main__":
    org_id = "test_org"
    main(org_id)

    