"""Test producer to send sample events to Kafka"""
from kafka import KafkaProducer
import json
import time
import random

with open("/app/events.json", "r") as f:
    events = json.load(f)
random.shuffle(events)

def send_events(org_id: str):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:29092",
                value_serializer=lambda m: json.dumps(m).encode("utf-8")
            )
            print("✅ Connected to Kafka")
            break
        except Exception as e:
            print(f"Error connecting to Kafka: {e}")
            time.sleep(5)  # Retry after delay
        
    topic = f"logs.raw.{org_id}"
    while True:
        try:
            for i, event in enumerate(events):
                print(f"Sending event {i+1} to {topic}...")
                producer.send(topic, event)
                print(f"Event {i+1} sent: {event.get('timestamp', event.get('eventTime', 'N/A'))}")
                time.sleep(random.randint(1, 3))  # Simulate delay between events
            
            producer.flush()
            print(f"\nAll {len(events)} events sent successfully!")
        except Exception as e:
            print(f"Error sending events: {e}")

if __name__ == "__main__":
    org_id = "test_org"
    print(f"Starting event producer for org_id: {org_id}")
    send_events(org_id)
