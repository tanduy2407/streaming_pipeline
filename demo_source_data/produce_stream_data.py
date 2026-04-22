"""Test producer to send sample events to Kafka"""
from kafka import KafkaProducer
import json
import time
import random

# Sample events - valid JSON only
events = [
    # --- WAZUH STYLE (10) ---
    {
        "timestamp": "2026-04-10T10:01:11.000Z",
        "agent": {"id": "002", "name": "web-srv-02"},
        "rule": {"id": "5716", "level": 6, "description": "sshd: authentication failed"},
        "data": {
            "srcip": "10.0.2.15",
            "srcuser": "unknown",
            "dstuser": "admin",
            "result": "failed"
        },
        "location": "/var/log/auth.log",
        "decoder": {"name": "sshd"}
    },
    {
        "timestamp": "2026-04-10T10:05:45.000Z",
        "agent": {"id": "003", "name": "db-srv-01"},
        "rule": {"id": "1002", "level": 4, "description": "sudo command executed"},
        "data": {
            "srcip": "10.0.3.22",
            "srcuser": "dbadmin",
            "dstuser": "root",
            "result": "success"
        },
        "location": "/var/log/syslog",
        "decoder": {"name": "sudo"}
    },
    {
        "timestamp": "2026-04-10T10:10:01.000Z",
        "agent": {"id": "004", "name": "api-srv-01"},
        "rule": {"id": "2001", "level": 3, "description": "file accessed"},
        "data": {
            "srcip": "10.0.4.10",
            "srcuser": "service",
            "dstuser": "n/a",
            "result": "success"
        },
        "location": "/var/log/app.log",
        "decoder": {"name": "app"}
    },
    {
        "timestamp": "2026-04-10T10:12:30.000Z",
        "agent": {"id": "005", "name": "web-srv-03"},
        "rule": {"id": "5715", "level": 5, "description": "sshd: authentication success"},
        "data": {
            "srcip": "10.0.1.50",
            "srcuser": "alice",
            "dstuser": "ubuntu",
            "result": "success"
        },
        "location": "/var/log/auth.log",
        "decoder": {"name": "sshd"}
    },
    {
        "timestamp": "2026-04-10T10:20:12.000Z",
        "agent": {"id": "006", "name": "batch-srv-01"},
        "rule": {"id": "3001", "level": 7, "description": "process started"},
        "data": {
            "srcip": "10.0.5.5",
            "srcuser": "batch",
            "dstuser": "system",
            "result": "success"
        },
        "location": "/var/log/process.log",
        "decoder": {"name": "process"}
    },
    {
        "timestamp": "2026-04-10T10:25:33.000Z",
        "agent": {"id": "007", "name": "proxy-srv-01"},
        "rule": {"id": "4001", "level": 2, "description": "connection allowed"},
        "data": {
            "srcip": "10.0.6.18",
            "srcuser": "guest",
            "dstuser": "proxy",
            "result": "success"
        },
        "location": "/var/log/proxy.log",
        "decoder": {"name": "proxy"}
    },
    {
        "timestamp": "2026-04-10T10:30:00.000Z",
        "agent": {"id": "008", "name": "mail-srv-01"},
        "rule": {"id": "5001", "level": 5, "description": "email sent"},
        "data": {
            "srcip": "10.0.7.9",
            "srcuser": "mailer",
            "dstuser": "external",
            "result": "success"
        },
        "location": "/var/log/mail.log",
        "decoder": {"name": "smtp"}
    },
    {
        "timestamp": "2026-04-10T10:35:42.000Z",
        "agent": {"id": "009", "name": "web-srv-04"},
        "rule": {"id": "5716", "level": 6, "description": "sshd: authentication failed"},
        "data": {
            "srcip": "10.0.1.77",
            "srcuser": "root",
            "dstuser": "root",
            "result": "failed"
        },
        "location": "/var/log/auth.log",
        "decoder": {"name": "sshd"}
    },
    {
        "timestamp": "2026-04-10T10:40:18.000Z",
        "agent": {"id": "010", "name": "cache-srv-01"},
        "rule": {"id": "6001", "level": 3, "description": "cache cleared"},
        "data": {
            "srcip": "10.0.8.8",
            "srcuser": "cache",
            "dstuser": "system",
            "result": "success"
        },
        "location": "/var/log/cache.log",
        "decoder": {"name": "cache"}
    },

    # --- CLOUDTRAIL STYLE (10) ---
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:01:22Z",
        "eventSource": "ec2.amazonaws.com",
        "eventName": "StartInstances",
        "awsRegion": "us-east-1",
        "sourceIPAddress": "198.51.100.1",
        "userIdentity": {"type": "IAMUser", "userName": "bob"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:05:11Z",
        "eventSource": "s3.amazonaws.com",
        "eventName": "PutObject",
        "awsRegion": "us-west-2",
        "sourceIPAddress": "198.51.100.2",
        "userIdentity": {"type": "IAMUser", "userName": "carol"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:10:45Z",
        "eventSource": "iam.amazonaws.com",
        "eventName": "CreateUser",
        "awsRegion": "us-east-1",
        "sourceIPAddress": "198.51.100.3",
        "userIdentity": {"type": "Root"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:15:00Z",
        "eventSource": "ec2.amazonaws.com",
        "eventName": "StopInstances",
        "awsRegion": "us-east-2",
        "sourceIPAddress": "198.51.100.4",
        "userIdentity": {"type": "IAMUser", "userName": "dave"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:20:30Z",
        "eventSource": "signin.amazonaws.com",
        "eventName": "ConsoleLogin",
        "awsRegion": "us-east-1",
        "sourceIPAddress": "198.51.100.5",
        "userIdentity": {"type": "IAMUser", "userName": "eve"},
        "responseElements": {"ConsoleLogin": "Failure"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:25:10Z",
        "eventSource": "lambda.amazonaws.com",
        "eventName": "Invoke",
        "awsRegion": "us-west-1",
        "sourceIPAddress": "198.51.100.6",
        "userIdentity": {"type": "IAMUser", "userName": "frank"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:30:55Z",
        "eventSource": "dynamodb.amazonaws.com",
        "eventName": "PutItem",
        "awsRegion": "ap-southeast-1",
        "sourceIPAddress": "198.51.100.7",
        "userIdentity": {"type": "IAMUser", "userName": "grace"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:35:33Z",
        "eventSource": "rds.amazonaws.com",
        "eventName": "CreateDBInstance",
        "awsRegion": "eu-west-1",
        "sourceIPAddress": "198.51.100.8",
        "userIdentity": {"type": "IAMUser", "userName": "henry"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:40:44Z",
        "eventSource": "cloudwatch.amazonaws.com",
        "eventName": "PutMetricData",
        "awsRegion": "us-east-1",
        "sourceIPAddress": "198.51.100.9",
        "userIdentity": {"type": "IAMUser", "userName": "ivy"},
        "responseElements": {"status": "success"}
    },
    {
        "eventVersion": "1.09",
        "eventTime": "2026-04-10T11:45:59Z",
        "eventSource": "ecs.amazonaws.com",
        "eventName": "RunTask",
        "awsRegion": "ap-northeast-1",
        "sourceIPAddress": "198.51.100.10",
        "userIdentity": {"type": "IAMUser", "userName": "jack"},
        "responseElements": {"status": "success"}
    }
]
random.shuffle(events)
def send_events(org_id: str):
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda m: json.dumps(m).encode("utf-8")
    )
    
    topic = f"logs.raw.{org_id}"
    
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
    finally:
        producer.close()

if __name__ == "__main__":
    org_id = "123"
    send_events(org_id)
