# Streaming Security Pipeline
## 1.Architecture Overview
The pipeline ingests, processes, and serves authentication events in a multi-tenant environment:

### Ingestion Layer
- Push-based: External systems send events via webhook
- Pull-based: Scheduled workers poll customer S3 buckets
### Streaming Layer (Kafka)
Raw events are written to:
`logs.raw.{org_id}`

After normalization (OCSF), events are published to:
`logs.normalized.{org_id}`

### Processing Layer (Flink)
Consumes normalized events

Performs:
- Data enrichment
- Windowed aggregation (e.g., success/failure counts)
- Handles late and out-of-order events (if event-time is used)
- Invalid or unparseable events are routed to: `dlq.normalized.{org_id}`

### Storage Layer (OpenSearch)
Aggregated results are written to OpenSearch

Data is tiered into hot/warm storage for cost-performance balance

## 2.Build Instructions
### 1. Prerequisites
- Python 3.9+
- Java 8 or 11
- Apache Flink (1.17+)
- Kafka cluster
- (Optional) Docker for local setup
### 2.Install Dependencies
```bash
pip install -r requirements.txt
```
### 3. Run the Kafka job
```bash
python normalized.py
```

### 4. Run the Flink job
```bash
python window_agg.py
python brute_force_detection.py
```

### 5. Run the the unit test
```bash
pytest normalizer\test\unit_test.py
pytest flink-job\test\test_job.py
```
## 3. Assumptions
### 1. Kafka Infrastructure is Pre-Provisioned
A Kafka cluster is already available and fully operational

All required topics are pre-created for all tenants:
- logs.raw.{org_id}
- logs.normalized.{org_id}
- dlq.normalized.{org_id}
Topic configuration (partitions, replication, retention) is managed externally

👉 Scope clarification:
This project does not include Kafka setup or topic management.

It focuses on:
- pulling data from Kafka, normalize and produce to Kafka topic
- processing data from Kafka and aggregating data with Flink jobs

### 2.Data Pipeline Scope
The implementation starts from: `logs.raw.{org_id}`

The following components are out of scope:
- Webhook ingestion service
- S3 polling workers
- Kafka cluster provisioning
- Flink cluster

👉 Scope focus:
- Consume raw envents data come from webhook and S3 polling in Kafka topic
- Consume and normalize the raw events into a define templed from Kafka
- Perform aggregation and enrichment in Flink jobs
- Handle failures (DLQ)
- Write results to OpenSearch

### 3. Multi-Tenant Topic Availability
The system assumes:

Topics for all org_id already exist

New tenants will have topics provisioned outside this pipeline

The Flink job uses: 
- a pattern subscription (logs.normalized.{org_id}) to dynamically consume all tenants
- a pattern publish (metrics.auth.{org_id}, alerts.{org_id}) to dynamically publish to all tenants

## 4. Data Flow
Webhook / S3 Poller  
  ↓  
`logs.raw.{org_id}`  
  ↓  
Normalization Layer  
  ↓  
`logs.normalized.{org_id}`  
  ↓  
Flink Job  
  ↓  
OpenSearch + `dlq.normalized.{org_id}`

### Design Decisions & Trade-offs
#### 1. Per-Tenant Topics
✅ Strong isolation (no cross-tenant mixing)

✅ Easier debugging and replay per tenant

❌ Large number of topics as tenants grow

👉 Trade-off: Chosen for isolation and scalability, at the cost of higher Kafka metadata overhead.

#### 2. Pattern-Based Consumption

Consumers subscribe to:

logs.normalized.{org_id}

✅ Automatically includes new tenants

❌ Less control compared to explicit topic subscription

#### 3. At-Least-Once Delivery
Kafka guarantees at-least-once delivery

Combined with Flink checkpointing for correctness

❌ Possible duplicate events at boundaries

👉 Trade-off: Duplicates are tolerated and handled downstream rather than enforcing stricter guarantees at Kafka level.

#### 4. Dead Letter Queue (DLQ)
✅ Prevents pipeline crashes due to bad data

✅ Enables debugging and reprocessing

❌ Requires monitoring and storage

## 5. Kafka Layer
### Overview

Apache Kafka acts as the central event streaming backbone in this architecture. It decouples ingestion, processing, and storage layers while enabling scalable, real-time, multi-tenant data processing.

This project does not provision or manage Kafka infrastructure. Instead, it consumes from and produces to existing Kafka topics as part of the data pipeline.

### Topic Design

The system uses a per-tenant topic pattern to ensure isolation and scalability.

#### 1. Raw Events
`logs.raw.{org_id}`

Contains unprocessed events from:
- Webhook ingestion (push-based)
- S3 polling workers (pull-based)

Format may vary depending on the source system

#### 2. Normalized Events
`logs.normalized.{org_id}`

Store:
- Contains events transformed into OCSF format
- Serves as the input to the Flink job
- Guarantees a consistent schema across all tenants

#### 3. Dead Letter Queue (DLQ)
`dlq.raw.{org_id}`

Stores:
- Invalid JSON messages
- Schema validation failures
- Unprocessable events
- Enables debugging and replay

## 6. Flink Layer
### 1. Source (Kafka Consumer)
The job consumes from:

`logs.normalized.{org_id}`

Uses pattern-based subscription to dynamically include all tenants (org_id)

Each event is assumed to follow the OCSF schema

Deserialization
- JSON → structured object (Python dict)
- Invalid messages are immediately routed to the DLQ

### 3. Keying Strategy (Multi-Tenant Isolation)
Stream will be key by org_id and src_ip
- All downstream operations are scoped per tenant
- Prevents cross-tenant data mixing
- Enables parallel and scalable processing

### 5. Windowing Strategy

Depending on implementation:

#### Tumbling Windows
Fixed-size windows with 2 minutes

```python
TumblingEventTimeWindows.of(Time.minutes(2))
```
- Simple and predictable
- Good for dashboards

Aggregated data will be publish to: `metrics.auth.{org_id}`

#### Session Windows
Groups bursts of activity
More natural for user/session behavior

```python
EventTimeSessionWindows.with_gap(Time.minutes(2))
```

Aggregated data will be publish to: `alerts.{org_id}`

#### Handle late events
Configure an allowed-lateness of 30 seconds on both the tumbling window and the session window

Late events may:
- Extend sessions
- Merge previously separate sessions

Late events after allow-lateness will be publish to DLQ topic:` dlq.normalized.{org_id}`

### 6. Aggregation Logic

Within each window:

Group by:
- org_id
- status_id

Compute:
- Count of success events
- Count of failure events