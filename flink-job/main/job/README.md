
This project is a **PyFlink streaming job** designed for a data engineering assessment.

# 1. OCSF Flink Authentication Aggregation Pipeline
It processes OCSF-normalized authentication events from Kafka (assumed) and performs **tenant-aware windowed aggregation** to compute authentication success and failure counts.

---

## 📌 Overview

The pipeline:

- Consumes authentication events from Kafka topic:  
  `logs.normalized.{org_id}`
- Groups events by:
  - `org_id` (tenant isolation)
- Applies **tumbling processing-time windows**
- Aggregates:
  - Successful login attempts (`status_id = 1`)
  - Failed login attempts (`status_id = 2`)
- Outputs results to:
  - `metrics.auth.{org_id}`
- Routes late events to:
  - `dlq.normalized.{org_id}`

---

## 🧠 Architecture

```
Kafka Source (logs.normalized.{org_id})
        ↓
PyFlink Stream Processing
        ↓
KeyBy (org_id)
        ↓
10-second Tumbling Window (Processing Time)
        ↓
Aggregation (success/failure counts)
        ↓
Kafka Sink (metrics.auth.{org_id})

Late Events → DLQ Topic
```

---

## ⚙️ Key Design Concepts

### 1. Tenant Isolation
Each `org_id` is processed independently using `key_by`, ensuring multi-tenant safety.

### 2. Windowing Strategy
- Type: Tumbling Window  
- Size: 2 minutes 
- Time characteristic: Processing Time  

### 3. Aggregation Logic

| status_id | Meaning  |
|----------|----------|
| 1        | Success  |
| 2        | Failure  |

---

## 🚀 How to Run (Assessment Mode)

This project is designed for assessment purposes, so Kafka and infrastructure are assumed.

### Step 1 — Install dependencies

```bash
pip install apache-flink
```

---

### Step 2 — Run the Flink job

```bash
python window_agg.py
```

---

### Step 3 — Runtime behavior

When executed, the job will:

- Initialize Flink streaming environment
- Simulate consuming from:
  ```
  logs.normalized.{org_id}
  ```
- Process streaming authentication events
- Output aggregated metrics to:
  ```
  metrics.auth.{org_id}
  ```
- Print results to console for validation

---

## 📊 Output Format

### Aggregated result

```json
{
  "org_id": "123",
  "counts": {
    "success": 120,
    "failure": 15
  },
  "window_start": "2026-04-21T10:00:00",
  "window_end": "2026-04-21T10:00:10"
}
```

---

### Late events (DLQ)

Late-arriving events are routed to:

```
dlq.normalized.{org_id}
```

---

# 2.Brute Force Detection Pipeline

This project also includes an optional **brute force detection Flink job** that identifies suspicious authentication behavior.

---

## 📌 Overview

The pipeline:

- Detects repeated failed login attempts per source IP
- Uses session-based windowing for burst detection
- Triggers alerts when failure threshold is exceeded
- Outputs security alerts to:
  - `alerts.{org_id}`
- Routes late events to:
  - `dlq.normalized.{org_id}`

---

## 🧠 Architecture

```
Kafka Source (logs.normalized.{org_id})
        ↓
PyFlink Stream Processing
        ↓
KeyBy (org_id, src_ip)
        ↓
2-minute Session Window
        ↓
Aggregation (Failure Count >= 10)
        ↓
Alert Sink (alerts.{org_id})

Late Events → DLQ Topic
```

---

## ⚙️ Key Design Concepts

### 1. Tenant Isolation
Each `org_id`, `src_ip` is processed independently using `key_by`, ensuring multi-tenant safety.

### 2. Windowing Strategy
- Type: Session Window  
- Size: 2 minutes  
- Time characteristic: Event Time  

### 3. Aggregation Logic

| status_id | Meaning  |
|----------|----------|
| 1        | Success  |
| 2        | Failure  |

### 4. Brute Force Rule
- Count only failed logins (`status_id = 2`)
- Trigger alert when:
  - `failure_count >= 10`
---

## 🚀 How to Run (Assessment Mode)

This project is designed for assessment purposes, so Kafka and infrastructure are assumed.

### Step 1 — Install dependencies

```bash
pip install apache-flink
```

---

### Step 2 — Run the Flink job

```bash
python brute_force_detection.py
```

---

### Step 3 — Runtime behavior

When executed, the job will:

- Initialize Flink streaming environment
- Simulate consuming from:
  ```
  logs.normalized.{org_id}
  ```
- Process streaming authentication events
- Output aggregated metrics to:
  ```
  alerts.{org_id}
  ```
- Print results to console for validation

---

## 📊 Output Format

### Aggregated result

```json
{
  "org_id": "123",
  "src_ip": "10.0.0.1",
  "failure_count": 12,
  "window_start": "2026-04-21T10:00:00",
  "window_end": "2026-04-21T10:02:00",
  "alert_type": "BRUTE_FORCE"
}
```

---

### Late events (DLQ)

Late-arriving events are routed to:

```
dlq.normalized.{org_id}
```

---

## 🔍 Notes
- Processing-time window (not event-time) for aggregation tumbling window
- Kafka topics are assumed infrastructure
- No local Kafka setup required
- Focus is on streaming logic and correctness

---

## ⚠️ Limitations

- No watermarking strategy
- No checkpointing (not required for assessment)
- Kafka is abstracted for simplicity

---

## 🎯 What This Demonstrates

This project demonstrates:

- PyFlink stream processing
- Windowed aggregation patterns
- Multi-tenant design
- Streaming metrics pipeline
- Clean separation of concerns


---
