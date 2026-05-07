"""
Flink job for detecting brute force attacks based on failed authentication attempts.
"""

import json

from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.common.time import Time
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from datetime import datetime
import json
from pyflink.common.typeinfo import Types
from flink_kafka_utils import create_env, create_kafka_source, create_kafka_sink, create_late_tag


class BruteForceAggregator(AggregateFunction):
    """Incrementally aggregates failed login attempts per (org_id, src_ip)."""

    def create_accumulator(self):
        # Initialize accumulator state for each key/window
        return {
            "org_id": None,
            "src_ip": None,
            "failure_count": 0
        }

    def add(self, value, acc):
        # Extract relevant fields from event
        metadata = value.get("metadata", {})
        src = value.get("src_endpoint", {})

        # Initialize key-level metadata once
        if acc["org_id"] is None:
            acc["org_id"] = metadata.get("org_id")
            acc["src_ip"] = src.get("ip")

        # Count only failed authentication attempts (status_id == 2)
        if value.get("status_id") == 2:
            acc["failure_count"] += 1

        return acc

    def get_result(self, acc):
        # Return final aggregated result for the window
        return acc

    def merge(self, acc1, acc2):
        # Merge accumulators (needed for session windows / parallel execution)
        acc1["failure_count"] += acc2["failure_count"]
        return acc1


class BruteForceWindowFunction(ProcessWindowFunction):
    """Evaluates aggregated results and emits alert if threshold is exceeded."""

    def process(self, key, context, aggregates):
        # Since AggregateFunction outputs one result, extract it
        result = next(iter(aggregates), None)
        if result is None:
            return

        # Apply threshold filter (only alert on suspicious behavior)
        if result["failure_count"] < 10:
            return

        # Extract window boundaries (event-time)
        window_start = context.window().start
        window_end = context.window().end

        # Emit alert downstream (Kafka sink)
        yield {
            "org_id": result["org_id"],
            "src_ip": result["src_ip"],
            "failure_count": result["failure_count"],
            "window_start": datetime.fromtimestamp(window_start / 1000).isoformat(),
            "window_end": datetime.fromtimestamp(window_end / 1000).isoformat(),
            "alert_type": "BRUTE_FORCE"
        }


class EventTimeAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return int(event["time"])


def assign_event_time(stream):
    return stream.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimeAssigner())  # ✅ correct
    )


# Side output tag for late events (arriving after allowed lateness)
late_tag = create_late_tag()
print(f"Late events will be sent to side output: {late_tag}")
# Allow late events up to 30 seconds after window closes
late_duration = 30000


def build_bruteforce_pipeline(kafka_stream):
    """Builds the core Flink pipeline for brute force detection."""
    return (
        kafka_stream
        # Key by tenant + source IP (isolation per org and attacker)
        .key_by(lambda e: (
            e["metadata"]["org_id"],
            e["src_endpoint"]["ip"]
        ))
        # Session window groups events based on inactivity gap
        # If no event arrives within 2 minutes, session is closed
        .window(EventTimeSessionWindows.with_gap(Time.minutes(2)))
        # Allow late events to still modify existing sessions
        # Important: late events may extend or merge sessions, will be handled by Flink
        .allowed_lateness(late_duration)
        # Redirect very late events to side output (DLQ)
        .side_output_late_data(late_tag)
        # Incremental aggregation + final window processing
        .aggregate(
            BruteForceAggregator(),
            BruteForceWindowFunction()
        )
    )


def create_flink_job(org_id: str):
    """Creates and wires the full Flink job (source → processing → sinks)."""

    # Initialize Flink execution environment
    env = create_env(parallelism=1)

    # Define Kafka topics (multi-tenant pattern)
    source_topic = f"logs.normalized.{org_id}"
    sink_topic = f"alerts.{org_id}"
    dlq_topic = f"dlq.normalized.{org_id}"

    # Create Kafka source (ingest normalized logs)
    kafka_source = create_kafka_source(
        topic=source_topic,
        group_id="flink-aggregation-group"
    )

    kafka_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    ).map(
        lambda x: json.loads(x))

    # Assign event time and watermarks for proper windowing and late event handling
    kafka_stream = assign_event_time(kafka_stream)

    # Build detection pipeline
    bruteforce_stream = build_bruteforce_pipeline(kafka_stream)
    late_stream = bruteforce_stream.get_side_output(late_tag)

    main_stream = bruteforce_stream

    main_stream = (
        main_stream
        .filter(lambda x: isinstance(x, dict))
        .map(lambda x: json.dumps(x), output_type=Types.STRING())
    )
    # Main output: detected brute force alerts
    main_stream.sink_to(create_kafka_sink(sink_topic))

    # Debug / local visibility
    bruteforce_stream.print()

    # Handle late events separately (DLQ for reprocessing or audit)
    late_stream = (
        late_stream
        .filter(lambda x: isinstance(x, dict))
        .map(lambda x: json.dumps(x), output_type=Types.STRING())
    )

    late_stream.sink_to(create_kafka_sink(dlq_topic))
    return env


def main():
    org_id = "test_org"  # In production, this could be passed as an argument or env variable
    """Main entry point for the Flink job."""
    print("Starting OCSF Event Aggregation Flink Job...")
    print(f"- Input Topic: logs.normalized.{org_id}")
    print(f"- Output Topic: alerts.{org_id}")
    print(f"- Session Window: 2-minute gap")
    print(f"- Brute Force Threshold: 10 failed attempts")

    # Build and execute Flink job
    env = create_flink_job(org_id)
    env.execute("OCSF-Brute-Force-Detection")


if __name__ == "__main__":
    main()
