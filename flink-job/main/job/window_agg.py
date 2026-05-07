"""
Flink job for tenant-aware windowed aggregation of OCSF authentication events.

Consumes OCSF-normalized events from Kafka topic `logs.normalized.{org_id}`
and computes tumbling windows that count authentication events 
grouped by status_id (success/failure) per org_id.
"""

from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.common.time import Time
from pyflink.common import WatermarkStrategy
from datetime import datetime
from typing import Dict, Any
import json
from pyflink.common.typeinfo import Types
from flink_kafka_utils import create_env, create_kafka_source, create_kafka_sink, create_late_tag


class AuthEventAggregator(AggregateFunction):
    """
    Incrementally aggregates authentication events by status_id.
    Uses a compact accumulator to avoid storing raw events.
    """

    def create_accumulator(self) -> Dict[str, Any]:
        # Initialize per-key/window state
        return {
            "org_id": None,
            "counts": {
                "success": 0,  # status_id = 1
                "failure": 0,  # status_id = 2
            }
        }

    def add(self, value: Dict[str, Any], accumulator: Dict[str, Any]) -> Dict[str, Any]:
        # Initialize org_id once (first event in window)
        if accumulator["org_id"] is None:
            accumulator["org_id"] = value.get("metadata").get("org_id")

        # Map status_id → logical label
        status_id = value.get("status_id")
        status_key = "success" if status_id == 1 else "failure"

        # Increment counter (NOTE: non-1 values are treated as failure)
        accumulator["counts"][status_key] += 1

        return accumulator

    def get_result(self, accumulator: Dict[str, Any]) -> Dict[str, Any]:
        # Emit aggregated counts
        return accumulator

    def merge(self, accumulator1: Dict[str, Any], accumulator2: Dict[str, Any]) -> Dict[str, Any]:
        # Merge partial aggregates (important for parallelism)
        if accumulator1["org_id"] is None:
            accumulator1["org_id"] = accumulator2["org_id"]

        for status_key in accumulator2["counts"]:
            accumulator1["counts"][status_key] += accumulator2["counts"][status_key]

        return accumulator1


class WindowResultFunction(ProcessWindowFunction):
    """
    Enriches aggregation result with window metadata (start/end timestamps).
    """

    def process(self, key, context, aggregates):
        # AggregateFunction emits a single result per window
        result = next(iter(aggregates), None)
        if result is None:
            return

        # Extract window boundaries (processing time)
        window_start = context.window().start
        window_end = context.window().end

        # Attach ISO timestamps for downstream consumers
        result["window_start"] = datetime.fromtimestamp(
            window_start / 1000).isoformat()
        result["window_end"] = datetime.fromtimestamp(
            window_end / 1000).isoformat()

        yield result


# Side output tag for late events
late_tag = create_late_tag()

# Allowed lateness (NOTE: only meaningful for event-time windows)
late_duration = Time.seconds(30)


def build_windowagg_pipeline(kafka_stream):
    """Builds the aggregation pipeline."""
    return (
        kafka_stream
        # Partition stream by tenant (org_id)
        .key_by(lambda e: e["metadata"]["org_id"])

        # Tumbling window based on processing time
        .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))

        # NOTE: allowed_lateness + late data handling has no effect in processing-time windows
        .allowed_lateness(late_duration)
        .side_output_late_data(late_tag)

        # Incremental aggregation + window finalization
        .aggregate(
            AuthEventAggregator(),
            WindowResultFunction()
        )
    )


def create_flink_job(org_id: str):
    """Creates full Flink job (source → transform → sink)."""

    env = create_env(parallelism=1)

    # Multi-tenant topic naming
    source_topic = f"logs.normalized.{org_id}"
    sink_topic = f"metrics.auth.{org_id}"
    dlq_topic = f"dlq.normalized.{org_id}"

    # Kafka source
    kafka_source = create_kafka_source(
        topic=source_topic,
        group_id="flink-aggregation-group"
    )
    print(f"✅ Kafka source created for topic: {source_topic}")
    kafka_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-source"
    ).map(
        lambda x: json.loads(x)
    )
    print("✅ Kafka source added to Flink environment")

    # Build aggregation pipeline
    windowagg_stream = build_windowagg_pipeline(kafka_stream)
    late_stream = windowagg_stream.get_side_output(late_tag)
    main_stream = windowagg_stream

    print("✅ Window aggregation pipeline built")

    main_stream = (
        main_stream
        .filter(lambda x: isinstance(x, dict))
        .map(lambda x: json.dumps(x), output_type=Types.STRING())
    )
    print("✅ Window aggregation results mapped to JSON strings")

    # Main sink: aggregated metrics
    main_stream.sink_to(create_kafka_sink(sink_topic))
    print(f"✅ Kafka sink created for topic: {sink_topic}")

    # Debug output (useful locally, remove in production)
    main_stream.print()

    # Side output: late events (will be empty with processing-time windows)
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
    print(f"- Output Topic: metrics.auth.{org_id}")
    print("- Window: 10-second tumbling (processing time)")
    print("- Aggregation: Count by status_id (success=1/failure=2)\n")

    env = create_flink_job(org_id)
    env.execute("OCSF-Authentication-Event-Aggregation")


if __name__ == "__main__":
    main()
