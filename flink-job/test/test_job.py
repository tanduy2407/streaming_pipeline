"""Unit tests for the Flink aggregation job."""

import pytest
from datetime import datetime
from job import (
    AuthEventAggregator,
    AuthEventWindowProcessor,
    OCSFEventDeserializer,
    AuthEventSerializer
)


class TestAuthEventAggregator:
    """Test cases for AuthEventAggregator."""

    @pytest.fixture
    def aggregator(self):
        return AuthEventAggregator()

    def test_create_accumulator(self, aggregator):
        """Test initialization of accumulator."""
        acc = aggregator.create_accumulator()

        assert acc["org_id"] is None
        assert "counts" in acc
        assert acc["counts"]["status_1"] == 0  # Success
        assert acc["counts"]["status_2"] == 0  # Failure
        assert acc["counts"]["status_3"] == 0  # Unknown

    def test_add_single_success_event(self, aggregator):
        """Test adding a single success event."""
        acc = aggregator.create_accumulator()
        event = {
            "org_id": "123",
            "status_id": 1,
            "user": {"name": "alice"},
            "src_endpoint": {"ip": "10.0.0.1"}
        }

        result_acc = aggregator.add(event, acc)

        assert result_acc["org_id"] == "123"
        assert result_acc["counts"]["status_1"] == 1
        assert result_acc["counts"]["status_2"] == 0

    def test_add_multiple_events_mixed_status(self, aggregator):
        """Test adding multiple events with different statuses."""
        acc = aggregator.create_accumulator()
        
        events = [
            {"org_id": "123", "status_id": 1},  # Success
            {"org_id": "123", "status_id": 1},  # Success
            {"org_id": "123", "status_id": 2},  # Failure
            {"org_id": "123", "status_id": 1},  # Success
        ]

        for event in events:
            acc = aggregator.add(event, acc)

        assert acc["counts"]["status_1"] == 3  # 3 successes
        assert acc["counts"]["status_2"] == 1  # 1 failure
        assert acc["counts"]["status_3"] == 0  # 0 unknown

    def test_add_event_with_missing_status_id(self, aggregator):
        """Test adding event with missing status_id (defaults to unknown)."""
        acc = aggregator.create_accumulator()
        event = {"org_id": "123"}  # No status_id

        result_acc = aggregator.add(event, acc)

        assert result_acc["counts"]["status_3"] == 1  # Counts as unknown

    def test_get_result(self, aggregator):
        """Test get_result returns the accumulator."""
        acc = aggregator.create_accumulator()
        event = {"org_id": "456", "status_id": 1}
        acc = aggregator.add(event, acc)

        result = aggregator.get_result(acc)

        assert result["org_id"] == "456"
        assert result["counts"]["status_1"] == 1

    def test_merge_accumulators(self, aggregator):
        """Test merging two accumulators."""
        acc1 = aggregator.create_accumulator()
        acc1["org_id"] = "789"
        acc1["counts"]["status_1"] = 5
        acc1["counts"]["status_2"] = 2

        acc2 = aggregator.create_accumulator()
        acc2["org_id"] = "789"
        acc2["counts"]["status_1"] = 3
        acc2["counts"]["status_3"] = 1

        merged = aggregator.merge(acc1, acc2)

        assert merged["org_id"] == "789"
        assert merged["counts"]["status_1"] == 8  # 5 + 3
        assert merged["counts"]["status_2"] == 2  # 2 + 0
        assert merged["counts"]["status_3"] == 1  # 0 + 1


class TestAuthEventWindowProcessor:
    """Test cases for AuthEventWindowProcessor."""

    @pytest.fixture
    def processor(self):
        return AuthEventWindowProcessor()

    def test_apply_generates_summary_record(self, processor):
        """Test that apply generates correct summary record."""
        
        # Mock window object
        class MockWindow:
            start = 1609459200000  # 2021-01-01 00:00:00 UTC
            end = 1609459500000    # 2021-01-01 00:05:00 UTC

        window = MockWindow()
        key = "org_123"
        
        aggregation_result = {
            "org_id": "org_123",
            "counts": {
                "status_1": 42,
                "status_2": 8,
                "status_3": 2
            }
        }

        results = list(processor.apply(key, window, [aggregation_result]))

        assert len(results) == 1
        summary = results[0]

        assert summary["org_id"] == "org_123"
        assert summary["window_start_ms"] == 1609459200000
        assert summary["window_end_ms"] == 1609459500000
        assert summary["success_count"] == 42
        assert summary["failure_count"] == 8
        assert summary["unknown_count"] == 2
        assert summary["total_events"] == 52

    def test_apply_calculates_total_events(self, processor):
        """Test that total_events is calculated correctly."""
        
        class MockWindow:
            start = 0
            end = 300000

        window = MockWindow()
        
        aggregation_result = {
            "org_id": "test_org",
            "counts": {
                "status_1": 100,
                "status_2": 50,
                "status_3": 10
            }
        }

        results = list(processor.apply("test_org", window, [aggregation_result]))
        summary = results[0]

        assert summary["total_events"] == 160

    def test_apply_includes_timestamp(self, processor):
        """Test that apply includes current timestamp in output."""
        
        class MockWindow:
            start = 0
            end = 300000

        window = MockWindow()
        aggregation_result = {
            "org_id": "org_456",
            "counts": {"status_1": 10, "status_2": 0, "status_3": 0}
        }

        results = list(processor.apply("org_456", window, [aggregation_result]))
        summary = results[0]

        assert "timestamp" in summary
        # Verify it's a valid ISO format timestamp
        datetime.fromisoformat(summary["timestamp"])


class TestOCSFEventDeserializer:
    """Test cases for OCSFEventDeserializer."""

    @pytest.fixture
    def deserializer(self):
        return OCSFEventDeserializer()

    def test_deserialize_valid_json(self, deserializer):
        """Test deserializing valid JSON message."""
        message = b'{"org_id": "123", "status_id": 1, "time": 1609459200000}'
        
        result = deserializer.deserialize(message)

        assert result["org_id"] == "123"
        assert result["status_id"] == 1
        assert result["time"] == 1609459200000

    def test_deserialize_complex_ocsf_event(self, deserializer):
        """Test deserializing a complex OCSF authentication event."""
        message = b'''
        {
            "class_uid": 3002,
            "status_id": 1,
            "org_id": "org_789",
            "user": {"name": "alice"},
            "src_endpoint": {"ip": "192.168.1.1"},
            "metadata": {"product": "wazuh"}
        }
        '''
        
        result = deserializer.deserialize(message)

        assert result["class_uid"] == 3002
        assert result["status_id"] == 1
        assert result["org_id"] == "org_789"
        assert result["user"]["name"] == "alice"

    def test_deserialize_invalid_json_returns_empty(self, deserializer):
        """Test deserializing invalid JSON returns empty dict."""
        message = b'not valid json {]'
        
        result = deserializer.deserialize(message)

        assert result == {}


class TestAuthEventSerializer:
    """Test cases for AuthEventSerializer."""

    @pytest.fixture
    def serializer(self):
        return AuthEventSerializer()

    def test_serialize_summary_record(self, serializer):
        """Test serializing a summary record."""
        summary = {
            "org_id": "org_123",
            "total_events": 50,
            "success_count": 42,
            "failure_count": 8,
            "unknown_count": 0,
            "timestamp": "2021-01-01T00:05:00"
        }
        
        result = serializer.serialize(summary)

        assert isinstance(result, bytes)
        # Verify it can be decoded back to dict
        import json
        decoded = json.loads(result.decode("utf-8"))
        assert decoded["org_id"] == "org_123"
        assert decoded["total_events"] == 50

    def test_serialize_and_deserialize_roundtrip(self, serializer):
        """Test that serialization is reversible."""
        import json
        
        original = {
            "window_start_ms": 1609459200000,
            "window_end_ms": 1609459500000,
            "org_id": "test_org",
            "total_events": 100
        }
        
        serialized = serializer.serialize(original)
        deserialized = json.loads(serialized.decode("utf-8"))

        assert deserialized == original
