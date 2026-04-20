"""Unit tests for the normalization pipeline."""
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "main"))
sys.path.insert(0, str(Path(__file__).parent.parent / "model"))
from normalized import (
    WazuhNormalizer,
    CloudTrailNormalizer,
    NormalizationError
)
from registry import NormalizerRegistry, NormalizationError as RegistryNormalizationError
from model import OcsfAuthenticationEvent


class TestSuccessfulNormalization:
    """Test 1: Successful normalization for each source type."""

    def test_wazuh_normalization_specific_fields(self):
        """Assert specific field values for Wazuh normalization."""
        normalizer = WazuhNormalizer()
        event = {
            "timestamp": "2026-04-10T14:32:01.000Z",
            "agent": {"id": "001", "name": "web-srv-01"},
            "rule": {"id": "5715", "level": 5, "description": "sshd: authentication success"},
            "data": {
                "srcip": "10.0.1.42",
                "dstuser": "root",
                "result": "success"
            }
        }
        result = normalizer.normalize(event)

        assert result.status_id == 1  # Success
        assert result.severity_id == 5
        assert result.user.name == "root"
        assert result.src_endpoint.ip == "10.0.1.42"
        assert result.dst_endpoint.hostname == "web-srv-01"
        assert result.metadata.product == "wazuh"

    def test_cloudtrail_normalization_specific_fields(self):
        """Assert specific field values for CloudTrail normalization."""
        normalizer = CloudTrailNormalizer()
        event = {
            "eventTime": "2026-04-10T14:35:22Z",
            "sourceIPAddress": "203.0.113.55",
            "userIdentity": {"userName": "alice"},
            "responseElements": {"ConsoleLogin": "Success"},
            "awsRegion": "us-east-1",
            "eventSource": "signin.amazonaws.com"
        }
        result = normalizer.normalize(event)

        assert result.status_id == 1  # Success
        assert result.user.name == "alice"
        assert result.src_endpoint.ip == "203.0.113.55"
        assert result.metadata.product == "cloudtrail"


class TestMalformedInput:
    """Test 2: Malformed input that triggers error handling."""

    def test_wazuh_malformed_missing_fields(self):
        """Test Wazuh normalization with missing required fields."""
        normalizer = WazuhNormalizer()
        event = {
            # Missing timestamp field - this should trigger an error
            "agent": {"name": "test"},
            "rule": {"level": 5},
            "data": {"result": "success"}
        }
        with pytest.raises(NormalizationError):
            normalizer.normalize(event)

    def test_wazuh_malformed_invalid_timestamp(self):
        """Test Wazuh normalization with invalid timestamp format."""
        normalizer = WazuhNormalizer()
        event = {
            "timestamp": "invalid-date-format",
            "agent": {"name": "test"},
            "rule": {"level": 5},
            "data": {"result": "success"}
        }
        with pytest.raises(NormalizationError):
            normalizer.normalize(event)

    def test_cloudtrail_malformed_missing_fields(self):
        """Test CloudTrail normalization with missing required fields."""
        normalizer = CloudTrailNormalizer()
        event = {
            # Missing eventTime field - this should trigger an error
            "sourceIPAddress": "10.0.0.1",
            "userIdentity": {"userName": "user"},
            "responseElements": {"ConsoleLogin": "Success"}
        }
        with pytest.raises(NormalizationError):
            normalizer.normalize(event)


class TestUnmappedFields:
    """Test 3: Unexpected/new fields landing in unmapped map."""

    def test_wazuh_unexpected_fields_in_unmapped(self):
        """Test Wazuh event with unexpected fields captured in unmapped."""
        normalizer = WazuhNormalizer()
        event = {
            "timestamp": "2026-04-10T14:32:01.000Z",
            "agent": {"name": "host01"},
            "rule": {"level": 3},
            "data": {"result": "success"},
            "custom_field": "custom_value",
            "new_field": {"nested": "data"},
            "location": "/var/log/auth.log"
        }
        result = normalizer.normalize(event)

        assert "custom_field" in result.unmapped
        assert result.unmapped["custom_field"] == "custom_value"
        assert "new_field" in result.unmapped
        assert "location" in result.unmapped

    def test_cloudtrail_unexpected_fields_in_unmapped(self):
        """Test CloudTrail event with unexpected fields captured in unmapped."""
        normalizer = CloudTrailNormalizer()
        event = {
            "eventTime": "2026-04-10T14:35:22Z",
            "sourceIPAddress": "203.0.113.55",
            "userIdentity": {"userName": "alice"},
            "responseElements": {"ConsoleLogin": "Success"},
            "awsRegion": "us-east-1",
            "eventSource": "signin.amazonaws.com",
            "customField": "customValue",
            "additionalData": {"key": "value"}
        }
        result = normalizer.normalize(event)

        assert "customField" in result.unmapped
        assert "additionalData" in result.unmapped


class TestRegistryCorrectNormalizer:
    """Test 4: Registry returns correct normalizer and errors for unknown types."""

    def test_registry_returns_wazuh_normalizer(self):
        """Test registry returns correct Wazuh normalizer instance."""
        normalizer = NormalizerRegistry.get("wazuh")
        assert isinstance(normalizer, WazuhNormalizer)

    def test_registry_returns_cloudtrail_normalizer(self):
        """Test registry returns correct CloudTrail normalizer instance."""
        normalizer = NormalizerRegistry.get("cloudtrail")
        assert isinstance(normalizer, CloudTrailNormalizer)

    def test_registry_case_insensitive_lookup(self):
        """Test registry lookup is case-insensitive."""
        normalizer_lower = NormalizerRegistry.get("wazuh")
        normalizer_upper = NormalizerRegistry.get("WAZUH")
        assert type(normalizer_lower) == type(normalizer_upper)

    def test_registry_error_for_unknown_type(self):
        """Test registry raises error for unknown source type."""
        with pytest.raises((NormalizationError, RegistryNormalizationError), match="No normalizer found"):
            NormalizerRegistry.get("unknown_source_type")
