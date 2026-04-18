from datetime import datetime, timezone
import json
from abc import ABC, abstractmethod
from model import OcsfAuthenticationEvent, Endpoint, User, Metadata


class NormalizationError(Exception):
    pass


class Normalizer(ABC):

    @abstractmethod
    def normalize(self, raw_json: str) -> OcsfAuthenticationEvent:
        pass

class WazuhNormalizer(Normalizer):

    def normalize(self, raw_json: str) -> OcsfAuthenticationEvent:
        try:
            data = json.loads(raw_json)
        except Exception as e:
            raise NormalizationError("Invalid JSON") from e

        try:
            unmapped = {}

            # --- TIME ---
            # Wazuh: "timestamp": ISO8601 → convert to epoch millis
            time = int(
                datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
                .timestamp() * 1000
            )

            # --- STATUS ---
            # Mapping logic:
            # - data.result == "success" → 1
            # - data.result == "failure" OR description contains "failed" → 2
            result = data.get("data", {}).get("result", "").lower()
            description = data.get("rule", {}).get("description", "").lower()

            if result == "success":
                status_id = 1
            elif result == "failure":
                status_id = 2
            else:
                status_id = 3

            # --- USER ---
            # OCSF decision:
            # - dstuser = authenticated account → user.name
            user_name = data.get("data", {}).get("dstuser")

            # --- SRC ENDPOINT ---
            src_ip = data.get("data", {}).get("srcip")

            # --- DST ENDPOINT ---
            # Using agent.name as destination host
            hostname = data.get("agent", {}).get("name")

            # --- SEVERITY ---
            severity = data.get("rule", {}).get("level")

            # --- UNMAPPED ---
            for key in data:
                if key not in {"timestamp", "data", "agent", "rule"}:
                    unmapped[key] = data[key]

            return OcsfAuthenticationEvent(
                time=time,
                status_id=status_id,
                severity_id=severity,
                src_endpoint=Endpoint(ip=src_ip),
                dst_endpoint=Endpoint(hostname=hostname),
                user=User(name=user_name),
                metadata=Metadata(
                    product="wazuh",
                    version=None,
                    org_id=None
                ),
                unmapped=unmapped
            )

        except Exception as e:
            raise NormalizationError("Failed to normalize Wazuh event") from e
        
class CloudTrailNormalizer(Normalizer):

    def normalize(self, raw_json: str) -> OcsfAuthenticationEvent:
        try:
            data = json.loads(raw_json)
        except Exception as e:
            raise NormalizationError("Invalid JSON") from e

        try:
            unmapped = {}

            # --- TIME ---
            # CloudTrail: eventTime
            time = int(
                datetime.fromisoformat(data["eventTime"].replace("Z", "+00:00"))
                .timestamp() * 1000
            )

            # --- STATUS ---
            # Mapping:
            # responseElements.ConsoleLogin = Success / Failure
            login_status = (
                data.get("responseElements", {})
                .get("ConsoleLogin", "")
                .lower()
            )

            if login_status == "success":
                status_id = 1
            elif login_status == "failure":
                status_id = 2
            else:
                status_id = 3

            # --- USER ---
            user_name = data.get("userIdentity", {}).get("userName")

            # --- SRC ENDPOINT ---
            src_ip = data.get("sourceIPAddress")

            # --- UNMAPPED ---
            for key in data:
                if key not in {
                    "eventTime",
                    "responseElements",
                    "userIdentity",
                    "sourceIPAddress"
                }:
                    unmapped[key] = data[key]

            return OcsfAuthenticationEvent(
                time=time,
                status_id=status_id,
                src_endpoint=Endpoint(ip=src_ip),
                user=User(name=user_name),
                metadata=Metadata(
                    product="cloudtrail",
                    version=None,
                    org_id=None
                ),
                unmapped=unmapped
            )

        except Exception as e:
            raise NormalizationError("Failed to normalize CloudTrail event") from e