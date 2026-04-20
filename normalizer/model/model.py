from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class Endpoint:
    ip: Optional[str] = None
    hostname: Optional[str] = None


@dataclass
class User:
    name: Optional[str] = None
    uid: Optional[str] = None


@dataclass
class Metadata:
    product: Optional[str]
    version: Optional[str]
    org_id: Optional[str]


@dataclass
class OcsfAuthenticationEvent:
    class_uid: int = 3002
    category_uid: int = 3
    activity_id: int = 1  # Logon

    time: int = 0  # epoch millis

    status_id: int  # 1=Success, 2=Failure (required)
    severity_id: Optional[int] = None

    src_endpoint: Optional[Endpoint] = None
    dst_endpoint: Optional[Endpoint] = None

    user: Optional[User] = None

    metadata: Optional[Metadata] = None

    unmapped: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "class_uid": self.class_uid,
            "category_uid": self.category_uid,
            "activity_id": self.activity_id,
            "time": self.time,
            "status_id": self.status_id,
            "severity_id": self.severity_id,
            "src_endpoint": {
                "ip": self.src_endpoint.ip,
                "hostname": self.src_endpoint.hostname
            } if self.src_endpoint else None,
            "dst_endpoint": {
                "ip": self.dst_endpoint.ip,
                "hostname": self.dst_endpoint.hostname
            } if self.dst_endpoint else None,
            "user": {
                "name": self.user.name,
                "uid": self.user.uid
            } if self.user else None,
            "metadata": {
                "product": self.metadata.product,
                "version": self.metadata.version,
                "org_id": self.metadata.org_id
            } if self.metadata else None,
            "unmapped": self.unmapped
        }