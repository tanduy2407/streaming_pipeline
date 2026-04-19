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

    status_id: int = 3  # 1=Success, 2=Failure, 3=Unknown
    severity_id: Optional[int] = None

    src_endpoint: Optional[Endpoint] = None
    dst_endpoint: Optional[Endpoint] = None

    user: Optional[User] = None

    metadata: Optional[Metadata] = None

    unmapped: Dict[str, Any] = field(default_factory=dict)