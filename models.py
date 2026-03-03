from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class InstanceConfig:
    """Configuration for one Elasticsearch/Kibana instance."""

    name: str
    base_url: str


@dataclass
class OperationResult:
    """Generic operation response for UI reporting."""

    instance: str
    target: str
    ok: bool
    status_code: Optional[int] = None
    message: str = ""


@dataclass
class BulkUserEntry:
    """Entry used by bulk user creation flows."""

    username: str
    password: str
    roles: List[str] = field(default_factory=list)
