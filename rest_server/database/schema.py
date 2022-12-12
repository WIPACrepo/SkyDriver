"""Collection of dataclass-based schema for the database."""

import dataclasses as dc
from typing import Any

from typeguard import typechecked


@typechecked
@dc.dataclass
class ScanIDDataclass:
    """A dataclass with a scan id."""

    scan_id: str
    is_deleted: bool


@typechecked
@dc.dataclass
class Result(ScanIDDataclass):
    """Encompasses the physics results for a scan."""

    json_result: dict[str, Any]  # actual keys/values are open to requestor


@typechecked
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_id: str
    progress: dict[str, Any] = dc.field(default_factory=dict)  # open to requestor
