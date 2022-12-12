"""Collection of dataclass-based schema for the database."""

from typing import Any

import pydantic


@pydantic.dataclasses.dataclass
class ScanIDDataclass:
    """A dataclass with a scan id."""

    scan_id: str
    is_deleted: bool


@pydantic.dataclasses.dataclass
class Result(ScanIDDataclass):
    """Encompasses the physics results for a scan."""

    json_result: dict[str, Any]  # actual keys/values are open to requestor


@pydantic.dataclasses.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_id: str
    progress: dict[str, Any]  # actual keys/values are open to requestor
