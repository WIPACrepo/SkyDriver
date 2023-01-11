"""Collection of dataclass-based schema for the database."""

import dataclasses as dc
from typing import Any

from typeguard import typechecked


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class ScanIDDataclass:
    """A dataclass with a scan id."""

    scan_id: str
    is_deleted: bool


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class ProgressMetadata:
    """Metadata about the Progress."""

    event: dict
    scan: dict


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class ProgressProcessingStats:
    """Details about the scan processing."""

    start: dict
    complete: dict
    runtime: dict
    rate: dict
    end: str = ""
    finished: bool = False
    predictions: dict = dc.field(default_factory=dict)  # open to requestor


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Progress:
    # NOTE: do we want to lock-down schema even further?
    metadata: ProgressMetadata
    summary: str
    epilogue: str
    tallies: dict
    processing_stats: ProgressProcessingStats


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Result(ScanIDDataclass):
    """Encompasses the physics results for a scan."""

    scan_result: dict[str, Any]  # actual keys/values are open to requestor
    is_final: bool  # is this result the final result?

    def __repr__(self) -> str:
        dicto = dc.asdict(self)
        dicto.pop("scan_result")
        # shorten b/c this can be a LARGE dict
        dicto["scan_result_HASH"] = hash(str(self.scan_result))
        rep = f"{self.__class__.__name__}{dicto}"
        return rep


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_id: str = ""  # found/created during first few seconds of scanning
    progress: dict[str, Any] = dc.field(default_factory=dict)  # open to requestor
