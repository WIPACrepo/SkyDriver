"""Collection of dataclass-based schema for the database."""

import dataclasses as dc
from typing import Any

from typeguard import typechecked

StrDict = dict[str, Any]


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

    event: StrDict
    scan: StrDict


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class ProgressProcessingStats:
    """Details about the scan processing."""

    start: StrDict
    complete: StrDict
    runtime: StrDict
    rate: StrDict
    end: str = ""
    finished: bool = False
    predictions: StrDict = dc.field(default_factory=dict)  # open to requestor


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Progress:
    # NOTE: do we want to lock-down schema even further?
    metadata: ProgressMetadata
    summary: str
    epilogue: str
    tallies: StrDict
    processing_stats: ProgressProcessingStats


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Result(ScanIDDataclass):
    """Encompasses the physics results for a scan."""

    scan_result: StrDict  # actual keys/values are open to requestor
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
class RunEventIdentity:
    """Encapsulates the identity of an event."""

    run_id: int
    event_id: int
    is_real: bool  # as opposed to simulation


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_i3live_json_dict: StrDict  # TODO: delete after time & replace w/ checksum/hash?
    progress: Progress

    # found/created during first few seconds of scanning
    runevent: RunEventIdentity | None = None
