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
class ProgressProcessingStats:
    """Details about the scan processing."""

    start: StrDict
    runtime: StrDict
    rate: StrDict = dc.field(default_factory=dict)
    end: str = ""
    finished: bool = False
    predictions: StrDict = dc.field(default_factory=dict)  # open to requestor


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Progress:

    summary: str
    epilogue: str
    tallies: StrDict
    processing_stats: ProgressProcessingStats


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Result(ScanIDDataclass):
    """Encompasses the physics results for a scan."""

    skyscan_result: StrDict  # actual keys/values are open to requestor
    is_final: bool  # is this result the final result?

    def __repr__(self) -> str:
        dicto = dc.asdict(self)
        dicto.pop("skyscan_result")
        # shorten b/c this can be a LARGE dict
        dicto["skyscan_result_HASH"] = hash(str(self.skyscan_result))
        rep = f"{self.__class__.__name__}{dicto}"
        return rep


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class EventMetadata:
    """Encapsulates the identity of an event."""

    run_id: int
    event_id: int
    event_type: str
    mjd: float
    is_real_event: bool  # as opposed to simulation


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class CondorClutser:
    """Stores information provided by HTCondor."""

    collector: str
    schedd: str
    cluster_id: int
    jobs: int
    submit_dict: StrDict


@typechecked(always=True)  # always b/c we want full data validation
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_i3live_json_dict: StrDict  # TODO: delete after time & replace w/ checksum/hash?
    server_args: str
    clientmanager_args: str
    env_vars: dict[str, StrDict]

    condor_clusters: list[CondorClutser] = dc.field(default_factory=list)

    # found/created during first few seconds of scanning
    event_metadata: EventMetadata | None = None
    scan_metadata: dict | None = None  # open to requestor

    # updated during scanning, multiple times
    progress: Progress | None = None

    # logs  # TODO
