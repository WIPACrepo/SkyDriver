"""Collection of dataclass-based schema for the database."""

import dataclasses as dc
from typing import Any

from typeguard import typechecked

StrDict = dict[str, Any]


@typechecked
@dc.dataclass
class ScanIDDataclass:
    """A dataclass with a scan id."""

    scan_id: str
    is_deleted: bool


@typechecked
@dc.dataclass
class ScanBacklogEntry(ScanIDDataclass):
    """An entry for the scan backlog used for rate-limiting."""

    timestamp: float
    serialized_k8s_job_obj: str


@typechecked
@dc.dataclass
class ProgressProcessingStats:
    """Details about the scan processing."""

    start: StrDict
    runtime: StrDict
    rate: StrDict = dc.field(default_factory=dict)
    end: str = ""
    finished: bool = False
    predictions: StrDict = dc.field(default_factory=dict)  # open to requestor


@typechecked
@dc.dataclass
class Progress:
    """Houses all the progress for a scan (changed throughout scan)."""

    summary: str
    epilogue: str
    tallies: StrDict
    processing_stats: ProgressProcessingStats
    predictive_scanning_threshold: float
    last_updated: str


@typechecked
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


@typechecked
@dc.dataclass
class EventMetadata:
    """Encapsulates the identity of an event."""

    run_id: int
    event_id: int
    event_type: str
    mjd: float
    is_real_event: bool  # as opposed to simulation


@typechecked
@dc.dataclass
class CondorClutser:
    """Stores information provided by HTCondor."""

    collector: str
    schedd: str
    cluster_id: int
    jobs: int


@typechecked
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    event_i3live_json_dict: StrDict  # TODO: delete after time & replace w/ checksum/hash?
    scanner_server_args: str
    tms_args: list[str]
    env_vars: dict[str, StrDict]

    condor_clusters: list[CondorClutser] = dc.field(default_factory=list)

    # found/created during first few seconds of scanning
    event_metadata: EventMetadata | None = None
    scan_metadata: dict | None = None  # open to requestor

    # updated during scanning, multiple times
    progress: Progress | None = None

    # signifies k8s jobs and condor cluster(s) are done
    complete: bool = False

    # logs  # TODO

    def __repr__(self) -> str:
        dicto = dc.asdict(self)
        # shorten b/c this can be a LARGE dict
        try:
            dicto["event_i3live_json_dict"]["value"]["data"] = hash(
                str(dicto["event_i3live_json_dict"]["value"]["data"])
            )
        except KeyError:
            pass
        dicto["event_i3live_json_dict__hashed_data"] = dicto.pop(
            "event_i3live_json_dict"
        )
        # obfuscate tokens
        # TODO
        rep = f"{self.__class__.__name__}{dicto}"
        return rep
