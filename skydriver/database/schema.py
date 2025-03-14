"""Collection of dataclass-based schema for the database."""

import dataclasses as dc
import enum
from typing import Any, Iterator, Literal

import wipac_dev_tools as wdt
from typeguard import typechecked

from .. import config

StrDict = dict[str, Any]


class ScanState(enum.Enum):
    """A non-persisted scan state."""

    SCAN_FINISHED_SUCCESSFULLY = enum.auto()

    STOPPED__PARTIAL_RESULT_GENERATED = enum.auto()
    STOPPED__WAITING_ON_FIRST_PIXEL_RECO = enum.auto()
    STOPPED__WAITING_ON_CLUSTER_STARTUP = enum.auto()
    STOPPED__WAITING_ON_SCANNER_SERVER_STARTUP = enum.auto()
    STOPPED__PRESTARTUP = enum.auto()

    IN_PROGRESS__PARTIAL_RESULT_GENERATED = enum.auto()
    IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO = enum.auto()
    PENDING__WAITING_ON_CLUSTER_STARTUP = enum.auto()
    PENDING__WAITING_ON_SCANNER_SERVER_STARTUP = enum.auto()
    PENDING__PRESTARTUP = enum.auto()


@typechecked
@dc.dataclass
class ScanIDDataclass:
    """A dataclass with a scan id."""

    scan_id: str


@typechecked
@dc.dataclass
class ScanBacklogEntry(ScanIDDataclass):
    """An entry for the scan backlog used for rate-limiting."""

    timestamp: float
    pickled_k8s_job: bytes
    priority: int = 0
    pending_timestamp: float = 0.0
    next_attempt: int = 0

    def __repr__(self) -> str:
        dicto = dc.asdict(self)
        dicto.pop("pickled_k8s_job")
        # shorten b/c this is a LARGE binary (that is, at least a large string)
        dicto["pickled_k8s_job_HASH"] = hash(str(self.pickled_k8s_job))
        rep = f"{self.__class__.__name__}{dicto}"
        return rep


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
class HTCondorLocation:
    """Stores location metadata for a HTCondor cluster."""

    collector: str
    schedd: str


@typechecked
@dc.dataclass
class KubernetesLocation:
    """Stores location metadata for a Kubernetes cluster."""

    host: str
    namespace: str


@typechecked
@dc.dataclass
class Cluster:
    """Stores information for a worker cluster."""

    orchestrator: Literal["condor", "k8s"]
    location: HTCondorLocation | KubernetesLocation
    n_workers: int

    uuid: str = ""  # "" is a non-started cluster -- universally unique
    cluster_id: str = ""  # "" is a non-started cluster -- quasi-unique to location

    starter_info: StrDict = dc.field(default_factory=dict)

    statuses: dict[str, dict[str, int]] = dc.field(default_factory=dict)
    top_task_errors: dict[str, int] = dc.field(default_factory=dict)

    def __post_init__(self) -> None:
        match self.orchestrator:
            case "condor":
                if not isinstance(self.location, HTCondorLocation):
                    raise TypeError(
                        "condor orchestrator must use condor sub-fields for 'location'"
                    )
            case "k8s":
                if not isinstance(self.location, KubernetesLocation):
                    raise TypeError(
                        "k8s orchestrator must use k8s sub-fields for 'location'"
                    )
            case other:
                raise ValueError(f"Unknown cluster orchestrator: {other}")

    def to_known_cluster(self) -> tuple[str, StrDict]:
        """Map to a config.KNOWN_CLUSTERS entry."""
        return next(
            (k, v)
            for k, v in config.KNOWN_CLUSTERS.items()
            if v["location"] == dc.asdict(self.location)
        )


@typechecked
@dc.dataclass
class EnvVars:
    """Encapsulates env var object originating from K8s objects."""

    scanner_server: list[StrDict]
    tms_starters: list[list[StrDict]]

    def __post_init__(self) -> None:
        #
        # obfuscate tokens & such (sensitive values)
        #
        def obfuscate(env_list: list[StrDict]) -> Iterator[StrDict]:
            for env_entry in env_list:
                if env_entry["value"]:
                    safe_val = wdt.data_safety_tools.obfuscate_value_if_sensitive(
                        env_entry["name"], env_entry["value"]
                    )
                    env_entry["value"] = safe_val
                yield env_entry

        self.scanner_server = list(obfuscate(self.scanner_server))
        self.tms_starters = [list(obfuscate(s)) for s in self.tms_starters]


def obfuscate_cl_args(args: str) -> str:
    # first, check if any sensitive strings (searches using substrings)
    if not wdt.data_safety_tools.is_name_sensitive(args):
        return args
    # now, go one-by-one
    out_args: list[str] = []
    current_option = ""
    for string in args.split():
        if string.startswith("--"):  # ex: --foo -> "... --foo bar baz ..."
            current_option = string
            out_args += string
        elif current_option:  # ex: baz -> "... --foo bar baz ..."
            out_args += wdt.data_safety_tools.obfuscate_value_if_sensitive(
                current_option, string
            )
        else:  # ex: my_module -> "python -m my_module ... --foo bar baz ..."
            out_args += string
    return " ".join(out_args)


@typechecked
@dc.dataclass
class EWMSTaskDirective:
    """Encapsulates the directive of a unique EWMS task entity."""

    tms_args: list[str]  # TODO - move to TMS
    env_vars: EnvVars  # TODO - move to TMS
    clusters: list[Cluster] = dc.field(default_factory=list)

    # signifies k8s workers and condor cluster(s) AKA workforce is done
    complete: bool = False  # TODO - move to TMS

    def __post_init__(self) -> None:
        self.tms_args = [obfuscate_cl_args(a) for a in self.tms_args]
        # NOTE - self.env_vars done in EnvVars


DEPRECATED_EVENT_I3LIVE_JSON_DICT = "use 'i3_event_id'"


@typechecked
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    timestamp: float
    is_deleted: bool

    ewms_task: EWMSTaskDirective

    # args placed in k8s job obj
    scanner_server_args: str

    priority: int = (
        0  # same as https://htcondor.readthedocs.io/en/latest/users-manual/priorities-and-preemption.html#job-priority
    )

    # open to requestor
    classifiers: dict[str, str | bool | float | int | None] = dc.field(default_factory=dict)

    # i3 event -- grabbed by scanner central server
    i3_event_id: str = ""  # id to i3_event coll
    # -> deprecated fields -- see __post_init__ for backward compatibility  logic
    event_i3live_json_dict: StrDict | str = DEPRECATED_EVENT_I3LIVE_JSON_DICT
    event_i3live_json_dict__hash: str | None = None  # **DEPRECATED**

    # found/created during first few seconds of scanning
    event_metadata: EventMetadata | None = None
    scan_metadata: dict | None = None  # open to scanner

    # updated during scanning, multiple times
    progress: Progress | None = None

    last_updated: float = 0.0

    def __post_init__(self) -> None:
        if (
            not self.i3_event_id
            and self.event_i3live_json_dict == DEPRECATED_EVENT_I3LIVE_JSON_DICT
        ):
            raise ValueError(
                "Manifest must define 'i3_event_id' "
                "(old manifests may define 'event_i3live_json_dict' instead)"
            )
        self.scanner_server_args = obfuscate_cl_args(self.scanner_server_args)

    def get_state(self) -> ScanState:
        """Determine the state of the scan by parsing attributes."""
        if (
            self.ewms_task.complete
            and self.progress
            and self.progress.processing_stats.finished
        ):
            return ScanState.SCAN_FINISHED_SUCCESSFULLY

        def get_nonfinished_state() -> ScanState:
            if self.progress:  # from scanner server
                if self.ewms_task.clusters:
                    # NOTE - we only know if the workers have started up once the server has gotten pixels
                    if self.progress.processing_stats.rate:
                        return ScanState.IN_PROGRESS__PARTIAL_RESULT_GENERATED
                    else:
                        return ScanState.IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO
                else:
                    return ScanState.PENDING__WAITING_ON_CLUSTER_STARTUP
            else:
                if self.ewms_task.clusters:
                    return ScanState.PENDING__WAITING_ON_SCANNER_SERVER_STARTUP
                else:
                    return ScanState.PENDING__PRESTARTUP

        if self.ewms_task.complete:
            return ScanState[f"STOPPED__{get_nonfinished_state().name.split('__')[1]}"]
        else:
            return get_nonfinished_state()

    def __repr__(self) -> str:
        dicto = dc.asdict(self)
        dicto.pop("event_i3live_json_dict")
        rep = f"{self.__class__.__name__}{dicto}"
        return rep
