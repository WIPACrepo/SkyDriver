"""Collection of dataclass-based schema for the database."""

import dataclasses as dc
import enum
from typing import Any

import wipac_dev_tools as wdt
from rest_tools.client import RestClient
from typeguard import typechecked

from skydriver import ewms

StrDict = dict[str, Any]


class ScanState(enum.Enum):
    """A non-persisted scan state."""

    SCAN_FINISHED_SUCCESSFULLY = enum.auto()

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
    pickled_k8s_job: bytes | None = None  # **DEPRECATED** replaced SkyScanK8sJob in db
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


PENDING_EWMS_WORKFLOW = "pending ewms"

DEPRECATED_EVENT_I3LIVE_JSON_DICT = "use 'i3_event_id'"
DEPRECATED_EWMS_TASK = "use 'ewms_workflow_id'"


@typechecked
@dc.dataclass
class Manifest(ScanIDDataclass):
    """Encapsulates the manifest of a unique scan entity."""

    timestamp: float
    is_deleted: bool

    # args placed in k8s job obj
    scanner_server_args: str

    # EWMS interface
    ewms_workflow_id: str | None = None  # id points to info in EWMS
    # -> deprecated fields -- see __post_init__ for backward compatibility  logic
    ewms_task: dict | str = DEPRECATED_EWMS_TASK  # **DEPRECATED**
    # ^^^ was used in skydriver 1.x to use local k8s starter/stopper

    priority: int = (
        0  # same as https://htcondor.readthedocs.io/en/latest/users-manual/priorities-and-preemption.html#job-priority
    )

    # open to requestor
    classifiers: dict[str, str | bool | float | int] = dc.field(default_factory=dict)

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
        # Backward compatibility: 'i3_event_id' replaced 'event_i3live_json_dict'
        if (
            not self.i3_event_id
            and self.event_i3live_json_dict == DEPRECATED_EVENT_I3LIVE_JSON_DICT
        ):
            raise ValueError(
                "Manifest must define 'i3_event_id' "
                "(old manifests may define 'event_i3live_json_dict' instead)"
            )

        # Backward compatibility: 'ewms_workflow_id' replaced 'ewms_task'
        if not self.ewms_workflow_id and self.ewms_task == DEPRECATED_EWMS_TASK:
            raise ValueError(
                "Manifest must define 'ewms_workflow_id' "
                "(old manifests may define 'ewms_task' instead)"
            )

        # don't show sensitive data to user
        self.scanner_server_args = obfuscate_cl_args(self.scanner_server_args)

    def __repr__(self) -> str:
        dicto = dc.asdict(self)
        dicto.pop("event_i3live_json_dict")
        rep = f"{self.__class__.__name__}{dicto}"
        return rep


async def get_scan_state(manifest: Manifest, ewms_rc: RestClient) -> str:
    """Determine the state of the scan by parsing attributes and talking with EWMS."""
    if manifest.progress and manifest.progress.processing_stats.finished:
        return ScanState.SCAN_FINISHED_SUCCESSFULLY.name

    def _has_scanner_server_started() -> bool:
        return bool(manifest.progress)  # attr only updated by scanner server requests

    def _has_request_been_sent_to_ewms() -> bool:
        return (
            manifest.ewms_workflow_id == PENDING_EWMS_WORKFLOW  # pending ewms req.
            or (  # backward compatibility...
                manifest.ewms_task != DEPRECATED_EWMS_TASK
                and manifest.ewms_task.get("clusters")
            )
        )

    def _get_nonfinished_state() -> ScanState:
        # has the scanner server started?
        # -> yes
        if _has_scanner_server_started():
            if _has_request_been_sent_to_ewms():
                return ScanState.PENDING__WAITING_ON_CLUSTER_STARTUP
            else:
                if manifest.progress.processing_stats.rate:
                    return ScanState.IN_PROGRESS__PARTIAL_RESULT_GENERATED
                else:
                    return ScanState.IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO
        # -> no
        else:
            if _has_request_been_sent_to_ewms():
                # NOTE: assume that the ewms-request and scanner server startup happen in tandem
                return ScanState.PENDING__WAITING_ON_SCANNER_SERVER_STARTUP
            else:
                return ScanState.PENDING__PRESTARTUP

    # is EWMS still running the scan workers?
    # -> yes
    if dtype := await ewms.get_deactivated_type(ewms_rc, manifest.ewms_workflow_id):
        return f"{dtype.upper()}__{_get_nonfinished_state().name.split('__')[1]}"
    # -> no
    else:
        return _get_nonfinished_state().name
