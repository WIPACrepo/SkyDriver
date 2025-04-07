"""Utility functions that don't fit anywhere else."""

import enum
import time
import uuid

from rest_tools.client import RestClient
from tornado import web

from . import database, ewms
from .database.schema import (
    DEPRECATED_EWMS_TASK,
    Manifest,
    NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
)


def make_scan_id() -> str:
    """Make a new scan id, chronological when sorted."""

    # get time component as hex
    now_millis = int(time.time() * 1000)  # ex: 1739560009352
    now_millis_hex = str(hex(now_millis)).removeprefix("0x")  # ex: 19505db3e88

    # middle component
    middle = "x"

    # get uuid component (but only substring at end) -- ex: 4348a28a8554441b96bc
    uuid_short_hex = uuid.uuid4().hex[len(now_millis_hex) + len(middle) :]

    # assemble -- # ex: 19505db3e88x4348a28a8554441b96bc
    return f"{now_millis_hex}{middle}{uuid_short_hex}"


class _ScanState(enum.Enum):
    """A non-persisted scan state."""

    SCAN_HAS_FINAL_RESULT = enum.auto()
    # ^^^ indicates the scanner sent finished results. in reality, the scanner or ewms
    # could've crashed immediately after BUT the user only cares about the RESULTS--so,
    # this would still be considered a SUCCESS in *this* context

    IN_PROGRESS__PARTIAL_RESULT_GENERATED = enum.auto()
    IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO = enum.auto()

    PENDING__WAITING_ON_SCANNER_SERVER_STARTUP = enum.auto()
    PENDING__PRESTARTUP = enum.auto()


def does_scan_state_indicate_final_result_received(state: str) -> bool:
    """Has the scan ended with a final result?"""
    return state == _ScanState.SCAN_HAS_FINAL_RESULT.name


def _has_cleared_backlog(manifest: Manifest) -> bool:
    return bool(
        (  # has a real workflow id
            manifest.ewms_workflow_id
            and manifest.ewms_workflow_id != NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS
        )
        or (  # backward compatibility...
            manifest.ewms_task != DEPRECATED_EWMS_TASK
            and isinstance(manifest.ewms_task, dict)
            and manifest.ewms_task.get("clusters")
        )
    )


def _get_nonfinished_state(manifest: Manifest) -> _ScanState:
    """Get the ScanState of the scan, only by parsing attributes."""
    # has scan cleared the backlog? (aka, has been *submitted* EWMS?)
    if _has_cleared_backlog(manifest):
        # has the scanner server started?
        if manifest.progress:
            # how far along is the scanner server?
            # seen some pixels -> aka clients have processed pixels
            if manifest.progress.processing_stats.rate:
                return _ScanState.IN_PROGRESS__PARTIAL_RESULT_GENERATED
            # 0% -> aka clients haven't finished any pixels (yet)
            else:
                return _ScanState.IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO
        # no -> hasn't started yet
        else:
            return _ScanState.PENDING__WAITING_ON_SCANNER_SERVER_STARTUP
    # no -> still in backlog (or aborted while in backlog)
    else:
        return _ScanState.PENDING__PRESTARTUP


async def get_scan_state(
    manifest: Manifest,
    ewms_rc: RestClient,
    results: database.interface.ResultClient,
) -> str:
    """Determine the state of the scan by parsing attributes and talking with EWMS.

    Returns the state as a human-readable string
    """
    try:
        if (await results.get(manifest.scan_id)).is_final:
            # NOTE: see note on 'SCAN_HAS_FINAL_RESULT' above
            return _ScanState.SCAN_HAS_FINAL_RESULT.name
    except web.HTTPError as e:
        # get() raises 404 when no result found
        if e.status_code != 404:
            raise

    state = _get_nonfinished_state(manifest).name  # start here, augment if needed

    # AUGMENT STATUS...
    if (  # Backward Compatibility: is this an old/pre-ewms scan?
        not manifest.ewms_workflow_id
        and isinstance(manifest.ewms_task, dict)
        and manifest.ewms_task.get("complete")
    ):
        # we didn't have info on what kind of stop
        return f"STOPPED__{state.split('__')[1]}"
    # has EWMS ceased running the scan workers?
    elif dtype := await ewms.get_deactivated_type(ewms_rc, manifest.ewms_workflow_id):
        # -> yes, the ewms workflow has been deactivated
        return f"{dtype.upper()}__{state.split('__')[1]}"
    else:
        # -> no, this is a non-finished scan
        return state
