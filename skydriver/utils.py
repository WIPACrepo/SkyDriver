"""Utility functions that don't fit anywhere else."""

import enum

from rest_tools.client import RestClient

from . import ewms
from .database.schema import DEPRECATED_EWMS_TASK, Manifest, PENDING_EWMS_WORKFLOW


class _ScanState(enum.Enum):
    """A non-persisted scan state."""

    SCAN_FINISHED_SUCCESSFULLY = enum.auto()

    IN_PROGRESS__PARTIAL_RESULT_GENERATED = enum.auto()
    IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO = enum.auto()
    PENDING__WAITING_ON_SCANNER_SERVER_STARTUP = enum.auto()
    PENDING__IN_BACKLOG = enum.auto()


async def get_scan_state(manifest: Manifest, ewms_rc: RestClient) -> str:
    """Determine the state of the scan by parsing attributes and talking with EWMS."""
    if manifest.progress and manifest.progress.processing_stats.finished:
        return _ScanState.SCAN_FINISHED_SUCCESSFULLY.name

    def _has_request_been_sent_to_ewms() -> bool:
        return bool(
            (  # has a real workflow id
                manifest.ewms_workflow_id
                and manifest.ewms_workflow_id != PENDING_EWMS_WORKFLOW
            )
            or (  # backward compatibility...
                manifest.ewms_task != DEPRECATED_EWMS_TASK
                and isinstance(manifest.ewms_task, dict)
                and manifest.ewms_task.get("clusters")
            )
        )

    def get_nonfinished_state() -> _ScanState:
        """Get the ScanState of the scan, only by parsing attributes."""
        # has scan cleared the backlog? (aka, has been submitted EWMS?)
        if _has_request_been_sent_to_ewms():
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
        # no -> still in backlog
        else:
            return _ScanState.PENDING__IN_BACKLOG

    # is EWMS still running the scan workers?
    # -> yes
    if manifest.ewms_workflow_id and (
        dtype := await ewms.get_deactivated_type(ewms_rc, manifest.ewms_workflow_id)
    ):
        return f"{dtype.upper()}__{get_nonfinished_state().name.split('__')[1]}"
    # -> BACKWARD COMPATIBILITY: is this an old/pre-ewms scan?
    elif isinstance(manifest.ewms_task, dict) and manifest.ewms_task.get("complete"):
        return f"STOPPED__{get_nonfinished_state().name.split('__')[1]}"
    # -> no, this is a non-finished scan
    else:
        return get_nonfinished_state().name
