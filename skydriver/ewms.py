"""Tools for interfacing with EMWS."""

import logging

import aiocache  # type: ignore[import-untyped]
import botocore.client  # type: ignore[import-untyped]
import requests
from rest_tools.client import RestClient

from .config import ENV
from .database.schema import PENDING_EWMS_WORKFLOW

LOGGER = logging.Logger(__name__)


async def request_stop_on_ewms(
    ewms_rc: RestClient,
    workflow_id: str,
    abort: bool,
) -> None:
    """Signal that an EWMS workflow is finished, and stop whatever is needed.

    Suppresses any HTTP errors.
    """
    try:
        if abort:
            await ewms_rc.request(
                "POST",
                f"/v0/workflows/{workflow_id}/actions/abort",
            )
        else:
            await ewms_rc.request(
                "POST",
                f"/v0/workflows/{workflow_id}/actions/finished",
            )
    except requests.exceptions.HTTPError as e:
        LOGGER.warning(repr(e))
        if ENV.CI:
            raise e


@aiocache.cached(ttl=1 * 60)  # don't cache too long, but avoid spamming ewms
async def get_deactivated_type(ewms_rc: RestClient, workflow_id: str) -> str | None:
    """Grab the 'deactivated' field for the workflow.

    Example: 'ABORTED', 'FINISHED
    """
    if workflow_id == PENDING_EWMS_WORKFLOW:
        return None

    workflow = await ewms_rc.request(
        "GET",
        f"/v0/workflows/{workflow_id}",
    )
    return workflow["deactivated"]


@aiocache.cached(ttl=1 * 60)  # don't cache too long, but avoid spamming ewms
async def get_taskforce_phases(
    ewms_rc: RestClient,
    workflow_id: str,
) -> list[dict[str, str]]:
    """Get all the states of all the taskforces associated with the workflow."""
    resp = await ewms_rc.request(
        "POST",
        "/v0/query/taskforces",
        {
            "query": {
                "workflow_id": workflow_id,
            }
        },
    )
    return [
        {
            k: tf.get(k)
            for k in [
                "taskforce_uuid",
                "phase",
                "phase_change_log",
                "compound_statuses",
                "top_task_errors",
            ]
        }
        for tf in resp["taskforces"]
    ]


def make_s3_object_key(scan_id: str) -> str:
    """Construct the object key from the scan_id (deterministic)."""
    return f"{scan_id}-s3-object"
