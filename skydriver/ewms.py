"""Tools for interfacing with EMWS."""

import logging
from collections import defaultdict

import aiocache  # type: ignore[import-untyped]
import requests
from rest_tools.client import RestClient

from .config import ENV, sdict
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
            LOGGER.info(f"sending 'abort' signal to ewms for {workflow_id=}...")
            await ewms_rc.request(
                "POST",
                f"/v0/workflows/{workflow_id}/actions/abort",
            )
        else:
            LOGGER.info(f"sending 'finished' signal to ewms for {workflow_id=}...")
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
async def get_taskforce_infos(
    ewms_rc: RestClient,
    workflow_id: str,
) -> list[sdict]:
    """Get all info of all the taskforces associated with the workflow."""
    if workflow_id == PENDING_EWMS_WORKFLOW or (not workflow_id):
        return []

    resp = await ewms_rc.request(
        "POST",
        "/v0/query/taskforces",
        {
            "query": {
                "workflow_id": workflow_id,
            }
        },
    )
    return resp["taskforces"]


async def get_workforce_statuses(
    ewms_rc: RestClient,
    workflow_id: str,
) -> dict[str, dict[str, dict[str, int]] | int]:
    """Get the compound statuses for the entire workflow's workforce (aka its taskforces),
    along with the number of currently running workers.

    Example:
        from ewms:
        >>> {'IDLE': {'null': 1}, 'RUNNING': {'Tasking': 24}}
        >>> {'IDLE': {'foo': 99}, 'RUNNING': {'Tasking': 20}}
        >>> {'RUNNING': {'Processing': 7}, 'REMOVED': {'Error': 1}}
        out:
        >>> {'IDLE': {'null': 1, 'foo': 99}, 'RUNNING': {'Tasking': 44, 'Processing': 7}, 'REMOVED': {'Error': 1}}
    """
    tf_state_dicts = await get_taskforce_infos(ewms_rc, workflow_id)

    # merge & sum the compound statuses
    merged = defaultdict(lambda: defaultdict(int))
    for state in tf_state_dicts:
        d = state["compound_statuses"]
        for outer_key, inner_dict in d.items():
            for inner_key, value in inner_dict.items():
                merged[outer_key][inner_key] += value

    return {
        "statuses": {k: dict(v) for k, v in merged.items()},  # convert to dict
        "n_running": sum(merged.get("RUNNING", {}).values()),
        # NOTE: it's tempting to sum other statuses' counts, but not all
        # statuses are mutually exclusive -- iow, ewms may double count for some jobs
    }


def make_s3_object_key(scan_id: str) -> str:
    """Construct the object key from the scan_id (deterministic)."""
    return f"{scan_id}-s3-object"
