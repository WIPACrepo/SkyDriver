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
    workflow_id: str | None,
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


def _increment_counts(target: defaultdict[str, int], source: dict[str, int]):
    """Increment the counts in `target` by the corresponding values in `source`.

    This function updates `target` (a `defaultdict(int)`) by adding values from `source`.
    If a key in `source` is missing in `target`, it is implicitly initialized to 0 before addition.

    Example:
        target = defaultdict(int, {"Tasking": 24})
        source = {"Tasking": 20, "Processing": 7}
        _increment_counts(target, source)
        # target becomes {"Tasking": 44, "Processing": 7}
    """
    for inner_key, value in source.items():
        target[inner_key] += value


async def get_workforce_statuses(
    ewms_rc: RestClient,
    workflow_id: str | None,
) -> dict[str, dict[str, dict[str, int]] | int]:
    """Aggregate the compound statuses of all taskforces in a workflow.

    This function retrieves workforce information, merges taskforce statuses,
    and computes the number of currently running workers, excluding 'FatalError'.

    Example:
        Input from ewms:
        >>> {'IDLE': {'null': 1}, 'RUNNING': {'Tasking': 24}}
        >>> {'IDLE': {'foo': 99}, 'RUNNING': {'Tasking': 20}}
        >>> {'RUNNING': {'Processing': 7}, 'REMOVED': {'Error': 1}}

        Aggregated output:
        >>> {'IDLE': {'null': 1, 'foo': 99}, 'RUNNING': {'Tasking': 44, 'Processing': 7}, 'REMOVED': {'Error': 1}}
    """
    tf_state_dicts = await get_taskforce_infos(ewms_rc, workflow_id)

    # Merge & sum the compound statuses
    merged_statuses: defaultdict[str, defaultdict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for state in tf_state_dicts:
        if not (d := state.get("compound_statuses")):
            continue
        for outer_key, inner_dict in d.items():
            _increment_counts(merged_statuses[outer_key], inner_dict)

    # Compute `n_running`, excluding 'FatalError'
    n_running = sum(
        count
        for substatus, count in merged_statuses.get("RUNNING", {}).items()
        if substatus != "FatalError"
    )

    return {
        "statuses": {k: dict(v) for k, v in merged_statuses.items()},  # to dicts
        "n_running": n_running,
        # NOTE: It's tempting to sum other statuses' counts, but not all
        # statuses are mutually exclusive—some jobs may be double-counted.
    }


def make_s3_object_key(scan_id: str) -> str:
    """Construct the object key from the scan_id (deterministic)."""
    return f"{scan_id}-s3-object"
