"""Tools for interfacing with EMWS."""

import json
import logging

import aiocache  # type: ignore[import-untyped]
import botocore.client  # type: ignore[import-untyped]
import requests
from rest_tools.client import RestClient

from . import database, images, s3
from .config import ENV, QUEUE_ALIAS_FROMCLIENT, QUEUE_ALIAS_TOCLIENT
from .database.schema import PENDING_EWMS_WORKFLOW

LOGGER = logging.Logger(__name__)


async def request_workflow_on_ewms(
    ewms_rc: RestClient,
    s3_client: botocore.client.BaseClient,
    manifest: database.schema.Manifest,
    scan_request_obj: dict,
) -> str:
    """Request a workflow in EWMS."""
    if manifest.ewms_workflow_id != database.schema.PENDING_EWMS_WORKFLOW:
        if manifest.ewms_workflow_id:
            raise TypeError("Scan has already been sent to EWMS")
        else:  # None
            raise TypeError("Scan is not designated for EWMS")

    s3_url_get = s3.generate_s3_get_url(s3_client, manifest.scan_id)
    image = images.get_skyscan_docker_image(scan_request_obj["docker_tag"])

    body = {
        "public_queue_aliases": [QUEUE_ALIAS_TOCLIENT, QUEUE_ALIAS_FROMCLIENT],
        "tasks": [
            {
                "cluster_locations": [
                    cname for cname, _ in scan_request_obj["request_clusters"]
                ],
                "input_queue_aliases": [QUEUE_ALIAS_TOCLIENT],
                "output_queue_aliases": [QUEUE_ALIAS_FROMCLIENT],
                "task_image": image,
                "task_args": (
                    "python -m skymap_scanner.client "
                    "--infile {{INFILE}} --outfile {{OUTFILE}} "
                    "--client-startup-json {{DATA_HUB}}/startup.json"
                ),
                "init_image": image,  # piggyback this image since it's already present
                "init_args": (
                    "bash -c "
                    '"'  # quote for bash -c "..."
                    "curl --fail-with-body --max-time 60 -o {{DATA_HUB}}/startup.json "
                    f"'{s3_url_get}'"  # single-quote the url
                    '"'  # unquote for bash -c "..."
                ),
                "n_workers": scan_request_obj["request_clusters"][0][1],
                # TODO: ^^^ pass on varying # of workers per cluster
                "pilot_config": {
                    "tag": "latest",
                    "environment": {
                        k: v
                        for k, v in {
                            "EWMS_PILOT_INIT_TIMEOUT": 61,  # 1 sec more than 'curl' timeout
                            "EWMS_PILOT_TASK_TIMEOUT": scan_request_obj[
                                "max_pixel_reco_time"
                            ],
                            "EWMS_PILOT_TIMEOUT_QUEUE_WAIT_FOR_FIRST_MESSAGE": scan_request_obj[
                                "skyscan_mq_client_timeout_wait_for_first_message"
                            ],
                            "EWMS_PILOT_TIMEOUT_QUEUE_INCOMING": ENV.SKYSCAN_MQ_TIMEOUT_TO_CLIENTS,
                            "EWMS_PILOT_CONTAINER_DEBUG": "True",  # toggle?
                            "EWMS_PILOT_INFILE_EXT": ".json",
                            "EWMS_PILOT_OUTFILE_EXT": ".json",
                        }.items()
                        if v  # filter out any falsy values
                    },
                    "input_files": [],
                },
                "worker_config": {
                    "do_transfer_worker_stdouterr": True,  # toggle?
                    "max_worker_runtime": scan_request_obj["max_worker_runtime"],
                    "n_cores": 1,
                    "priority": scan_request_obj["priority"],
                    "worker_disk": scan_request_obj["worker_disk_bytes"],
                    "worker_memory": scan_request_obj["worker_memory_bytes"],
                    "condor_requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx && has_avx2",
                },
            }
        ],
    }

    try:
        resp = await ewms_rc.request("POST", "/v0/workflows", body)
    except requests.exceptions.HTTPError:
        LOGGER.error(f"request to ewms failed with:")
        LOGGER.error(json.dumps(body, indent=4))
        raise
    else:
        return resp["workflow"]["workflow_id"]


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
        {"workflow_id": workflow_id},
    )
    return [
        {"taskforce": tf["taskforce_uuid"], "phase": tf["phase"]}
        for tf in resp["taskforces"]
    ]
