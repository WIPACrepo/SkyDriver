"""Run the EWMS Init Container logic."""

import argparse
import asyncio
import dataclasses as dc
import json
import logging
import time
from pathlib import Path

import boto3  # type: ignore[import-untyped]
import requests
from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import from_environment_as_dataclass, logging_tools

LOGGER = logging.getLogger(__package__)


# WARNING: these values must remain constant, they are cross-referenced in the db
QUEUE_ALIAS_TOCLIENT = "to-client-queue"  # ''
QUEUE_ALIAS_FROMCLIENT = "from-client-queue"  # ''

CURL_TIMEOUT = 60


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    SKYSCAN_SKYDRIVER_ADDRESS: str
    SKYSCAN_SKYDRIVER_AUTH: str

    EWMS_ADDRESS: str
    EWMS_TOKEN_URL: str
    EWMS_CLIENT_ID: str
    EWMS_CLIENT_SECRET: str

    EWMS_TASK_IMAGE: str
    EWMS_CLUSTERS: list[str]  # auto-parsed from space-delimited string
    EWMS_N_WORKERS: int

    EWMS_PILOT_TASK_TIMEOUT: int
    EWMS_PILOT_TIMEOUT_QUEUE_INCOMING: int

    EWMS_WORKER_MAX_WORKER_RUNTIME: int
    EWMS_WORKER_PRIORITY: int
    EWMS_WORKER_DISK_BYTES: int
    EWMS_WORKER_MEMORY_BYTES: int

    S3_URL: str
    S3_ACCESS_KEY_ID: str
    S3_SECRET_KEY: str
    S3_BUCKET: str
    S3_OBJECT_KEY: str
    S3_EXPIRES_IN: int


ENV = from_environment_as_dataclass(EnvConfig)


def generate_presigned_s3_get_url(scan_id: str) -> str:
    """Generate a pre-signed S3 url for retrieving shared files."""
    LOGGER.info("connecting to s3...")
    s3_client = boto3.client(
        "s3",
        "us-east-1",
        endpoint_url=ENV.S3_URL,
        aws_access_key_id=ENV.S3_ACCESS_KEY_ID,
        aws_secret_access_key=ENV.S3_SECRET_KEY,
    )

    params = {
        "Bucket": ENV.S3_BUCKET,
        "Key": ENV.S3_OBJECT_KEY,
    }
    LOGGER.info(f"generating presigned s3-url for scan {scan_id} ({params})...")
    get_url = s3_client.generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=ENV.S3_EXPIRES_IN,  # seconds
    )
    LOGGER.info(get_url)
    return get_url


async def request_workflow_on_ewms(ewms_rc: RestClient, s3_url_get: str) -> str:
    """Request a workflow in EWMS."""
    body = {
        "public_queue_aliases": [QUEUE_ALIAS_TOCLIENT, QUEUE_ALIAS_FROMCLIENT],
        "tasks": [
            {
                "cluster_locations": ENV.EWMS_CLUSTERS,
                "input_queue_aliases": [QUEUE_ALIAS_TOCLIENT],
                "output_queue_aliases": [QUEUE_ALIAS_FROMCLIENT],
                "task_image": ENV.EWMS_TASK_IMAGE,
                "task_args": (
                    "python -m skymap_scanner.client "
                    "--infile {{INFILE}} --outfile {{OUTFILE}} "
                    "--client-startup-json {{DATA_HUB}}/startup.json"
                ),
                "init_image": ENV.EWMS_TASK_IMAGE,  # piggyback this image since it's already present
                "init_args": (  # to get the s3 object/file
                    "bash -c "
                    '"'  # quote for bash -c "..."
                    "curl --fail-with-body "
                    f"--max-time {CURL_TIMEOUT} "
                    "-o {{DATA_HUB}}/startup.json "
                    f"'{s3_url_get}'"  # single-quote the url
                    '"'  # unquote for bash -c "..."
                ),
                "n_workers": ENV.EWMS_N_WORKERS,
                "pilot_config": {
                    "tag": "latest",
                    "environment": {
                        k: v
                        for k, v in {
                            "EWMS_PILOT_INIT_TIMEOUT": CURL_TIMEOUT + 1,
                            "EWMS_PILOT_TASK_TIMEOUT": ENV.EWMS_PILOT_TASK_TIMEOUT,
                            "EWMS_PILOT_TIMEOUT_QUEUE_INCOMING": ENV.EWMS_PILOT_TIMEOUT_QUEUE_INCOMING,
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
                    "max_worker_runtime": ENV.EWMS_WORKER_MAX_WORKER_RUNTIME,
                    "n_cores": 1,
                    "priority": ENV.EWMS_WORKER_PRIORITY,
                    "worker_disk": ENV.EWMS_WORKER_DISK_BYTES,
                    "worker_memory": ENV.EWMS_WORKER_MEMORY_BYTES,
                    "condor_requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx && has_avx2",
                },
            }
        ],
    }

    try:
        LOGGER.info("requesting to ewms...")
        resp = await ewms_rc.request("POST", "/v0/workflows", body)
    except requests.exceptions.HTTPError:
        LOGGER.error("request to ewms failed using:")
        LOGGER.error(json.dumps(body, indent=4))
        raise
    else:
        return resp["workflow"]["workflow_id"]


async def get_ewms_attrs(
    ewms_rc: RestClient,
    workflow_id: str,
) -> dict[str, dict[str, str]]:
    """Retrieve the EWMS attributes for the workflow."""
    LOGGER.info(f"getting EWMS attributes for workflow {workflow_id}...")

    # loop until mqprofiles is not empty and all "is_activated" fields are true
    while True:
        LOGGER.info("requesting EWMS mqprofiles...")
        resp = await ewms_rc.request(
            "GET",
            f"/v0/mqs/workflows/{workflow_id}/mq-profiles/public",
        )
        LOGGER.info(json.dumps(resp, indent=4))
        mqprofiles = resp["mqprofiles"]
        if mqprofiles and all(m["is_activated"] for m in mqprofiles):
            break
        else:
            LOGGER.info("mqprofiles are not all activated, retrying soon...")
            time.sleep(10)

    LOGGER.info(f"mqprofiles: {mqprofiles}")

    # convert mqprofiles to dicts based on the queue aliases
    toclient = next(p for p in mqprofiles if p["alias"] == QUEUE_ALIAS_TOCLIENT)
    fromclient = next(p for p in mqprofiles if p["alias"] == QUEUE_ALIAS_FROMCLIENT)

    return {  # NOTE: these fields are accessed by name in the skymap scanner
        "toclient": {
            "name": toclient["mqid"],
            "auth_token": toclient["auth_token"],
            "broker_type": toclient["broker_type"],
            "broker_address": toclient["broker_address"],
        },
        "fromclient": {
            "name": fromclient["mqid"],
            "auth_token": fromclient["auth_token"],
            "broker_type": fromclient["broker_type"],
            "broker_address": fromclient["broker_address"],
        },
    }


def _assure_json(val: str) -> Path:
    fpath = Path(val)
    if fpath.suffix != ".json":
        raise ValueError(f"File {fpath} is not a JSON file.")
    return fpath


async def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Retrieve EWMS attributes for use by a Skymap Scanner instance.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_id",
        type=str,
        help="the scan id",
    )
    parser.add_argument(
        "--json-out",
        type=_assure_json,
        help="the json file to write the map of EWMS attributes to",
    )
    args = parser.parse_args()
    logging_tools.log_argparse_args(args)

    ewms_rc = ClientCredentialsAuth(
        ENV.EWMS_ADDRESS,
        ENV.EWMS_TOKEN_URL,
        ENV.EWMS_CLIENT_ID,
        ENV.EWMS_CLIENT_SECRET,
        logger=LOGGER,
    )
    skyd_rc = RestClient(
        ENV.SKYSCAN_SKYDRIVER_ADDRESS,
        ENV.SKYSCAN_SKYDRIVER_AUTH,
        logger=LOGGER,
    )

    # 0. check that a workflow has not already been requested for this scan
    resp = await skyd_rc.request("GET", f"/scan/{args.scan_id}/ewms/workflow-id")
    if not resp["is_pending_ewms_workflow"]:
        raise ValueError("this scan is not pending an EWMS workflow")
    #
    # 1. talk to ewms
    workflow_id = await request_workflow_on_ewms(
        ewms_rc, generate_presigned_s3_get_url(args.scan_id)
    )
    #
    # 2. update skydriver
    await skyd_rc.request(
        "POST",
        f"/scan/{args.scan_id}/ewms/workflow-id",
        {"workflow_id": workflow_id},
    )
    #
    # 3. talk to ewms (again) & write to file
    ewms_dict = await get_ewms_attrs(ewms_rc, workflow_id)
    LOGGER.info(f"dumping EWMS attributes to '{args.json_out}'...")
    with open(args.json_out, "w") as f:
        json.dump(ewms_dict, f)


if __name__ == "__main__":
    hand = logging.StreamHandler()
    hand.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)8s] %(name)s[%(process)d] %(message)s <%(filename)s:%(lineno)s/%(funcName)s()>",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(hand)
    logging_tools.set_level(
        "DEBUG",
        first_party_loggers=LOGGER,
        third_party_level="INFO",
        future_third_parties=[],
        specialty_loggers={"rest_tools": "INFO"},
    )
    asyncio.run(main())
    LOGGER.info("Done.")
