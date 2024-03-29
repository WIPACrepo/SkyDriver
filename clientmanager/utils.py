"""General Utilities."""


import dataclasses as dc
import logging
from pathlib import Path
from typing import Any

import boto3  # type: ignore[import-untyped]
import requests
from rest_tools.client import RestClient

from .config import ENV

LOGGER = logging.getLogger(__name__)


def connect_to_skydriver() -> RestClient:
    """Connect to SkyDriver REST server & check scan id."""
    if not ENV.SKYSCAN_SKYDRIVER_SCAN_ID:
        raise RuntimeError(
            "Cannot connect to SkyDriver without `SKYSCAN_SKYDRIVER_SCAN_ID`"
        )

    skydriver_rc = RestClient(
        ENV.SKYSCAN_SKYDRIVER_ADDRESS,
        token=ENV.SKYSCAN_SKYDRIVER_AUTH,
    )

    LOGGER.info("Connected to SkyDriver")
    return skydriver_rc


def skydriver_aborted_scan(skydriver_rc: RestClient) -> bool:
    """Return whether the scan has been signaled for deletion."""
    ret = skydriver_rc.request_seq(
        "GET",
        f"/scan/{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}/manifest",
    )
    return ret["is_deleted"]  # type: ignore[no-any-return]


def update_skydriver(
    skydriver_rc: RestClient,
    orchestrator: str,
    location: dict[str, str],
    uuid: str,
    cluster_id: str | int,
    n_workers: int,
    starter_info: dict[str, Any],
    #
    statuses: dict[str, dict[str, int]] | None = None,
    top_task_errors: dict[str, int] | None = None,
) -> None:
    """Send SkyDriver updates from the `submit_result`."""
    skydriver_cluster_obj = {
        "orchestrator": orchestrator,
        "location": location,
        "uuid": uuid,
        "cluster_id": str(cluster_id),
        "n_workers": n_workers,
        "starter_info": starter_info,
    }
    if statuses:
        skydriver_cluster_obj["statuses"] = statuses
    if top_task_errors:
        skydriver_cluster_obj["top_task_errors"] = top_task_errors

    skydriver_rc.request_seq(
        "PATCH",
        f"/scan/{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}/manifest",
        {"cluster": skydriver_cluster_obj},
    )


@dc.dataclass
class S3File:
    """Wrap an S3 file."""

    url: str
    fname: str


def s3ify(filepath: Path) -> S3File:
    """Put the file in s3 and return info about it."""
    if not (
        ENV.EWMS_TMS_S3_URL
        and ENV.EWMS_TMS_S3_ACCESS_KEY_ID
        and ENV.EWMS_TMS_S3_SECRET_KEY
        and ENV.EWMS_TMS_S3_BUCKET
        and ENV.SKYSCAN_SKYDRIVER_SCAN_ID
    ):
        raise RuntimeError(
            "must define all EWMS_TMS_S3_* environment variables to use S3"
        )
    s3_client = boto3.client(
        "s3",
        "us-east-1",
        endpoint_url=ENV.EWMS_TMS_S3_URL,
        aws_access_key_id=ENV.EWMS_TMS_S3_ACCESS_KEY_ID,
        aws_secret_access_key=ENV.EWMS_TMS_S3_SECRET_KEY,
    )
    bucket = ENV.EWMS_TMS_S3_BUCKET
    key = f"{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}-s3-{filepath.stem}"

    # get GET url
    get_url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket,
            "Key": key,
        },
        ExpiresIn=ENV.EWMS_TMS_S3_EXPIRATION,  # seconds
    )
    s3_file = S3File(get_url, key)

    # check if already there (via other process/container)
    try:
        resp = requests.get(get_url)
        resp.raise_for_status()
        LOGGER.debug(resp)
        LOGGER.info(f"File is already in S3. Using url: {get_url}")
        return s3_file
    except requests.exceptions.HTTPError:
        LOGGER.info("File is not in S3 yet. Posting...")

    # POST
    upload_details = s3_client.generate_presigned_post(bucket, key)
    with open(filepath, "rb") as f:
        response = requests.post(
            upload_details["url"],
            data=upload_details["fields"],
            files={"file": (filepath.name, f)},  # maps filename to obj
        )
    LOGGER.info(f"Upload response: {response.status_code}")
    LOGGER.info(str(response.content))

    return s3_file
