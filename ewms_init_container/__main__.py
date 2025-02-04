"""Run the EWMS Init Container logic."""

import argparse
import asyncio
import json
import logging
import os
import time
from pathlib import Path

from rest_tools.client import ClientCredentialsAuth, RestClient

LOGGER = logging.getLogger(__package__)


async def get_workflow_id(scan_id: str) -> str:
    """Retrieve the workflow id for the scan (w/ `scan_id`)."""
    LOGGER.info(f"getting workflow id for scan {scan_id}...")

    skyd_rc = RestClient(
        os.environ["SKYSCAN_SKYDRIVER_ADDRESS"],
        os.environ["SKYSCAN_SKYDRIVER_AUTH"],
        logger=LOGGER,
    )
    resp = await skyd_rc.request("GET", f"/scan/{scan_id}/manifest")
    workflow_id = resp["ewms_workflow_id"]

    LOGGER.info(f"workflow id: {workflow_id}")
    return workflow_id


async def get_ewms_attrs(workflow_id: str) -> dict[str, dict[str, str]]:
    """Retrieve the EWMS attributes for the workflow."""
    LOGGER.info(f"getting EWMS attributes for workflow {workflow_id}...")

    ewms_rc = ClientCredentialsAuth(
        os.environ["EWMS_ADDRESS"],
        os.environ["EWMS_TOKEN_URL"],
        os.environ["EWMS_CLIENT_ID"],
        os.environ["EWMS_CLIENT_SECRET"],
        logger=LOGGER,
    )

    # loop until mqprofiles is not empty and all "is_activated" fields are true
    while True:
        LOGGER.info("requesting EWMS mqprofiles...")
        mqprofiles = (
            await ewms_rc.request(
                "GET",
                f"/v0/mqs/workflows/{workflow_id}/mq-profiles/public",
            )
        )["mqprofiles"]
        if mqprofiles and all(m["is_activated"] for m in mqprofiles):
            break
        else:
            LOGGER.info("mqprofiles are not all activated, retrying soon...")
            time.sleep(10)

    LOGGER.info(f"mqprofiles: {mqprofiles}")

    # convert mqprofiles to dicts based on the queue aliases
    toclient = next(
        p for p in mqprofiles if p["mqid"] == os.environ["QUEUE_ALIAS_TOCLIENT"]
    )
    fromclient = next(
        p for p in mqprofiles if p["mqid"] == os.environ["QUEUE_ALIAS_FROMCLIENT"]
    )

    return {
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

    workflow_id = await get_workflow_id(args.scan_id)
    ewms_dict = await get_ewms_attrs(workflow_id)

    LOGGER.info(f"dumping EWMS attributes to '{args.json_out}'...")
    with open(args.json_out, "w") as f:
        json.dump(ewms_dict, f)


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
