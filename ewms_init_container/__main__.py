"""Run the EWMS Init Container logic."""

import argparse
import asyncio
import dataclasses as dc
import json
import logging
import time
from pathlib import Path

from rest_tools.client import ClientCredentialsAuth, RestClient
from wipac_dev_tools import from_environment_as_dataclass, logging_tools

LOGGER = logging.getLogger(__package__)


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    SKYSCAN_SKYDRIVER_ADDRESS: str
    SKYSCAN_SKYDRIVER_AUTH: str

    EWMS_ADDRESS: str
    EWMS_TOKEN_URL: str
    EWMS_CLIENT_ID: str
    EWMS_CLIENT_SECRET: str

    QUEUE_ALIAS_TOCLIENT: str
    QUEUE_ALIAS_FROMCLIENT: str


ENV = from_environment_as_dataclass(EnvConfig)


async def get_workflow_id(scan_id: str) -> str:
    """Retrieve the workflow id for the scan (w/ `scan_id`)."""
    LOGGER.info(f"getting workflow id for scan {scan_id}...")

    skyd_rc = RestClient(
        ENV.SKYSCAN_SKYDRIVER_ADDRESS,
        ENV.SKYSCAN_SKYDRIVER_AUTH,
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
        ENV.EWMS_ADDRESS,
        ENV.EWMS_TOKEN_URL,
        ENV.EWMS_CLIENT_ID,
        ENV.EWMS_CLIENT_SECRET,
        logger=LOGGER,
    )

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
    toclient = next(p for p in mqprofiles if p["mqid"] == ENV.QUEUE_ALIAS_TOCLIENT)
    fromclient = next(p for p in mqprofiles if p["mqid"] == ENV.QUEUE_ALIAS_FROMCLIENT)

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
    logging_tools.log_argparse_args(args)

    workflow_id = await get_workflow_id(args.scan_id)
    ewms_dict = await get_ewms_attrs(workflow_id)

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
        "INFO",
        first_party_loggers=LOGGER,
        third_party_level="INFO",
        future_third_parties=[],
        specialty_loggers={"rest_tools": "INFO"},
    )
    asyncio.run(main())
    LOGGER.info("Done.")
