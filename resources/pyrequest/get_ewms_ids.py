"""Helper script to lookup all associated EWMS IDs for a scan ID, or visa versa."""

import argparse
import asyncio
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import AsyncIterable

import requests
from rest_tools.client import RestClient

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

EWMS_HELPER_SCRIPT_URL = "https://raw.githubusercontent.com/Observation-Management-Service/ewms-workflow-management-service/refs/heads/main/resources/get_all_ids.py"


def print_scan_id(scan_id: str) -> None:
    """Print a scan ID in a formatted way."""
    print(f"scan_id :: {scan_id}")


async def grab_scan_info(rc: RestClient, scan_ids: list[str]) -> AsyncIterable[dict]:
    """Fetch manifest metadata for each scan ID from SkyDriver."""
    for scan_id in scan_ids:
        try:
            resp = await rc.request(
                "GET",
                f"/scan/{scan_id}/manifest",
                {"include_deleted": True},
            )
        except requests.exceptions.HTTPError as e:
            if "404" in str(e):
                LOGGER.warning(f"Scan not found: {scan_id}")
                continue
            raise
        yield {
            k: v
            for k, v in resp.items()
            if k in {"scan_id", "ewms_workflow_id", "ewms_address"}
        }


def run_ewms_helper_script(cl_args: str) -> list[dict]:
    """Download and run the EWMS helper script with the given CLI args, returning parsed JSON output."""
    with tempfile.NamedTemporaryFile(suffix=".py") as f:
        subprocess.run(
            ["curl", "-sSfL", EWMS_HELPER_SCRIPT_URL, "-o", f.name],
            check=True,
        )

        # check if the token file exists -- this is hard to do with subproc w/ captured stdout
        if (
            not Path(
                f"~/device-refresh-token-ewms-{'prod' if 'prod' in cl_args else 'dev'}"
            )
            .expanduser()
            .exists()
        ):
            LOGGER.warning("you need a device client token first, so running 2x...")
            subprocess.run(["python3", f.name, *cl_args.split()], check=True)

        # get ids from ewms
        proc = subprocess.run(
            ["python3", f.name, *cl_args.split()],
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            check=True,
            text=True,
        )
        print("ewms ids ::")
        print(proc.stdout.strip())
        return json.loads(proc.stdout)


def print_banner(char: str = "-", width: int = 40) -> None:
    """Print a visual banner using the specified character."""
    print("\n" + char * width)


async def main():
    """Main entrypoint for the CLI: maps scan IDs <-> EWMS metadata."""
    parser = argparse.ArgumentParser(
        description="Lookup all associated EWMS IDs for a scan ID, or visa versa",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_ids",
        nargs="*",
        help="One or more SkyDriver scan IDs",
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=["dev", "prod"],
        help=(
            "Target SkyDriver environment "
            "(dev → https://skydriver-dev.icecube.aq, prod → https://skydriver.icecube.aq)"
        ),
    )
    parser.add_argument(
        "--ewms-ids",
        default="",
        help="Command-line args to forward to the EWMS helper script (use only if not providing scan_ids)",
    )
    args = parser.parse_args()

    if bool(args.scan_ids) == bool(args.ewms_ids):
        raise ValueError("Specify exactly one of: SCAN_IDS or --ewms-ids")

    rc = get_rest_client(args.skydriver_type)

    # SkyDriver -> EWMS
    if args.scan_ids:
        print_banner()
        async for ids in grab_scan_info(rc, args.scan_ids):
            print_scan_id(ids["scan_id"])
            ewms_type = (
                ids["ewms_address"]
                .removeprefix("https://ewms-")
                .removesuffix(".icecube.aq")
            )
            run_ewms_helper_script(f"--ewms {ewms_type} --wf {ids['ewms_workflow_id']}")
            print_banner()
    # EWMS -> SkyDriver
    else:
        print_banner()
        for ids in run_ewms_helper_script(args.ewms_ids):
            resp = await rc.request(
                "POST",
                "/scans/find",
                {
                    "filter": {"ewms_workflow_id": ids["workflow_id"]},
                    "include_deleted": True,
                },
            )
            for manifest in resp["manifests"]:
                print_scan_id(manifest["scan_id"])
            print_banner()

    print_banner(".")


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
