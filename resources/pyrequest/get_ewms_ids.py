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
from urllib.parse import urlparse

import requests
from rest_tools.client import RestClient
from wipac_dev_tools import logging_tools

from _connect import get_rest_client  # type: ignore[import-not-found]

EWMS_HELPER_SCRIPT_URLS = [
    "https://raw.githubusercontent.com/Observation-Management-Service/ewms-workflow-management-service/refs/heads/main/resources/get_all_ids.py",
    "https://raw.githubusercontent.com/Observation-Management-Service/ewms-workflow-management-service/refs/heads/main/resources/utils.py",
]
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


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


def run_ewms_helper_script(ewms_args: str) -> list[dict]:
    """Download and run the EWMS helper script with the given CLI args, returning parsed JSON output."""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        downloaded = []
        for url in EWMS_HELPER_SCRIPT_URLS:
            name = Path(urlparse(url).path).name  # keep the file's original name
            dest = tmpdir_path / name
            subprocess.run(
                ["curl", "-sSfL", url, "-o", str(dest)],
                check=True,
            )
            downloaded.append(dest)

        script = [p for p in downloaded if p.name == "get_all_ids.py"][0]

        # check if the token file exists -- this is hard to do with subproc w/ captured stdout
        if (
            not Path(
                f"~/device-refresh-token-ewms-{'prod' if 'prod' in ewms_args else 'dev'}"
            )
            .expanduser()
            .exists()
        ):
            LOGGER.warning("you need a device client token first, so running 2x...")
            subprocess.run(["python3", script, *ewms_args.split()], check=True)

        # get ids from ewms
        try:
            proc = subprocess.run(
                ["python3", script, *ewms_args.split()],
                stdout=subprocess.PIPE,
                stderr=sys.stderr,
                check=True,
                text=True,
            )
            print("ewms ids ::")
            print(proc.stdout.strip())
        except subprocess.CalledProcessError as e:
            print(e.stdout.strip(), file=sys.stderr)
            raise RuntimeError(
                "issue running ewms helper script — see above error message "
                "(if this is an issue with '--ewms-args', run with '--ewms-args -h')"
            ) from None  # suppress 'CalledProcessError' stack trace

        # done here
        if " -h" in ewms_args:
            sys.exit(2)
        else:
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
        "--scan-ids",
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
        "--ewms-args",
        default="",
        help="Command-line args to forward to the EWMS helper script (use only if not providing --scan-ids)",
    )
    args = parser.parse_args()
    logging_tools.log_argparse_args(args, LOGGER)

    if bool(args.scan_ids) == bool(args.ewms_args):
        raise ValueError("Specify exactly one of: --scan-ids or --ewms-args")

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
        for ids in run_ewms_helper_script(args.ewms_args):
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
