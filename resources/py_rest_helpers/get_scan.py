"""Simple script to grab one or more scan manifests, statuses, and logs.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging
from typing import Any

from ._connect import (
    get_rest_client,
)  # assumed to return an HTTP client with an async request method

logging.getLogger().setLevel(logging.INFO)


async def fetch_scan_info(
    rc: Any,
    scan_id: str,
    show_manifest: bool,
    show_status: bool,
    show_logs: bool,
) -> None:
    logging.info(f"Processing scan {scan_id}")

    if show_manifest:
        logging.info(f"Getting manifest for scan {scan_id}")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/manifest", {"include_deleted": True}
        )
        print(f"\n=== Manifest for scan {scan_id} ===")
        print(json.dumps(resp, indent=4), flush=True)

    if show_status:
        logging.info(f"Getting status for scan {scan_id}")
        resp = await rc.request("GET", f"/scan/{scan_id}/status")
        print(f"\n=== Status for scan {scan_id} ===")
        print(json.dumps(resp, indent=4), flush=True)

    if show_logs:
        logging.info(f"Getting logs for scan {scan_id}")
        resp = await rc.request("GET", f"/scan/{scan_id}/logs")
        print(f"\n=== Logs for scan {scan_id} ===")
        print(json.dumps(resp, indent=4), flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Get data for one or more scans",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_ids",
        nargs="+",
        help="One or more scan IDs",
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="The URL to connect to a SkyDriver server",
    )
    parser.add_argument(
        "--manifest",
        action="store_true",
        help="Include manifest info",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Include status info",
    )
    parser.add_argument(
        "--logs",
        action="store_true",
        help="Include logs info",
    )
    args = parser.parse_args()

    if not (args.manifest or args.status or args.logs):
        parser.error(
            "At least one of --manifest, --status, or --logs must be specified."
        )

    rc = get_rest_client(args.skydriver_url)

    for scan_id in args.scan_ids:
        await fetch_scan_info(rc, scan_id, args.manifest, args.status, args.logs)

    logging.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
