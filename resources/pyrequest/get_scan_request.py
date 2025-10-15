"""Helper to GET one or more stored scan-requests and print them.

Usage:
    python get_scan_request.py --scan-ids <scan_id> [<scan_id> ...] --skydriver {dev,prod}
"""

import argparse
import asyncio
import json
import logging
from typing import Any

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.getLogger().setLevel(logging.INFO)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="GET one or more stored scan-requests and print them",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scan-ids",
        nargs="+",
        required=True,
        help="one or more scan IDs to retrieve",
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=["dev", "prod"],
        help="SkyDriver environment",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    for scan_id in args.scan_ids:
        logging.info(f"Fetching stored request for scan_id={scan_id}")
        source: dict[str, Any] = await rc.request("GET", f"/scan-request/{scan_id}")
        print(f"\n=== scan {scan_id} ===", flush=True)
        print(json.dumps(source, indent=4), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
