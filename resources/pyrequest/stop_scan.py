"""Simple script to stop one or more in-progress scans.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging

from _connect import get_rest_client  # type: ignore[import-not-found]  # ty: ignore[import-not-found]

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


async def stop_scan(rc, scan_id):
    """Request a stop for a single scan."""
    try:
        LOGGER.info(f"Requesting scan to be stopped: {scan_id}")
        resp = await rc.request("DELETE", f"/scan/{scan_id}")
        print(f"Scan {scan_id} response:\n{json.dumps(resp, indent=4)}", flush=True)
    except Exception as e:  # noqa: BLE001  # report failure for this scan, then continue
        LOGGER.error(f"Failed to stop scan {scan_id}: {e}")


async def main():
    parser = argparse.ArgumentParser(
        description="Stop one or more in-progress scans",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scan-ids",
        nargs="+",
        required=True,
        help="one or more scan IDs to stop",
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=["dev", "prod"],
        help=(
            "the type of the SkyDriver instance for REST API URL "
            "(ex: prod -> https://skydriver.icecube.aq; dev -> https://skydriver-dev.icecube.aq)"
        ),
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    tasks = [stop_scan(rc, scan_id) for scan_id in args.scan_ids]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
