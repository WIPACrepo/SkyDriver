"""Simple script to stop one or more in-progress scans.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.getLogger().setLevel(logging.INFO)


async def stop_scan(rc, scan_id):
    """Request a stop for a single scan."""
    try:
        logging.info(f"Requesting scan to be stopped: {scan_id}")
        resp = await rc.request("DELETE", f"/scan/{scan_id}")
        print(f"Scan {scan_id} response:\n{json.dumps(resp, indent=4)}", flush=True)
    except Exception as e:
        logging.error(f"Failed to stop scan {scan_id}: {e}")


async def main():
    parser = argparse.ArgumentParser(
        description="Stop one or more in-progress scans",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_ids",
        nargs="+",
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
    logging.info("Done.")
