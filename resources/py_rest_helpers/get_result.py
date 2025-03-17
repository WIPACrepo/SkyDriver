"""Simple script to grab a scan's result.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging
from pathlib import Path

from ._connect import get_rest_client

logging.getLogger().setLevel(logging.INFO)


async def main():
    parser = argparse.ArgumentParser(
        description="Get a scan's result",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_id",
        help="the scan's id",
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="the url to connect to a SkyDriver server",
    )
    parser.add_argument(
        "--file",
        required=True,
        type=Path,
        help="a file to write the result to",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_url)

    logging.info(f"getting result for scan {args.scan_id}")
    resp = await rc.request(
        "GET", f"/scan/{args.scan_id}/result", {"include_deleted": True}
    )
    with open(args.file, "w") as f:
        print(json.dump(resp, f, indent=4))
    logging.info(f"result written to {args.file}")


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
