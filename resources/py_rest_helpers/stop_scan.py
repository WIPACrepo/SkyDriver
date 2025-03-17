"""Simple script to grab stop an in-progress scan.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging

from ._connect import get_rest_client

logging.getLogger().setLevel(logging.INFO)


async def main():
    parser = argparse.ArgumentParser(
        description="Stop an in-progress scan",
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
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_url)

    logging.info(f"requesting scan to be stopped {args.scan_id}")
    resp = await rc.request("DELETE", f"/scan/{args.scan_id}")
    print(json.dumps(resp, indent=4), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
