"""Simple script to rescan a scan."""

import argparse
import asyncio
import logging

from ._connect import get_rest_client

logging.getLogger().setLevel(logging.INFO)


async def main():
    parser = argparse.ArgumentParser(
        description="Submit a rescan request.",
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

    manifest = await rc.request("POST", f"/scan/{args.scan_id}/actions/rescan")
    print(manifest["scan_id"], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
