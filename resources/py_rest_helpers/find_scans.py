"""Simple script to grab scan ids for a given query.

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
        description="Find scan ids for a given query",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "query",
        type=json.loads,
        help="the JSON query (mongo syntax)",
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="the url to connect to a SkyDriver server",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_url)

    logging.info(f"getting manifest for scans w/ {args.query}")
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": args.query,
            "include_deleted": True,
        },
    )
    print(json.dumps(resp, indent=4), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
