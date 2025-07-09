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
    print(len(resp), flush=True)
    for key in resp.keys():
        print(f"{key}: {len(resp[key])}")


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
