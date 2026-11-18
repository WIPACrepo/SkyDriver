"""Simple script to grab a scan's result.

Useful for quick debugging in prod.
"""

import argparse
import asyncio
import json
import logging

from _connect import get_rest_client  # type: ignore[import-not-found]  # ty: ignore[import-not-found]

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(
        description="Get a scan's result",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scan-id",
        required=True,
        help="the scan's id",
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

    LOGGER.info(f"getting result for scan {args.scan_id}")
    resp = await rc.request(
        "GET", f"/scan/{args.scan_id}/result", {"include_deleted": True}
    )
    with open(f"{args.scan_id}.result.json", "w") as f:  # noqa: ASYNC230  # one-shot CLI write
        json.dump(resp, f, indent=4)
        LOGGER.info(f"result written to {f.name}")


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
