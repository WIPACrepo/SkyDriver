"""Simple script to rescan one or more scans."""

import argparse
import asyncio
import logging

from rest_tools.client import RestClient

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.getLogger().setLevel(logging.INFO)


async def rescan(rc: RestClient, scan_id: str) -> str:
    """Request a single rescan."""
    try:
        manifest = await rc.request("POST", f"/scan/{scan_id}/actions/rescan")
        logging.info(f"Requested rescan: old={scan_id} | new={manifest["scan_id"]}")
        return manifest["scan_id"]
    except Exception as e:
        logging.error(f"Failed to rescan {scan_id}: {e}")
        return "error"


async def main():
    parser = argparse.ArgumentParser(
        description="Submit one or more rescan requests.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_ids",
        nargs="+",
        help="one or more scan IDs to rescan",
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

    new_ids = []
    for scan_id in args.scan_ids:  # do this sync b/c we want ids to be in order
        new_ids.append(await rescan(rc, scan_id))

    logging.info(f"new ids: {" ".join(new_ids)}")
    logging.info(f"n={len(new_ids)}")


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
