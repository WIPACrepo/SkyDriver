"""Simple script to rescan one or more scans."""

import argparse
import asyncio
import logging

import wipac_dev_tools
from rest_tools.client import RestClient

from _connect import get_rest_client  # type: ignore[import-not-found]

logging.getLogger().setLevel(logging.INFO)


async def rescan(
    rc: RestClient,
    scan_id: str,
    abort_first: bool,
    replace_scan: bool,
) -> str:
    """Request a single rescan."""
    try:
        manifest = await rc.request(
            "POST",
            f"/scan/{scan_id}/actions/rescan",
            {
                "abort_first": abort_first,
                "replace_scan": replace_scan,
            },
        )
        logging.info(f"Requested rescan: old={scan_id} | new={manifest['scan_id']}")
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
    parser.add_argument(
        "--abort-first",
        type=wipac_dev_tools.strtobool,
        required=True,
        help="Whether to abort the original scan (true/false)",
    )
    parser.add_argument(
        "--replace-scan",
        type=wipac_dev_tools.strtobool,
        required=True,
        help="Whether the new scan should replace the original (true/false)",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    new_ids = []
    for scan_id in args.scan_ids:
        new_ids.append(await rescan(rc, scan_id, args.abort_first, args.replace_scan))

    logging.info(f"new ids: {" ".join(new_ids)}")
    logging.info(f"n={len(new_ids)}")


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
