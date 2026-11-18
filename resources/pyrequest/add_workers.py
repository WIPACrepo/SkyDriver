"""Simple script to add workers to scans."""

import argparse
import asyncio
import logging

from _connect import get_rest_client  # type: ignore[import-not-found]  # ty: ignore[import-not-found]
from rest_tools.client import RestClient

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


async def add_workers(
    rc: RestClient, scan_id: str, n_workers: int, location: str
) -> None:
    """Request a single 'add-workers' action."""
    try:
        resp = await rc.request(
            "POST",
            f"/scan/{scan_id}/actions/add-workers",
            {"n_workers": n_workers, "cluster_location": location},
        )
        LOGGER.info(f"{scan_id}: {resp}")
    except Exception as e:  # noqa: BLE001  # report failure for this scan, then continue
        LOGGER.error(f"Failed {scan_id}: {e}")


async def main():
    parser = argparse.ArgumentParser(
        description="Submit one or more 'add-workers' requests.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scan-ids",
        nargs="+",
        required=True,
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
        "--n-workers",
        required=True,
        type=int,
        help="the number of workers to add",
    )
    parser.add_argument(
        "--location",
        required=True,
        help="the condor location to add workers to",
    )
    args = parser.parse_args()

    rc = get_rest_client(args.skydriver_type)

    tasks = [
        add_workers(rc, scan_id, args.n_workers, args.location)
        for scan_id in args.scan_ids
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    LOGGER.info("Done.")
