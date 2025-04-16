"""Simple script to add workers to scans."""

import argparse
import asyncio
import logging

from rest_tools.client import RestClient

from ._connect import get_rest_client

logging.getLogger().setLevel(logging.INFO)


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
        logging.info(f"{scan_id}: {resp}")
    except Exception as e:
        logging.error(f"Failed {scan_id}: {e}")


async def main():
    parser = argparse.ArgumentParser(
        description="Submit one or more 'add-workers' requests.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "scan_ids",
        nargs="+",
        help="one or more scan IDs to rescan",
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="the url to connect to a SkyDriver server",
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

    rc = get_rest_client(args.skydriver_url)

    tasks = [
        add_workers(rc, scan_id, args.n_workers, args.location)
        for scan_id in args.scan_ids
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
