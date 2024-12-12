import argparse
import asyncio
import logging

from rest_tools.client import RestClient

import test_getter
import test_runner

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Main coroutine to manage tasks using TaskGroup
async def test_all(
    rc: RestClient,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    scanner_server_memory: str,
):
    test_combos = list(test_getter.setup_tests())

    for i, (event_file, reco_algo) in enumerate(test_combos):
        print(f"Launching test #{i+1}...")
        await test_runner.launch_a_scan(
            rc,
            event_file,
            cluster,
            n_workers,
            max_pixel_reco_time,
            reco_algo,
            scanner_server_memory,
        )


async def main():
    parser = argparse.ArgumentParser(
        description="Launch and monitor a scan for an event",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="the url to connect to a SkyDriver server",
    )
    parser.add_argument(
        "--cluster",
        required=True,
        help="the cluster to use for running workers. Ex: sub-2",
    )
    parser.add_argument(
        "--n-workers",
        required=True,
        type=int,
        help="number of workers to request",
    )
    parser.add_argument(
        "--max-pixel-reco-time",
        required=True,
        type=int,
        help="how long a reco should take",
    )
    parser.add_argument(
        "--scanner-server-memory",
        required=False,
        default="512M",
        help="server memory required",
    )
    args = parser.parse_args()

    rc = test_runner.get_rest_client(args.skydriver_url)

    await test_all(
        rc,
        args.cluster,
        args.n_workers,
        args.max_pixel_reco_time,
        args.scanner_server_memory,
    )


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
