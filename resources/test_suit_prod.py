import argparse
import asyncio
import logging
from pathlib import Path

from rest_tools.client import RestClient

import test_getter
import test_runner

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def _monitor_scan(rc: RestClient, scan_id: str, log_file: Path):
    try:
        await test_runner.monitor(rc, scan_id, log_file)
        logging.info(f"Scan {scan_id} completed successfully.")
    except Exception as e:
        logging.error(f"Error monitoring scan {scan_id}: {e}")


# Main coroutine to manage tasks using TaskGroup
async def test_all(
    rc: RestClient,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    scanner_server_memory: str,
):
    test_combos = list(test_getter.setup_tests())
    scan_monitors = {}

    for i, (event_file, reco_algo) in enumerate(test_combos):
        logging.info(
            f"Launching test #{i+1} with file {event_file} and algo {reco_algo}"
        )
        try:
            scan_id = await test_runner.launch_a_scan(
                rc,
                event_file,
                cluster,
                n_workers,
                max_pixel_reco_time,
                reco_algo,
                scanner_server_memory,
            )
        except Exception as e:
            logging.error(f"Failed to launch test #{i+1}: {e}")

        log_file = Path(f"./logs/{scan_id}.log")
        scan_monitors[scan_id] = asyncio.create_task(
            _monitor_scan(rc, scan_id, log_file)
        )

    while scan_monitors:
        # Wait for the first task to complete
        done, _ = await asyncio.wait(
            scan_monitors.values(), return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            scan_id = next(key for key, value in scan_monitors.items() if value == task)
            try:
                await task
                logging.info(f"Task for scan {scan_id} finished.")
            except Exception as e:
                logging.error(f"Task for scan {scan_id} failed: {e}")

            # Remove the completed task from the dictionary
            del scan_monitors[scan_id]

    logging.info("All tasks completed.")


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
