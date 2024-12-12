import argparse
import asyncio
import logging
import subprocess
from pathlib import Path

import requests
from rest_tools.client import RestClient

import test_getter
import test_runner

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GH_URL_RESULTS = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/results_json"
GH_URL_COMPARE_SCRIPT = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/compare_scan_results.py"


def download_file(url: str, destination: Path):
    resp = requests.get(url)
    resp.raise_for_status()
    with open(destination, "wb") as f:
        f.write(resp.content)


def fetch_expected_result(event_file: Path, reco_algo: str) -> Path:
    dest = Path(f"./expected_results/{reco_algo}.{event_file.name}.json")
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{GH_URL_RESULTS}/{reco_algo}/..."  # TODO: get right file
    download_file(url, dest)
    return dest


def fetch_compare_script() -> Path:
    destination = Path("./compare_scan_results.py")
    download_file(GH_URL_COMPARE_SCRIPT, destination)
    return destination


def compare_results(scan_result: dict, event_file: Path, reco_algo: str):
    expected_file = fetch_expected_result(event_file, reco_algo)
    compare_script = fetch_compare_script()
    scan_result_file = Path()  # TODO: write scan_result to file

    try:
        result = subprocess.run(
            [
                "python",
                str(compare_script),
                "--actual",
                scan_result_file,
                "--expected",
                str(expected_file),
                "--assert",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            logging.info(f"Results for scan match expected output.")
        else:
            logging.error(f"Mismatch in results:")
            logging.error(result.stderr)

    except Exception as e:
        logging.error(f"Error running comparison: {e}")


async def wait_then_check_results(
    rc: RestClient,
    scan_id: str,
    log_file: Path,
    event_file: Path,
    reco_algo: str,
):
    try:
        scan_result = await test_runner.monitor(rc, scan_id, log_file)
        logging.info(f"Scan {scan_id} completed successfully.")
        compare_results(scan_result, event_file, reco_algo)
    except Exception as e:
        logging.error(f"Error monitoring scan {scan_id}: {e}")


def create_scan_tasks(
    test_combos: list[tuple[Path, str]],
    rc: RestClient,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    scanner_server_memory: str,
) -> dict[str, tuple[Path, Path, str]]:
    tasks = {}
    for i, (event_file, reco_algo) in enumerate(test_combos):
        logging.info(
            f"Launching test #{i+1} with file {event_file} and algo {reco_algo}"
        )
        try:
            scan_id = asyncio.run(
                test_runner.launch_a_scan(
                    rc,
                    event_file,
                    cluster,
                    n_workers,
                    max_pixel_reco_time,
                    reco_algo,
                    scanner_server_memory,
                )
            )
            log_file = Path(f"./logs/{scan_id}.log")
            tasks[scan_id] = (log_file, event_file, reco_algo)
        except Exception as e:
            logging.error(f"Failed to launch test #{i+1}: {e}")
    return tasks


async def test_all(
    rc: RestClient,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    scanner_server_memory: str,
):
    test_combos = list(test_getter.setup_tests())
    scan_tasks = create_scan_tasks(
        test_combos,
        rc,
        cluster,
        n_workers,
        max_pixel_reco_time,
        scanner_server_memory,
    )

    tasks = []
    for scan_id, (log_file, event_file, reco_algo) in scan_tasks.items():
        tasks.append(
            asyncio.create_task(
                wait_then_check_results(
                    rc,
                    scan_id,
                    log_file,
                    event_file,
                    reco_algo,
                )
            )
        )

    while tasks:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            tasks.remove(task)
            try:
                await task
                logging.info("A task completed.")
            except Exception as e:
                logging.error(f"A task failed: {e}")

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
