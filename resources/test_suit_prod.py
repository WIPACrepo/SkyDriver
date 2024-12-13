import argparse
import asyncio
import json
import logging
import shutil
import subprocess
from pathlib import Path

from rest_tools.client import RestClient

import test_getter
import test_runner

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GH_URL_RESULTS = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/results_json"
GH_URL_COMPARE_SCRIPT = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/compare_scan_results.py"


class ResultChecker:
    """Class to check/compare/assert scan results."""

    def __init__(self):
        self.compare_script_fpath = Path("./test-suit-sandbox/compare_scan_results.py")
        test_getter.download_file(GH_URL_COMPARE_SCRIPT, self.compare_script_fpath)

    def compare_results(self, scan_result: dict, test: test_getter.TestParamSet):
        """Compare scan result against expected result."""
        scan_result_file = (
            Path("./test-suit-sandbox/actual_results")
            / f"{test.reco_algo}-{test.event_file.name}.json"
        )
        scan_result_file.parent.mkdir(parents=True, exist_ok=True)
        with open(scan_result_file, "wb") as f:
            json.dump(scan_result, f)

        result = subprocess.run(
            [
                "python",
                str(self.compare_script_fpath),
                "--actual",
                str(scan_result_file),
                "--expected",
                str(test.result_file),
                "--diff-out-dir",
                str(scan_result_file.parent),
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
            raise ValueError(f"Mismatch in results: {test}")


async def wait_then_check_results(
    rc: RestClient,
    test: test_getter.TestParamSet,
    checker: ResultChecker,
):
    # monitor
    logging.info(
        f"Monitoring scan; see logs in {test.log_file}: {test.reco_algo} + {test.event_file}"
    )
    test.log_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        scan_result = await test_runner.monitor(rc, test.scan_id, test.log_file)
        logging.info(f"Scan {test.scan_id} completed successfully.")
    except Exception as e:
        logging.error(f"Error monitoring scan {test.scan_id}: {e}")
        raise

    # check
    logging.info(
        f"Comparing scan result to expected values: {test.reco_algo} + {test.event_file}"
    )
    checker.compare_results(scan_result, test)


async def launch_scans(
    tests: list[test_getter.TestParamSet],
    rc: RestClient,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    scanner_server_memory: str,
) -> list[test_getter.TestParamSet]:
    for i, test in enumerate(tests):
        logging.info(
            f"Launching test {i+1}/{len(tests)}: {test.reco_algo} + {test.event_file}"
        )
        try:
            scan_id = await test_runner.launch_a_scan(
                rc,
                test.event_file,
                cluster,
                n_workers,
                max_pixel_reco_time,
                test.reco_algo,
                scanner_server_memory,
            )
            test.scan_id = scan_id
            test.log_file = Path(f"./test-suit-sandbox/logs/{scan_id}.log")
        except Exception as e:
            logging.error(f"Failed to launch test #{i+1}: {e}")
            raise
    return tests


async def test_all(
    rc: RestClient,
    cluster: str,
    n_workers: int,
    max_pixel_reco_time: int,
    scanner_server_memory: str,
):
    tests = list(test_getter.setup_tests())
    tests = await launch_scans(
        tests,
        rc,
        cluster,
        n_workers,
        max_pixel_reco_time,
        scanner_server_memory,
    )
    checker = ResultChecker()

    logging.info("Starting scan watchers...")
    tasks = []
    for test in tests:
        tasks.append(
            asyncio.create_task(
                wait_then_check_results(rc, test, checker),
            )
        )

    while tasks:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            tasks.remove(task)
            try:
                await task
                logging.info("A test completed.")
            except Exception as e:
                logging.error(f"A test failed: {repr(e)}")

    logging.info("All tests completed.")


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

    rootdir = Path("./test-suit-sandbox")
    if rootdir.exists():
        shutil.rmtree(rootdir)
    rootdir.mkdir(exist_ok=True)

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
