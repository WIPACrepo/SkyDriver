import argparse
import asyncio
import json
import logging
import os
import shutil
import subprocess
import tarfile
from datetime import date, datetime
from pathlib import Path

import texttable  # type: ignore
import wipac_dev_tools
from rest_tools.client import RestClient

from . import config, test_getter, test_runner

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DryRunException(Exception):
    """Raised in order to stop execution when using the dry run option."""


class ResultChecker:
    """Class to check/compare/assert scan results."""

    def __init__(self, no_cache: bool):
        self.compare_script_fpath = config.SANDBOX_DIR / "compare_scan_results.py"
        test_getter.download_file(
            config.GH_URL_COMPARE_SCRIPT,
            self.compare_script_fpath,
            no_cache,
        )

    def compare_results(
        self,
        test: test_getter.TestParamSet,
        scan_result: dict | None = None,
    ) -> None:
        """Compare scan result against expected result."""
        logging.info(
            f"Comparing scan result to expected values: {test.reco_algo} + {test.event_file}"
        )

        scan_result_file = (
            config.SANDBOX_DIR
            / "actual_results"
            / f"{test.reco_algo}-{test.event_file.name}.json"
        )
        scan_result_file.parent.mkdir(parents=True, exist_ok=True)

        if scan_result is None:
            if not scan_result_file.exists():
                raise FileNotFoundError(scan_result_file)
        else:
            with open(scan_result_file, "w") as f:
                json.dump(scan_result, f)

        diffs_dir = config.SANDBOX_DIR / "result_diffs"
        diffs_dir.mkdir(parents=True, exist_ok=True)

        def _check_time_bomb() -> bool:
            # see https://github.com/icecube/skymap_scanner/blob/cb422e412d1607ce1e0ea2db4402a4e3461908ed/.github/workflows/tests.yml#L539-L560
            if date.today() > date(2026, 3, 18):
                raise RuntimeError(
                    "************************* HEY LOOK AT THIS FAILURE ************************* "
                    "we need to attend to # see https://github.com/icecube/skymap_scanner/blob/cb422e412d1607ce1e0ea2db4402a4e3461908ed/.github/workflows/tests.yml#L539-L560"
                )
            return True

        result = subprocess.run(
            [
                "python",
                str(self.compare_script_fpath),
                "--actual",
                str(scan_result_file),
                "--expected",
                str(test.result_file),
                "--diff-out-dir",
                str(diffs_dir),
                "--assert",
            ]
            + (  # see https://github.com/icecube/skymap_scanner/blob/cb422e412d1607ce1e0ea2db4402a4e3461908ed/.github/workflows/tests.yml#L539-L560
                ["--compare-different-versions-ok"]
                if test.reco_algo == "splinempe" and _check_time_bomb()
                else []
            ),
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            logging.info("> PASSED: Results for scan match expected output.")
        else:
            logging.error("> FAILED: Mismatch in results...")
            logging.error(result.stderr)
            raise TestException("Mismatch in results", test)


class TestException(Exception):
    """Raised for any testing error."""

    def __init__(self, message: str, test: test_getter.TestParamSet):
        super().__init__(message)
        self.test = test


async def wait_then_check_results(
    rc: RestClient,
    test: test_getter.TestParamSet,
    checker: ResultChecker,
) -> test_getter.TestParamSet:
    """Wait until the scan is done, then check its result."""
    try:
        logging.info(
            f"Monitoring scan; see logs in {test.log_file}: {test.reco_algo} + {test.event_file}"
        )
        test.log_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Wait until the scan is done...
            scan_result = await test_runner.monitor(rc, test.scan_id, test.log_file)
            logging.info(f"Scan {test.scan_id} completed successfully.")
        except Exception as e:
            logging.error(f"Error monitoring scan {test.scan_id}: {e}")
            raise
        else:
            # then check its result
            checker.compare_results(test, scan_result)

    except Exception as e:
        # no error in testing shall bring down the test suite
        raise TestException(repr(e), test) from e

    # done here
    return test


async def launch_scans(
    tests: list[test_getter.TestParamSet],
    rc: RestClient,
    cluster: str,
    n_workers: int,
    skyscan_docker_tag: str,
    priority: int,
) -> list[test_getter.TestParamSet]:
    for i, test in enumerate(tests):
        logging.info(
            f"Launching test {i+1}/{len(tests)}: {test.reco_algo} + {test.event_file}"
        )
        test.test_status = test_getter.TestStatus.REQUESTED
        try:
            # rescan?
            if test.rescan_origin_id:
                manifest = await test_runner.rescan_a_scan(
                    rc,
                    test.rescan_origin_id,
                )
                test.scan_id = manifest["scan_id"]
                assert test.scan_id != test.rescan_origin_id
            # or normal scan?
            else:
                manifest = await test_runner.launch_a_scan(
                    rc,
                    test.event_file,
                    cluster,
                    n_workers,
                    test.reco_algo,
                    skyscan_docker_tag,
                    priority,
                )
                test.scan_id = manifest["scan_id"]
        except Exception as e:
            logging.error(f"Failed to launch test #{i+1}: {e}")
            raise
    return tests


def display_test_status(tests: list[test_getter.TestParamSet]):
    """Display test statuses in a clean table format."""
    sorted_tests = sorted(
        enumerate(tests, start=1),
        key=lambda x: (x[1].test_status.name, x[0]),
    )
    table = texttable.Texttable()

    scan_id_len = 10

    # columns
    table.add_row(["#", "Event File", "Reco Algo", "Scan ID", "Status"])
    table.set_cols_align(["r", "l", "l", "r", "l"])
    table.set_cols_width([2, 25, 18, scan_id_len, 10])
    table.set_cols_dtype(["i", "t", "t", "t", "t"])

    # Add rows for each test
    for i, test in sorted_tests:
        scan_id = test.scan_id[:scan_id_len] if test.scan_id else "N/A"
        status = test.test_status.name
        table.add_row([i, test.event_file.name, test.reco_algo, scan_id, status])

    print(table.draw())


def _match_rescans_to_tests(
    rescans: list[test_getter.TestParamSet], tests: list[test_getter.TestParamSet]
) -> None:
    """Match rescans to tests, in order to send the rescan id to skydriver."""
    logging.info("matching tests to rescan-tests")
    logging.info(json.dumps([r.to_json() for r in rescans], indent=4))
    for t in tests:
        for r in rescans:
            if (t.reco_algo, t.event_file.name) == (r.reco_algo, r.event_file.name):
                t.rescan_origin_id = r.scan_id
                break
        if not t.rescan_origin_id:
            raise RuntimeError(f"could not match test to rescan-test: {t}")


async def test_all(
    rc: RestClient,
    cluster: str,
    n_workers: int,
    skyscan_docker_tag: str,
    priority: int,
    tests: list[test_getter.TestParamSet],
    checker: ResultChecker,
) -> None:
    """Do all the tests."""
    # launch!
    tests = await launch_scans(  # adds scan ids to 'tests'
        tests,
        rc,
        cluster,
        n_workers,
        skyscan_docker_tag,
        priority,
    )
    with open(config.SANDBOX_MAP_FPATH, "w") as f:  # dump to file
        json.dump([t.to_json() for t in tests], f, indent=4)
    logging.info(f"scan ids: {" ".join(t.scan_id for t in tests)}")
    display_test_status(tests)

    # start test-waiters
    logging.info("Starting scan watchers...")
    tasks = set()
    for test in tests:
        tasks.add(
            asyncio.create_task(
                wait_then_check_results(rc, test, checker),
            )
        )

    # wait on all tests
    n_failed = 0
    while tasks:
        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                test = await task
                test.test_status = test_getter.TestStatus.PASSED
                logging.info(f"A test completed successfully! {test}")
            except TestException as e:
                n_failed += 1
                test = e.test
                test.test_status = test_getter.TestStatus.FAILED
                logging.error(f"A test failed: {repr(e)}")
            display_test_status(tests)

    tests_result_summary(n_failed, len(tests))


def tests_result_summary(
    n_failed: int,
    n_tests: int,
    raise_on_fail: bool = True,
) -> None:
    """How'd it all go?"""
    msg = f"{n_failed}/{n_tests} tests failed."

    if n_failed:
        logging.error(msg)
        if raise_on_fail:
            raise RuntimeError(msg)
    else:
        logging.info(msg)
        logging.info("All tests passed!")


def rotate_sandbox() -> None:
    """Tar old sandbox and prep for new."""
    if not config.SANDBOX_DIR.exists():
        raise NotADirectoryError(config.SANDBOX_DIR)

    logging.info(
        f"taring the existing '{config.SANDBOX_DIR}', then overwriting the directory"
    )

    # tar it
    with tarfile.open(
        f"{config.SANDBOX_DIR}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tar",
        "w",
    ) as tar:
        tar.add(config.SANDBOX_DIR, arcname=os.path.basename(config.SANDBOX_DIR))

    # then rm -rf the dir (saving the downloaded files)
    for entry in config.SANDBOX_DIR.iterdir():
        if entry.name in {
            "expected_results",  # dir
            "realtime_events",  # dir
            "compare_scan_results.py",  # file
            "tests.yml",  # file
        }:
            continue
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def reconstruct_tests_from_sandbox(sandbox: Path) -> list[test_getter.TestParamSet]:
    """Make test objects from the sanbox."""
    if sandbox.is_dir():
        with open(sandbox / config.SANDBOX_MAP_FPATH.name) as f:
            json_data = json.loads(f.read())
    else:
        with tarfile.open(sandbox) as tar:
            member = tar.getmember(
                f"{config.SANDBOX_DIR.name}/{config.SANDBOX_MAP_FPATH.name}"
            )
            with tar.extractfile(member) as f:  # type: ignore[union-attr]
                json_data = json.loads(f.read())

    return [
        test_getter.TestParamSet(
            Path(x["event_file"]),
            x["reco_algo"],
            Path(x["result_file"]),
            x["scan_id"],
        )
        for x in json_data
    ]


def compare_only(checker: ResultChecker, no_cache: bool) -> None:
    """Compare tests results from whatever is in the sandbox."""
    tests = reconstruct_tests_from_sandbox(config.SANDBOX_DIR)

    # re-download the test files? -- doesn't touch result files
    if no_cache:
        for t in tests:
            t.download_files(no_cache)

    # compare to expected results
    fails = []
    for t in tests:
        try:
            checker.compare_results(t)
        except TestException as e:
            fails.append(e)

    # log summary
    tests_result_summary(len(fails), len(tests), raise_on_fail=False)

    # fail-specific logging
    if fails:
        for f in fails:
            logging.error(f"{f}: {f.test}")
        tests_result_summary(len(fails), len(tests), raise_on_fail=True)


async def main():
    parser = argparse.ArgumentParser(
        description="Launch and monitor a scan for an event",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        "--cluster",
        required=True,
        help="the cluster to use for running workers. Ex: osg",
    )
    parser.add_argument(
        "--skyscan-docker-tag",
        default="latest",
        help="the skymap scanner docker tag to use",
    )
    parser.add_argument(
        "--n-workers",
        required=True,
        type=int,
        help="number of workers to request",
    )
    parser.add_argument(
        "--priority",
        default=-1,
        type=int,
        help="scan priority",
    )
    parser.add_argument(
        "--rescan",
        default=False,
        action="store_true",
        help="submit rescans for all test-scans in existing (previously ran) sandbox",
    )
    parser.add_argument(
        "--rescan-dir",
        type=Path,
        default=config.SANDBOX_DIR,
        help="the existing (previously ran) sandbox to submit rescans for",
    )
    parser.add_argument(
        "--one",
        default=False,
        action="store_true",
        help="just requests a single scan instead of the whole suite",
    )
    parser.add_argument(
        "--compare-only",
        default=False,
        action="store_true",
        help="only compare results",
    )
    parser.add_argument(
        "--repull-tests",
        required=True,
        type=wipac_dev_tools.strtobool,
        help="whether to re-download test files instead of using cached files: yes/no",
    )
    parser.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="don't send anything to skydriver",
    )
    args = parser.parse_args()
    if args.one and args.rescan:
        raise RuntimeError("cannot give --one and --rescan together")

    checker = ResultChecker(args.repull_tests)

    # --compare-only
    if args.compare_only:
        compare_only(checker, args.repull_tests)
        return

    # --rescan
    if args.rescan:
        rescans = reconstruct_tests_from_sandbox(args.rescan_dir)
    else:
        rescans = None

    # tar existing sandbox
    if config.SANDBOX_DIR.exists():
        rotate_sandbox()
    config.SANDBOX_DIR.mkdir(exist_ok=True)

    # get rest client
    rc = test_runner.get_rest_client(args.skydriver_type)

    # run tests
    tests = list(test_getter.setup_tests(no_cache=args.repull_tests))
    if args.one:
        tests = [tests[-1]]  # #0 is often millipede original (slowest), so pick faster
    if rescans:
        _match_rescans_to_tests(rescans, tests)
    if args.dry_run:
        raise DryRunException()
    await test_all(
        rc,
        args.cluster,
        args.n_workers,
        args.skyscan_docker_tag,
        args.priority,
        tests,
        checker,
    )


if __name__ == "__main__":
    raise RuntimeError("must run from pacakge: 'python -m prod_tester ...'")
