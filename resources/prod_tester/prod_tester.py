"""Prod tester for SkyDriver.

Runs (or re-runs) the skymap_scanner test suite against a live SkyDriver
instance and compares the results against expected outputs.

Note: settings here (e.g. SKYSCAN_MINI_TEST, nsides={1: 0}) are for plumbing
checks, not physics. They may be unrealistic and/or change without notice.
"""

import argparse
import asyncio
import contextlib
import dataclasses
import enum
import itertools
import json
import logging
import pickle
import shutil
import subprocess
import sys
import tarfile
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import TextIO

import requests
import texttable  # type: ignore[import-untyped]
import wipac_dev_tools
import yaml
from rest_tools.client import RestClient, SavedDeviceGrantAuth

########################################################################################
# Constants
########################################################################################

SANDBOX_DIR = Path(__file__).parent / "test-suite-sandbox"
SANDBOX_MAP_FPATH = SANDBOX_DIR / "map.json"

# Sandbox layout -- single source of truth.
EVENTS_DIR = SANDBOX_DIR / "realtime_events"
EXPECTED_RESULTS_DIR = SANDBOX_DIR / "expected_results"
ACTUAL_RESULTS_DIR = SANDBOX_DIR / "actual_results"
DIFFS_DIR = SANDBOX_DIR / "result_diffs"
LOGS_DIR = SANDBOX_DIR / "logs"
COMPARE_SCRIPT_LOCAL_PATH = SANDBOX_DIR / "compare_scan_results.py"
GHA_YAML_LOCAL_PATH = SANDBOX_DIR / "tests.yml"

# These survive `rotate_sandbox()` -- they're cached upstream artifacts that
# don't change run-to-run. Anything else in the sandbox gets cleaned.
SANDBOX_KEEP_ON_ROTATE: frozenset[Path] = frozenset(
    {EVENTS_DIR, EXPECTED_RESULTS_DIR, COMPARE_SCRIPT_LOCAL_PATH, GHA_YAML_LOCAL_PATH}
)

# Upstream URLs, all rooted at one skymap_scanner ref.
SKYMAP_SCANNER_RAW_BASE = (
    "https://raw.githubusercontent.com/icecube/skymap_scanner/main"
)
GHA_FILE_URL = f"{SKYMAP_SCANNER_RAW_BASE}/.github/workflows/tests.yml"
GH_URL_COMPARE_SCRIPT = f"{SKYMAP_SCANNER_RAW_BASE}/tests/compare_scan_results.py"
EVENT_DIR_URL = f"{SKYMAP_SCANNER_RAW_BASE}/tests/data/realtime_events/"
RESULT_DIR_URL = f"{SKYMAP_SCANNER_RAW_BASE}/tests/data/results_json/"

# Maps event filename -> expected-result filename, per
# https://github.com/icecube/skymap_scanner/tree/main/tests/data/results_json
EVENT_RESULT_MAP: dict[str, str] = {
    "hese_event_01.json": "run00127907.evt000020178442.HESE_1.json",
    "run00136662-evt000035405932-BRONZE.pkl": "run00136662.evt000035405932.neutrino_1.json",
    "run00136766-evt000007637140-GOLD.pkl": "run00136766.evt000007637140.neutrino_1.json",
    "138632_31747601.json": "run00138632.evt000031747601.neutrino_1.json",
}

# SkyDriver instance routing.
SKYDRIVER_URL_SLUGS: dict[str, str] = {
    "prod": "skydriver",
    "dev": "skydriver-dev",
}
KEYCLOAK_TOKEN_URL = "https://keycloak.icecube.wisc.edu/auth/realms/IceCube"
KEYCLOAK_CLIENT_ID = "skydriver-external"

# splinempe time-bomb: while date is at-or-before the cutoff, we pass
# --compare-different-versions-ok to the comparison script. Once the cutoff
# passes, the run hard-fails so we're forced to either bump this date or
# remove the splinempe special-case entirely. See:
# https://github.com/icecube/skymap_scanner/blob/cb422e412d1607ce1e0ea2db4402a4e3461908ed/.github/workflows/tests.yml#L539-L560
SPLINEMPE_VERSION_SKEW_OK_UNTIL = date(2026, 3, 18)

SINGLE_SCAN_MONITOR_SLEEP = 60

########################################################################################
# Logging
########################################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
LOGGER = logging.getLogger("prod-tester")


########################################################################################
# Exceptions
########################################################################################


class DryRunException(Exception):
    """Raised in order to stop execution when using the dry run option."""

    def __init__(self) -> None:
        super().__init__("Dry run mode; exiting early.")


class TestException(Exception):
    """Raised for any per-test error; carries the offending TestParamSet."""

    def __init__(self, message: str, test: TestParamSet):
        super().__init__(message)
        self.test = test


class MatchFailed(RuntimeError):
    """Raised when a rescan-test cannot be matched to a planned test."""


########################################################################################
# Data model
########################################################################################


class TestStatus(enum.Enum):
    """The status of a test."""

    NOT_STARTED = enum.auto()
    REQUESTED = enum.auto()
    EWMS_IDLE = enum.auto()
    EWMS_RUNNING = enum.auto()
    DELETED = enum.auto()
    NOW_COMPLETE = enum.auto()
    PASSED = enum.auto()
    FAILED = enum.auto()


@dataclasses.dataclass
class TestParamSet:
    """The set of parameters for a specific test. Local paths are immutable post-construction."""

    event_file: Path  # always the local .json path
    reco_algo: str
    result_file: Path

    scan_id: str | None = None
    test_status: TestStatus = TestStatus.NOT_STARTED
    rescan_origin_id: str | None = None  # set when rescanning a previous test-scan

    @property
    def log_file(self) -> Path | None:
        """Per-scan monitor log path, or None if no scan has been launched yet."""
        if self.scan_id is None:
            return None
        return LOGS_DIR / f"{self.scan_id}.log"

    def to_json(self) -> dict[str, str | None]:
        """To a json-friendly dict."""
        return dict(
            event_file=str(self.event_file),
            reco_algo=self.reco_algo,
            result_file=str(self.result_file),
            scan_id=self.scan_id,
            test_status=self.test_status.name,
            rescan_origin_id=self.rescan_origin_id,
        )


########################################################################################


class SkymapScannerRepoFileFetcher:
    """Tools for fetching files from the skymap scanner repo's GHA matrix."""

    @staticmethod
    def download_file(url: str, dest: Path, force_redownload: bool) -> Path:
        """Download a file from a URL if missing (or force_redownload=True)."""
        if not force_redownload and dest.exists():
            return dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info(f"downloading from {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dest.write_bytes(response.content)
        return dest

    @staticmethod
    def fetch_event_file(local_json_path: Path, force_redownload: bool) -> None:
        """Fetch an event file to local_json_path; falls back to upstream .pkl + converts.

        SkyDriver only accepts JSON, so .pkl events are converted on disk.
        """
        if not force_redownload and local_json_path.exists():
            return

        local_json_path.parent.mkdir(parents=True, exist_ok=True)
        json_name = local_json_path.name

        # Try .json upstream first
        try:
            SkymapScannerRepoFileFetcher.download_file(
                f"{EVENT_DIR_URL}{json_name}", local_json_path, force_redownload
            )
            return
        except requests.exceptions.HTTPError as e:
            # only fall back on 404; any other HTTP error is a real error
            if e.response.status_code != 404:
                raise

        # Fallback: try .pkl upstream, then convert -> .json
        pkl_path = local_json_path.with_suffix(".pkl")
        SkymapScannerRepoFileFetcher.download_file(
            f"{EVENT_DIR_URL}{pkl_path.name}", pkl_path, force_redownload
        )
        with open(pkl_path, "rb") as f:
            contents = pickle.load(f)
        pkl_path.unlink()
        local_json_path.write_text(json.dumps(contents, indent=4))

    @staticmethod
    def fetch_result_file(
        local_path: Path,
        event_filename: str,
        reco_algo: str,
        force_redownload: bool,
    ) -> None:
        """Fetch the expected-result file for an (event, reco_algo) pair."""
        # event_filename may be either suffix; EVENT_RESULT_MAP could be keyed on .json or .pkl
        try:
            result_filename = EVENT_RESULT_MAP[event_filename]
        except KeyError:
            # hm... maybe user wants to re-download and the event file was originally pkl
            result_filename = EVENT_RESULT_MAP[
                Path(event_filename).with_suffix(".pkl").name
            ]
        SkymapScannerRepoFileFetcher.download_file(
            f"{RESULT_DIR_URL}{reco_algo}/{result_filename}",
            local_path,
            force_redownload,
        )

    @staticmethod
    def ensure_compare_script(force_redownload: bool) -> Path:
        """Make sure the upstream compare script is on disk; return its path."""
        SkymapScannerRepoFileFetcher.download_file(
            GH_URL_COMPARE_SCRIPT, COMPARE_SCRIPT_LOCAL_PATH, force_redownload
        )
        return COMPARE_SCRIPT_LOCAL_PATH


########################################################################################


class SkymapScannerRepoTestToolset:
    """Tools for parsing the skymap scanner repo's GHA matrix."""

    # GHA matrix keys -- part of the upstream contract with skymap_scanner CI.
    RECO_ALGO_KEY = "reco_algo"
    EVENTFILE_KEY = "eventfile"

    @staticmethod
    def _read_gha_matrix(force_redownload: bool) -> dict:
        """Parse the matrix defined in the skymap_scanner test-run-realistic GHA job."""
        yaml_path = SkymapScannerRepoFileFetcher.download_file(
            GHA_FILE_URL, GHA_YAML_LOCAL_PATH, force_redownload
        )
        with open(yaml_path) as f:
            gha_data = yaml.safe_load(f)

        # Extract the matrix values for "test-run-realistic"
        job = gha_data.get("jobs", {}).get("test-run-realistic", {})
        matrix = job.get("strategy", {}).get("matrix", {})
        return {
            SkymapScannerRepoTestToolset.RECO_ALGO_KEY: matrix.get(
                SkymapScannerRepoTestToolset.RECO_ALGO_KEY, []
            ),
            SkymapScannerRepoTestToolset.EVENTFILE_KEY: matrix.get(
                SkymapScannerRepoTestToolset.EVENTFILE_KEY, []
            ),
            "exclude": matrix.get("exclude", []),
        }

    @staticmethod
    def _expand_matrix(matrix: dict) -> list[dict]:
        """Permute matrix parameters, honoring the 'exclude' field."""
        excluded = {
            (
                item[SkymapScannerRepoTestToolset.RECO_ALGO_KEY],
                item[SkymapScannerRepoTestToolset.EVENTFILE_KEY],
            )
            for item in matrix["exclude"]
        }
        return [
            {
                SkymapScannerRepoTestToolset.RECO_ALGO_KEY: reco,
                SkymapScannerRepoTestToolset.EVENTFILE_KEY: event,
            }
            for reco, event in itertools.product(
                matrix[SkymapScannerRepoTestToolset.RECO_ALGO_KEY],
                matrix[SkymapScannerRepoTestToolset.EVENTFILE_KEY],
            )
            if (reco, event) not in excluded
        ]

    @staticmethod
    def fetch_tests(force_redownload: bool) -> Iterator[TestParamSet]:
        """Yield TestParamSets for every (reco_algo, event) pair from the upstream GHA matrix."""
        LOGGER.info("setting up tests...")
        matrix = SkymapScannerRepoTestToolset._expand_matrix(
            SkymapScannerRepoTestToolset._read_gha_matrix(force_redownload)
        )
        LOGGER.info(json.dumps(matrix, indent=4))

        # put all the events into a local directory
        EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        # put all the expected-results into a local directory
        EXPECTED_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

        # prep each test
        for m in matrix:
            upstream_filename = m[SkymapScannerRepoTestToolset.EVENTFILE_KEY]
            reco_algo = m[SkymapScannerRepoTestToolset.RECO_ALGO_KEY]

            # Local event file is always .json (regardless of upstream suffix)
            event_file = EVENTS_DIR / Path(upstream_filename).with_suffix(".json").name
            result_file = (
                EXPECTED_RESULTS_DIR / reco_algo / EVENT_RESULT_MAP[upstream_filename]
            )

            SkymapScannerRepoFileFetcher.fetch_event_file(event_file, force_redownload)
            SkymapScannerRepoFileFetcher.fetch_result_file(
                result_file, upstream_filename, reco_algo, force_redownload
            )

            yield TestParamSet(
                event_file=event_file, reco_algo=reco_algo, result_file=result_file
            )

    @staticmethod
    def refetch_test_files(test: TestParamSet, force_redownload: bool) -> None:
        """Re-download event + expected-result files for an already-constructed test."""
        SkymapScannerRepoFileFetcher.fetch_event_file(test.event_file, force_redownload)
        SkymapScannerRepoFileFetcher.fetch_result_file(
            test.result_file, test.event_file.name, test.reco_algo, force_redownload
        )

    @staticmethod
    def match_rescans_to_tests(
        rescans: list[TestParamSet], tests: list[TestParamSet]
    ) -> None:
        """Match rescans to tests, in order to send the rescan id to skydriver."""
        LOGGER.info("matching tests to rescan-tests")
        LOGGER.info(json.dumps([r.to_json() for r in rescans], indent=4))
        for t in tests:
            for r in rescans:
                if (t.reco_algo, t.event_file.name) == (r.reco_algo, r.event_file.name):
                    t.rescan_origin_id = r.scan_id
                    break
            if t.rescan_origin_id is None:
                raise MatchFailed(f"could not match test to rescan-test: {t}")


########################################################################################


def get_rest_client(skydriver_type: str, retries: int = 3) -> RestClient:
    """Return a REST client for SkyDriver.

    Presents a QR code in the terminal for initial validation. For non-interactive
    use (e.g. cron), run this manually first to populate the saved token file.
    """
    if skydriver_type not in SKYDRIVER_URL_SLUGS:
        raise ValueError(f"Unknown skydriver type: {skydriver_type}")

    name = SKYDRIVER_URL_SLUGS[skydriver_type]
    url = f"https://{name}.icecube.aq"
    LOGGER.info(f"connecting to {url}...")

    return SavedDeviceGrantAuth(
        url,
        token_url=KEYCLOAK_TOKEN_URL,
        filename=str(Path(f"~/device-refresh-token-{name}").expanduser().resolve()),
        client_id=KEYCLOAK_CLIENT_ID,
        retries=retries,
    )


########################################################################################


class SingleScanToolbox:
    """Tools for launching and monitoring a SkyDriver scan."""

    @staticmethod
    async def rescan_a_scan(rc: RestClient, rescan_origin_id: str) -> dict:
        """Request SkyDriver to rescan a prior scan."""
        manifest = await rc.request("POST", f"/scan/{rescan_origin_id}/actions/rescan")
        return manifest  # type: ignore[no-any-return]

    @staticmethod
    async def launch_a_scan(
        rc: RestClient,
        event_file: Path,
        cluster: str,
        n_workers: int,
        reco_algo: str,
        skyscan_docker_tag: str,
        priority: int,
    ) -> dict:
        """Request SkyDriver to scan an event."""
        body = {
            "reco_algo": reco_algo,
            "event_i3live_json": event_file.read_text().strip(),
            "nsides": {1: 0},  # {8: 0, 64: 12, 512: 24},
            "real_or_simulated_event": "real",
            "predictive_scanning_threshold": 1,  # 0.3,
            "cluster": {cluster: n_workers},
            "docker_tag": skyscan_docker_tag,
            "max_pixel_reco_time": 30 * 60,  # seconds
            "scanner_server_memory": "1G",
            "priority": priority,
            "scanner_server_env": {
                "SKYSCAN_MINI_TEST": True,
                "_SKYSCAN_CI_MINI_TEST": True,  # env var changed to this in the "skydriver 2"-ready scanner
            },
            "classifiers": {"_TEST": True},
        }
        manifest = await rc.request("POST", "/scan", body)
        return manifest  # type: ignore[no-any-return]

    @staticmethod
    async def monitor(rc: RestClient, test: TestParamSet) -> dict:  # noqa: C901, PLR0915
        """Monitor an event scan until done; return the result."""
        if test.log_file is not None:
            out_cm: contextlib.AbstractContextManager = open(test.log_file, "w")
        else:
            out_cm = contextlib.nullcontext(sys.stdout)

        with out_cm as out:
            printer = Printer(out)
            printer.print_now("MANIFEST:")
            manifest = await rc.request(
                "GET",
                f"/scan/{test.scan_id}/manifest",
                {"include_deleted": True},
            )
            printer.print_now(manifest)

            # stash previous object for detecting changes
            prev_status: dict = {}
            prev_progress: dict = {}
            prev_result: dict = {}

            # loop w/ sleep
            done = False
            while not done:
                printer.print_now("-" * 60)

                # get status
                try:
                    printer.print_now("STATUS:")
                    status = await rc.request(
                        "GET",
                        f"/scan/{test.scan_id}/status",
                        {"include_deleted": True},
                    )
                    if prev_status != status:
                        printer.print_now(status)
                        prev_status = status
                    if status["ewms_workforce"]["workflow_id"] != "not-yet-requested":  # fmt:skip
                        if status["ewms_workforce"]["n_running"]:
                            test.test_status = TestStatus.EWMS_RUNNING
                        else:
                            test.test_status = TestStatus.EWMS_IDLE
                    else:
                        printer.print_now("<no change in status>")
                    # loop control -- finish this iteration, then break
                    if status["scan_complete"]:
                        test.test_status = TestStatus.NOW_COMPLETE
                        done = True
                    elif status["is_deleted"]:
                        test.test_status = TestStatus.DELETED
                        done = True
                except Exception as e:  # 404 (scanner not yet online)
                    printer.print_now(f"suppressed error: {repr(e)}")

                # get manifest.progress
                try:
                    printer.print_now("MANIFEST.PROGRESS:")
                    progress = (
                        await rc.request(
                            "GET",
                            f"/scan/{test.scan_id}/manifest",
                            {"include_deleted": True},
                        )
                    )["progress"]
                    if prev_progress != progress:
                        printer.print_now(progress)
                        prev_progress = progress
                    else:
                        printer.print_now("<no change in manifest.progress>")
                except Exception as e:
                    # 404 (scanner not yet online) or KeyError (no progress yet)
                    printer.print_now(f"suppressed error: {repr(e)}")

                # get result
                try:
                    printer.print_now("RESULT:")
                    result = await rc.request(
                        "GET",
                        f"/scan/{test.scan_id}/result",
                        {"include_deleted": True},
                    )
                    if prev_result != result:
                        printer.print_now(result)
                        prev_result = result
                    else:
                        printer.print_now("<no change in result>")
                except Exception as e:
                    printer.print_now(f"suppressed error: {repr(e)}")

                # done? else, wait
                if not done:
                    printer.print_now(test.scan_id)
                    await asyncio.sleep(SINGLE_SCAN_MONITOR_SLEEP)

            printer.print_now("scan is done!")
            printer.print_now(test.scan_id)
            printer.print_now("RESULT.SKYSCAN_RESULT:")
            return (
                await rc.request(
                    "GET",
                    f"/scan/{test.scan_id}/result",
                    {"include_deleted": True},
                )
            )["skyscan_result"]


########################################################################################


class ResultChecker:
    """Compares scan results against expected outputs via the upstream compare script."""

    def __init__(self, compare_script_fpath: Path):
        self.compare_script_fpath = compare_script_fpath

    def compare_results(
        self,
        test: TestParamSet,
        scan_result: dict | None = None,
    ) -> None:
        """Compare a scan result against the expected; raise TestException on mismatch."""
        LOGGER.info(
            f"Comparing scan result to expected values: {test.reco_algo} + {test.event_file}"
        )

        scan_result_file = (
            ACTUAL_RESULTS_DIR / f"{test.reco_algo}-{test.event_file.name}.json"
        )
        scan_result_file.parent.mkdir(parents=True, exist_ok=True)

        # if a fresh result was provided, persist it; else expect it on disk
        if scan_result is None:
            if not scan_result_file.exists():
                raise FileNotFoundError(scan_result_file)
        else:
            with open(scan_result_file, "w") as f:
                json.dump(scan_result, f)

        DIFFS_DIR.mkdir(parents=True, exist_ok=True)

        # splinempe currently allows a version-skew-OK comparison; this is time-bombed
        extra_args: list[str] = []
        if test.reco_algo == "splinempe":
            LOGGER.warning(
                "Using the flag '--compare-different-versions-ok' for splinempe "
                "test results. This may be outdated, but should not cause broad issues. "
                "See https://github.com/icecube/skymap_scanner/blob/cb422e412d1607ce1e0ea2db4402a4e3461908ed/.github/workflows/tests.yml#L539-L560."
            )
            extra_args = ["--compare-different-versions-ok"]

        result = subprocess.run(
            [
                sys.executable,
                str(self.compare_script_fpath),
                "--actual",
                str(scan_result_file),
                "--expected",
                str(test.result_file),
                "--diff-out-dir",
                str(DIFFS_DIR),
                "--assert",
                *extra_args,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            LOGGER.info("> PASSED: Results for scan match expected output.")
        else:
            LOGGER.error("> FAILED: Mismatch in results...")
            LOGGER.error(result.stderr)
            raise TestException("Mismatch in results", test)


########################################################################################


class SandboxManager:
    """Tools for interacting with the sandbox directory."""

    @staticmethod
    def rotate_sandbox() -> None:
        """Tar the existing sandbox, then clean it (preserving cached upstream artifacts)."""
        if not SANDBOX_DIR.exists():
            raise NotADirectoryError(SANDBOX_DIR)

        LOGGER.info(f"taring '{SANDBOX_DIR}', then overwriting the directory")

        # tar it
        tar_path = (
            SANDBOX_DIR.parent
            / f"{SANDBOX_DIR.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tar"
        )
        with tarfile.open(tar_path, "w") as tar:
            tar.add(SANDBOX_DIR, arcname=SANDBOX_DIR.name)

        # then rm -rf the dir (saving the downloaded files)
        keep_names = {p.name for p in SANDBOX_KEEP_ON_ROTATE}
        for entry in SANDBOX_DIR.iterdir():
            if entry.name in keep_names:
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()

    @staticmethod
    def reconstruct_tests_from_sandbox(sandbox: Path) -> list[TestParamSet]:
        """Make test objects from the sandbox dir or its tarball."""
        if sandbox.is_dir():
            with open(sandbox / SANDBOX_MAP_FPATH.name) as f:
                json_data = json.load(f)
        else:
            with tarfile.open(sandbox) as tar:
                member = tar.getmember(f"{SANDBOX_DIR.name}/{SANDBOX_MAP_FPATH.name}")
                extracted = tar.extractfile(member)
                if extracted is None:
                    raise RuntimeError(
                        f"could not extract {member.name} from {sandbox}"
                    )
                with extracted as f:
                    json_data = json.load(f)

        return [
            TestParamSet(
                event_file=Path(x["event_file"]),
                reco_algo=x["reco_algo"],
                result_file=Path(x["result_file"]),
                # legacy sandboxes used "" as a sentinel; treat as None
                scan_id=x.get("scan_id") or None,
                rescan_origin_id=x.get("rescan_origin_id") or None,
            )
            for x in json_data
        ]

    @staticmethod
    def compare_only(checker: ResultChecker, force_redownload: bool) -> tuple[int, int]:
        """Compare results from whatever is already in the sandbox; do not run scans."""
        tests = SandboxManager.reconstruct_tests_from_sandbox(SANDBOX_DIR)

        # re-download the test files? -- doesn't touch actual scan-result files
        for t in tests:
            SkymapScannerRepoTestToolset.refetch_test_files(t, force_redownload)

        # compare to expected results
        fails: list[TestException] = []
        for t in tests:
            try:
                checker.compare_results(t)
            except TestException as e:
                fails.append(e)

        # fail-specific logging
        if fails:
            for f in fails:
                LOGGER.error(f"{f}: {f.test}")

        return len(fails), len(tests)

    @staticmethod
    def persist_test_map(tests: list[TestParamSet]) -> None:
        """Write the current test list to the sandbox map file (called incrementally)."""
        SANDBOX_MAP_FPATH.parent.mkdir(parents=True, exist_ok=True)
        with open(SANDBOX_MAP_FPATH, "w") as f:
            json.dump([t.to_json() for t in tests], f, indent=4)


########################################################################################


class Printer:
    """Printing tools."""

    def __init__(self, file: TextIO = sys.stdout):
        self.file = file

    def print_now(self, thing: str | list | dict | None) -> None:
        """Print a thing (with flushing) to the terminal, JSON-compatible."""
        if not isinstance(thing, str):
            print(json.dumps(thing, indent=4), file=self.file, flush=True)
        else:
            print(thing, file=self.file, flush=True)

    def display_test_status(self, tests: list[TestParamSet]) -> None:
        """Display test statuses in a clean table format."""
        self.print_now(f"Scan IDs: {' '.join(t.scan_id or '-' for t in tests)}")

        sorted_tests = sorted(
            enumerate(tests, start=1),
            key=lambda x: (x[1].test_status.name, x[0]),
        )
        table = texttable.Texttable()

        scan_id_len = 10

        # columns
        table.add_row(["#", "Event File", "Reco Algo", "Scan ID", "Status"])
        table.set_cols_align(["r", "l", "l", "r", "l"])
        table.set_cols_width([2, 25, 18, scan_id_len, 12])
        table.set_cols_dtype(["i", "t", "t", "t", "t"])

        # Add rows for each test
        for i, test in sorted_tests:
            # truncate scan_id for display; "N/A" for not-yet-launched
            if test.scan_id:
                scan_id_disp = test.scan_id[:scan_id_len]
            else:
                scan_id_disp = "N/A"
            table.add_row(
                [
                    i,
                    test.event_file.name,
                    test.reco_algo,
                    scan_id_disp,
                    test.test_status.name,
                ]
            )

        self.print_now(table.draw())


########################################################################################


class EndGame:
    """Handles how the testing suite ends."""

    @staticmethod
    def tests_result_summary(n_failed: int, n_tests: int) -> None:
        """Log a final summary; raise if any tests failed."""
        msg = f"tests: total={n_tests}, {n_failed=}, n_passed={n_tests - n_failed}"
        if n_failed:
            LOGGER.error(msg)
            raise RuntimeError(msg)
        else:
            LOGGER.info(msg)
            LOGGER.info("All tests passed!")


########################################################################################


class FullTestSuite:
    """Orchestrates the testing suite."""

    @staticmethod
    async def launch_scans(
        tests: list[TestParamSet],
        rc: RestClient,
        cluster: str,
        n_workers: int,
        skyscan_docker_tag: str,
        priority: int,
    ) -> None:
        """Launch all scans in-place; persist after each so partial failures don't lose track."""
        for i, test in enumerate(tests):
            LOGGER.info(
                f"Launching test {i + 1}/{len(tests)}: {test.reco_algo} + {test.event_file}"
            )
            test.test_status = TestStatus.REQUESTED
            try:
                # rescan?
                if test.rescan_origin_id is not None:
                    manifest = await SingleScanToolbox.rescan_a_scan(
                        rc, test.rescan_origin_id
                    )
                    test.scan_id = manifest["scan_id"]
                    # rescan must produce a new scan id; otherwise SkyDriver misbehaved
                    if test.scan_id == test.rescan_origin_id:
                        raise RuntimeError(
                            f"rescan returned the origin id: {test.scan_id}"
                        )
                # or normal scan?
                else:
                    manifest = await SingleScanToolbox.launch_a_scan(
                        rc,
                        test.event_file,
                        cluster,
                        n_workers,
                        test.reco_algo,
                        skyscan_docker_tag,
                        priority,
                    )
                    test.scan_id = manifest["scan_id"]
                LOGGER.info(f"launched scan_id={test.scan_id}")
            except Exception:
                # persist whatever we have so we don't lose track of in-flight scans
                LOGGER.error(
                    f"Failed to launch test #{i + 1}; persisting partial state"
                )
                SandboxManager.persist_test_map(tests)
                raise
            else:
                SandboxManager.persist_test_map(tests)

    @staticmethod
    async def wait_then_check_results(
        rc: RestClient,
        test: TestParamSet,
        checker: ResultChecker,
    ) -> TestParamSet:
        """Wait until the scan is done, then check its result."""
        # explicit guard: monitor + log_file require a scan_id
        if test.scan_id is None or test.log_file is None:
            raise TestException("scan_id not set; cannot monitor", test)

        try:
            LOGGER.info(
                f"Monitoring scan; see logs in {test.log_file}: {test.reco_algo} + {test.event_file}"
            )
            test.log_file.parent.mkdir(parents=True, exist_ok=True)

            try:
                # Wait until the scan is done...
                scan_result = await SingleScanToolbox.monitor(rc, test)
                LOGGER.info(f"Scan {test.scan_id} completed successfully.")
            except Exception as e:
                LOGGER.error(f"Error monitoring scan {test.scan_id}: {e}")
                raise
            else:
                # then check its result
                checker.compare_results(test, scan_result)
        except Exception as e:
            # no error in testing shall bring down the test suite
            raise TestException(repr(e), test) from e

        return test

    @staticmethod
    async def print_updates(tests: list[TestParamSet], printer: Printer) -> None:
        """Print test updates to the terminal."""
        stored_hash = 0
        while True:
            printer.print_now("TEST STATUSES:")
            new_hash = sum([hash(str(t.to_json())) for t in tests])
            if new_hash != stored_hash:
                stored_hash = new_hash
                printer.display_test_status(tests)
            else:
                printer.print_now("<no changes in test statuses>")
            await asyncio.sleep(SINGLE_SCAN_MONITOR_SLEEP / 2)

    @staticmethod
    async def test_all(
        rc: RestClient,
        cluster: str,
        n_workers: int,
        skyscan_docker_tag: str,
        priority: int,
        tests: list[TestParamSet],
        checker: ResultChecker,
    ) -> tuple[int, int]:
        """Do all the tests."""
        # launch! (mutates each test's scan_id and persists incrementally)
        await FullTestSuite.launch_scans(
            tests, rc, cluster, n_workers, skyscan_docker_tag, priority
        )
        printer = Printer()
        printer.display_test_status(tests)  # do now in case of errors, reprinted later

        # start test-waiters -- in background
        LOGGER.info("Starting scan watchers...")
        scan_tasks = {
            asyncio.create_task(FullTestSuite.wait_then_check_results(rc, t, checker))
            for t in tests
        }

        # start table printer watcher
        update_task = asyncio.create_task(FullTestSuite.print_updates(tests, printer))

        # wait on all tests
        n_failed = 0
        while scan_tasks:
            done, scan_tasks = await asyncio.wait(
                scan_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    test = await task
                    test.test_status = TestStatus.PASSED
                    LOGGER.info(f"A test completed successfully! {test}")
                except TestException as e:
                    n_failed += 1
                    e.test.test_status = TestStatus.FAILED
                    LOGGER.error(f"A test failed: {repr(e)}")

        update_task.cancel()
        printer.display_test_status(tests)  # one final print
        return n_failed, len(tests)


########################################################################################


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Launch and monitor SkyDriver scans for the skymap_scanner test suite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=list(SKYDRIVER_URL_SLUGS.keys()),
        help=(
            "the type of the SkyDriver instance for REST API URL "
            "(prod -> https://skydriver.icecube.aq; dev -> https://skydriver-dev.icecube.aq)"
        ),
    )
    parser.add_argument(
        "--cluster",
        required=True,
        help="the cluster to use for running workers (e.g., osg)",
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
        nargs="?",
        type=Path,
        const=SANDBOX_DIR,  # value when --rescan is given without an arg
        default=None,  # value when --rescan is omitted
        help="rescan all test-scans in an existing sandbox dir/tar (defaults to current sandbox)",
    )
    parser.add_argument(
        "--one",
        default=False,
        action="store_true",
        help="just request a single scan instead of the whole suite",
    )
    parser.add_argument(
        "--compare-only",
        default=False,
        action="store_true",
        help="only compare results of the most recent test scans -- do NOT run tests",
    )
    parser.add_argument(
        "--repull-tests",
        required=True,
        type=wipac_dev_tools.strtobool,
        help="force re-download of upstream test files instead of using cache",
    )
    parser.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="don't send anything to skydriver",
    )
    return parser


async def main() -> None:
    """Entrypoint: parse args, set up sandbox, and run."""
    args = _build_parser().parse_args()

    if args.one and args.rescan is not None:
        raise RuntimeError("cannot give --one and --rescan together")

    compare_script = SkymapScannerRepoFileFetcher.ensure_compare_script(
        args.repull_tests
    )
    checker = ResultChecker(compare_script)

    # --compare-only short-circuits everything else
    if args.compare_only:
        n_failed, n_tests = SandboxManager.compare_only(checker, args.repull_tests)
        EndGame.tests_result_summary(n_failed, n_tests)  # ~> raises if any tests failed
        return

    # --rescan
    rescans: list[TestParamSet] | None = None
    if args.rescan is not None:
        rescans = SandboxManager.reconstruct_tests_from_sandbox(args.rescan)

    # tar existing sandbox
    if SANDBOX_DIR.exists():
        SandboxManager.rotate_sandbox()
    SANDBOX_DIR.mkdir(exist_ok=True)

    # get rest client
    rc = get_rest_client(args.skydriver_type)

    # run tests
    tests = list(
        SkymapScannerRepoTestToolset.fetch_tests(force_redownload=args.repull_tests)
    )
    if args.one:
        # #0 is often millipede original (slowest), so pick faster
        tests = [tests[-1]]
    if rescans is not None:
        SkymapScannerRepoTestToolset.match_rescans_to_tests(rescans, tests)
    if args.dry_run:
        raise DryRunException()

    # now, really run tests
    n_failed, n_tests = await FullTestSuite.test_all(
        rc,
        args.cluster,
        args.n_workers,
        args.skyscan_docker_tag,
        args.priority,
        tests,
        checker,
    )
    EndGame.tests_result_summary(n_failed, n_tests)  # ~> raises if any tests failed


if __name__ == "__main__":
    asyncio.run(main())
