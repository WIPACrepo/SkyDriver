"""Tools for getting/prepping tests."""

import dataclasses
import enum
import itertools
import json
import logging
import os
import pickle
from pathlib import Path
from typing import Iterator

import requests
import yaml

import config

RECO_ALGO_KEY = "reco_algo"
EVENTFILE_KEY = "eventfile"


class TestStatus(enum.Enum):
    """The status of test."""

    UNKNOWN = enum.auto()
    RUNNING = enum.auto()
    PASSED = enum.auto()
    FAILED = enum.auto()


@dataclasses.dataclass
class TestParamSet:
    """The set of parameters for a specific test."""

    event_file: Path
    reco_algo: str
    result_file: Path

    scan_id: str = ""

    test_status: TestStatus = TestStatus.UNKNOWN

    @property
    def log_file(self) -> Path:
        """Based on the scan id.S"""
        if not self.scan_id:
            raise ValueError("scan_id not set")
        return config.SANDBOX_DIR / f"logs/{self.scan_id}.log"


def fetch_file(url, mode="text"):
    """Fetch a file from a URL."""
    print(f"downloading from {url}...")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text if mode == "text" else response.content


def download_file(url: str, dest: Path, alias_dest: Path = None):
    """Download a file from a URL."""
    if os.path.exists(dest):
        return
    if alias_dest and os.path.exists(alias_dest):
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    file_content = fetch_file(url, mode="binary")
    with open(dest, "wb") as f:
        f.write(file_content)


class GHATestFetcher:
    """Class for fetching the tests from parsing the skymap scanner github actions CI."""

    # Constants
    TEST_RUN_REALISTIC_JOB = "test-run-realistic"
    STRATEGY_KEY = "strategy"
    MATRIX_KEY = "matrix"
    EXCLUDE_KEY = "exclude"

    def _read_gha_matrix(self):
        """Parse the 'matrix' defined in the github actions CI job."""
        yaml_content = fetch_file(config.GHA_FILE_URL)
        gha_data = yaml.safe_load(yaml_content)

        # Extract the matrix values for "test-run-realistic"
        test_run_realistic = gha_data.get("jobs", {}).get(
            self.TEST_RUN_REALISTIC_JOB, {}
        )
        matrix = test_run_realistic.get(self.STRATEGY_KEY, {}).get(self.MATRIX_KEY, {})

        reco_algo = matrix.get(RECO_ALGO_KEY, [])
        eventfile = matrix.get(EVENTFILE_KEY, [])
        exclude = matrix.get(self.EXCLUDE_KEY, [])

        return {
            RECO_ALGO_KEY: reco_algo,
            EVENTFILE_KEY: eventfile,
            self.EXCLUDE_KEY: exclude,
        }

    def _expand_matrix(self, matrix) -> list[dict]:
        """Permute the matrix parameters, obeying the 'exclude' field."""
        combinations = list(
            itertools.product(matrix[RECO_ALGO_KEY], matrix[EVENTFILE_KEY])
        )
        excluded = {
            (item[RECO_ALGO_KEY], item[EVENTFILE_KEY])
            for item in matrix[self.EXCLUDE_KEY]
        }

        expanded_matrix = [
            {
                RECO_ALGO_KEY: reco,
                EVENTFILE_KEY: event,
            }
            for reco, event in combinations
            if (reco, event) not in excluded
        ]

        return expanded_matrix

    def get_runtime_matrix(self) -> list[dict]:
        """Get the 'matrix' defined in the github actions CI job."""
        matrix_dict = self._read_gha_matrix()
        return self._expand_matrix(matrix_dict)


def setup_tests() -> Iterator[TestParamSet]:
    """Get all the files needed for running all the tests used in skymap_scanner CI.

    Yields all possible combinations of reco_algo and eventfiles from skymap_scanner tests.
    """
    logging.info("setting up tests...")

    matrix = GHATestFetcher().get_runtime_matrix()
    print(json.dumps(matrix, indent=4))

    # put all the events into a local directory
    events_dir = config.SANDBOX_DIR / "realtime_events"
    events_dir.mkdir(exist_ok=True)
    # put all the expected-results into a local directory
    results_dir = config.SANDBOX_DIR / "expected_results"
    results_dir.mkdir(exist_ok=True)

    # prep each test
    for m in matrix:
        event_fname = m[EVENTFILE_KEY]
        test = TestParamSet(
            event_file=events_dir / event_fname,
            reco_algo=m[RECO_ALGO_KEY],
            result_file=(
                results_dir / m[RECO_ALGO_KEY] / config.EVENT_RESULT_MAP[event_fname]
            ),
        )

        # get event file
        download_file(
            f"{config.EVENT_DIR_URL}{event_fname}",
            test.event_file,
            alias_dest=test.event_file.with_suffix(".json"),  # see below
        )
        # -> transform pkl file into json file -- skydriver only takes json
        if test.event_file.suffix == ".pkl":
            with open(test.event_file, "rb") as f:
                contents = pickle.load(f)
            test.event_file.unlink()  # rm
            test.event_file = test.event_file.with_suffix(
                ".json"
            )  # use a different fname
            with open(test.event_file, "w") as f:
                json.dump(contents, f, indent=4)

        # get the expected-result file
        download_file(
            f"{config.RESULT_DIR_URL}{m[RECO_ALGO_KEY]}/{config.EVENT_RESULT_MAP[event_fname]}",
            test.result_file,
        )

        yield test
