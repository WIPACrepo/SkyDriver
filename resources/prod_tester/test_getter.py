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

from . import config

RECO_ALGO_KEY = "reco_algo"
EVENTFILE_KEY = "eventfile"


class TestStatus(enum.Enum):
    """The status of test."""

    UNKNOWN = enum.auto()
    REQUESTED = enum.auto()
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

    rescan_origin_id: str = ""  # set if the test suit is rescanning previous test-scans

    @property
    def log_file(self) -> Path:
        """Based on the scan id.S"""
        if not self.scan_id:
            raise ValueError("scan_id not set")
        return config.SANDBOX_DIR / f"logs/{self.scan_id}.log"

    def to_json(self) -> dict:
        """To a json-friendly dict."""
        return dict(
            event_file=str(self.event_file),
            reco_algo=self.reco_algo,
            result_file=str(self.result_file),
            scan_id=self.scan_id,
            rescan_origin_id=self.rescan_origin_id,
        )

    def download_files(self, no_cache: bool):
        """Download all files locally."""

        # get event file -- all event files will be saved as .json
        as_json = self.event_file.with_suffix(".json")  # file could already be *.json
        if no_cache or not as_json.exists():
            # download it
            try:
                download_file(
                    f"{config.EVENT_DIR_URL}{self.event_file.name}",
                    self.event_file,
                    no_cache,
                )
            # hm... maybe user wants to re-download and the event file was originally pkl
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    download_file(
                        f"{config.EVENT_DIR_URL}{self.event_file.with_suffix(".pkl").name}",
                        self.event_file,
                        no_cache,
                    )
                else:
                    raise
            # -> transform pkl file into json file -- skydriver only takes json
            if self.event_file.suffix == ".pkl":
                with open(self.event_file, "rb") as f:
                    contents = pickle.load(f)
                self.event_file.unlink()  # rm
                with open(as_json, "w") as f:
                    json.dump(contents, f, indent=4)
        self.event_file = as_json  # use the .json filepath

        # get the expected-result file
        try:
            url = f"{config.RESULT_DIR_URL}{self.reco_algo}/{config.EVENT_RESULT_MAP[self.event_file.name]}"
        # hm... maybe user wants to re-download and the event file was originally pkl
        except KeyError:
            url = f"{config.RESULT_DIR_URL}{self.reco_algo}/{config.EVENT_RESULT_MAP[self.event_file.with_suffix(".pkl").name]}"
        # now, download
        download_file(url, self.result_file, no_cache)


def download_file(url: str, dest: Path, force: bool) -> Path:
    """Download a file from a URL if it doesn't already exist (unless force=True)."""
    if not force and os.path.exists(dest):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading from {url}...")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    with open(dest, "wb") as f:
        f.write(response.content)
    return dest


class GHATestFetcher:
    """Class for fetching the tests from parsing the skymap scanner github actions CI."""

    # Constants
    TEST_RUN_REALISTIC_JOB = "test-run-realistic"
    STRATEGY_KEY = "strategy"
    MATRIX_KEY = "matrix"
    EXCLUDE_KEY = "exclude"

    def _read_gha_matrix(self, no_cache: bool):
        """Parse the 'matrix' defined in the github actions CI job."""
        with open(
            download_file(
                config.GHA_FILE_URL,
                config.SANDBOX_DIR / "tests.yml",
                no_cache,
            )
        ) as f:
            gha_data = yaml.safe_load(f)

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

    def get_runtime_matrix(self, no_cache: bool) -> list[dict]:
        """Get the 'matrix' defined in the github actions CI job."""
        matrix_dict = self._read_gha_matrix(no_cache)
        return self._expand_matrix(matrix_dict)


def setup_tests(no_cache: bool) -> Iterator[TestParamSet]:
    """Get all the files needed for running all the tests used in skymap_scanner CI.

    Yields all possible combinations of reco_algo and eventfiles from skymap_scanner tests.
    """
    logging.info("setting up tests...")

    matrix = GHATestFetcher().get_runtime_matrix(no_cache)
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
        event_file = events_dir / event_fname
        result_file = (
            results_dir / m[RECO_ALGO_KEY] / config.EVENT_RESULT_MAP[event_fname]
        )

        test = TestParamSet(
            event_file=event_file,
            reco_algo=m[RECO_ALGO_KEY],
            result_file=result_file,
        )
        test.download_files(no_cache)

        yield test
