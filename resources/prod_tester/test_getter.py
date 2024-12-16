import dataclasses
import itertools
import json
import os
import pickle
from pathlib import Path
from typing import Iterator

import requests
import yaml

EVENT_RESULT_MAP = {
    # these correspond to the files in https://github.com/icecube/skymap_scanner/tree/main/tests/data/results_json
    "hese_event_01.json": "run00127907.evt000020178442.HESE_1.json",
    "run00136662-evt000035405932-BRONZE.pkl": "run00136662.evt000035405932.neutrino_1.json",
    "run00136766-evt000007637140-GOLD.pkl": "run00136766.evt000007637140.neutrino_1.json",
    "138632_31747601.json": "run00138632.evt000031747601.neutrino_1.json",
}


@dataclasses.dataclass
class TestParamSet:
    """The set of parameters for a specific test."""

    event_file: Path
    reco_algo: str
    result_file: Path

    scan_id: str = ""

    @property
    def log_file(self) -> Path:
        """Based on the scan id.S"""
        if not self.scan_id:
            raise ValueError("scan_id not set")
        return Path(f"./test-suit-sandbox/logs/{self.scan_id}.log")


RECO_ALGO_KEY = "reco_algo"
EVENTFILE_KEY = "eventfile"


def fetch_file(url, mode="text"):
    """Fetch a file from a URL."""
    print(f"downloading from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    return response.text if mode == "text" else response.content


def download_file(url: str, dest: Path):
    if not os.path.exists(dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        file_content = fetch_file(url, mode="binary")
        with open(dest, "wb") as f:
            f.write(file_content)


class GHATestFetcher:
    # Constants
    GHA_FILE_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/.github/workflows/tests.yml"
    TEST_RUN_REALISTIC_JOB = "test-run-realistic"
    STRATEGY_KEY = "strategy"
    MATRIX_KEY = "matrix"
    EXCLUDE_KEY = "exclude"

    def _read_gha_matrix(self):
        yaml_content = fetch_file(self.GHA_FILE_URL)
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
        matrix_dict = self._read_gha_matrix()
        return self._expand_matrix(matrix_dict)


def setup_tests() -> Iterator[TestParamSet]:
    """Get all the files needed for running all the tests used in skymap_scanner CI.

    Yields all possible combinations of reco_algo and eventfiles from skymap_scanner tests.
    """
    matrix = GHATestFetcher().get_runtime_matrix()
    print(json.dumps(matrix, indent=4))

    # Download all the events into a local directory
    event_dir_url = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/realtime_events/"
    events_dir = Path("./test-suit-sandbox/realtime_events")
    events_dir.mkdir(exist_ok=True)
    # Download all the expected-results into a local directory
    result_dir_url = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/results_json/"
    results_dir = Path("./test-suit-sandbox/expected_results")
    results_dir.mkdir(exist_ok=True)

    for m in matrix:
        event_fname = m[EVENTFILE_KEY]

        event_file = events_dir / event_fname
        download_file(
            f"{event_dir_url}{event_fname}",
            event_file,
        )
        if event_file.suffix == ".pkl":
            with open(event_file, "rb") as f:
                contents = pickle.load(f)
            event_file.unlink()  # rm
            event_file = event_file.with_suffix(".json")  # use a different fname
            with open(event_file, "w") as f:
                json.dump(contents, f, indent=4)

        result_file = results_dir / m[RECO_ALGO_KEY] / EVENT_RESULT_MAP[event_fname]
        download_file(
            f"{result_dir_url}{m[RECO_ALGO_KEY]}/{EVENT_RESULT_MAP[event_fname]}",
            result_file,
        )

        yield TestParamSet(
            event_file=event_file,
            reco_algo=m[RECO_ALGO_KEY],
            result_file=result_file,
        )
