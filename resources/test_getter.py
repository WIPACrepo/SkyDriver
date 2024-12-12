import itertools
import json
import os
from pathlib import Path
from typing import Iterator

import requests
import yaml

RECO_ALGO_KEY = "reco_algo"
EVENTFILE_KEY = "eventfile"


def fetch_file(url, mode="text"):
    """Fetch a file from a URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text if mode == "text" else response.content


class GHATestFetcher:
    # Constants
    GHA_FILE_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/.github/workflows/tests.yml"
    TEST_RUN_REALISTIC_JOB = "test-run-realistic"
    STRATEGY_KEY = "strategy"
    MATRIX_KEY = "matrix"
    EXCLUDE_KEY = "exclude"

    def read_gha_matrix(self):
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

    def expand_matrix(self, matrix):
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

    def process(self):
        matrix_dict = self.read_gha_matrix()
        return self.expand_matrix(matrix_dict)


def setup_tests() -> Iterator[tuple[Path, str]]:
    """Get all the files needed for running all the tests used in skymap_scanner CI.

    Yields all possible combinations of reco_algo and eventfiles from skymap_scanner tests.
    """
    test_combos = GHATestFetcher().process()
    print(json.dumps(test_combos, indent=4))

    # Download all the events into a local directory
    event_dir_url = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/realtime_events/"
    download_dir = "realtime_events"
    os.makedirs(download_dir, exist_ok=True)

    for test in test_combos:
        file_name = test[EVENTFILE_KEY]
        file_path = os.path.join(download_dir, file_name)
        file_url = f"{event_dir_url}{file_name}"

        # Check if the file is already downloaded
        if os.path.exists(file_path):
            print(f"File already exists: {file_name}")
            continue

        # Fetch and save the file
        print(f"Downloading: {file_name} from {file_url}")
        try:
            file_content = fetch_file(file_url, mode="binary")
            with open(file_path, "wb") as f:
                f.write(file_content)
            print(f"Downloaded: {file_name}")
        except requests.RequestException as e:
            print(f"Failed to download {file_name}: {e}")

    for test in test_combos:
        yield Path(os.path.join(download_dir, test[EVENTFILE_KEY])), test[RECO_ALGO_KEY]
