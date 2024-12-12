import itertools
import json

import requests
import yaml

RECO_ALGO_KEY = "reco_algo"
EVENTFILE_KEY = "eventfile"


def fetch_file(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text


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


# Instantiate and process the matrix
test_combos = GHATestFetcher().process()

# Output the expanded matrix in JSON format
print(json.dumps(test_combos, indent=4))

# download all the events in local dir
# TODO
event_dir_url = "https://raw.githubusercontent.com/icecube/skymap_scanner/refs/heads/main/tests/data/realtime_events/"
for test in test_combos:
    # fetch file, checking if already downloaded
    file_url = f"{event_dir_url}/{test[EVENTFILE_KEY]}"
    pass
