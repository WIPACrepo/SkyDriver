"""Configuration file for prod tester."""

from pathlib import Path

SANDBOX_DIR = Path("./test-suit-sandbox")

GHA_FILE_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/.github/workflows/tests.yml"

GH_URL_COMPARE_SCRIPT = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/compare_scan_results.py"

EVENT_DIR_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/realtime_events/"
RESULT_DIR_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/results_json/"
