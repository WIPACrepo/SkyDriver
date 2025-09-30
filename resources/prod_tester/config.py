"""Configuration file for prod tester."""

from pathlib import Path

SANDBOX_DIR = Path(__file__).parent / "test-suit-sandbox"
SANDBOX_MAP_FPATH = SANDBOX_DIR / "map.json"

GHA_FILE_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/.github/workflows/tests.yml"

GH_URL_COMPARE_SCRIPT = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/compare_scan_results.py"

EVENT_DIR_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/realtime_events/"
RESULT_DIR_URL = "https://raw.githubusercontent.com/icecube/skymap_scanner/main/tests/data/results_json/"


EVENT_RESULT_MAP = {
    # these correspond to the files in https://github.com/icecube/skymap_scanner/tree/main/tests/data/results_json
    "hese_event_01.json": "run00127907.evt000020178442.HESE_1.json",
    "run00136662-evt000035405932-BRONZE.pkl": "run00136662.evt000035405932.neutrino_1.json",
    "run00136766-evt000007637140-GOLD.pkl": "run00136766.evt000007637140.neutrino_1.json",
    "138632_31747601.json": "run00138632.evt000031747601.neutrino_1.json",
}
