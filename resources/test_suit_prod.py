import logging
import os
import subprocess
from pathlib import Path

import test_getter

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants
SCRIPT_PATH = "test_runner.py"  # Path to your script
LOG_DIR = "subproc_logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Cluster and other shared settings for the script
DEFAULT_CLUSTER = "sub-2"
DEFAULT_WORKERS = 2
DEFAULT_MAX_PIXEL_RECO_TIME = 300
DEFAULT_MEMORY = "512M"
SKYDRIVER_URL = "https://your-skydriver-url.example.com"  # Replace with actual URL

# Track subprocess results
success_scan_ids = []
failed_combos = []

# Run subprocesses for each test combination
processes = []
for i, (event_file, reco_algo) in enumerate(test_getter.setup_tests()):
    stdout_log = Path(LOG_DIR) / f"subproc_{i}_stdout.log"
    stderr_log = Path(LOG_DIR) / f"subproc_{i}_stderr.log"

    cmd = [
        "python",
        SCRIPT_PATH,
        "--event-file",
        event_file,
        "--skydriver-url",
        SKYDRIVER_URL,
        "--cluster",
        DEFAULT_CLUSTER,
        "--n-workers",
        str(DEFAULT_WORKERS),
        "--max-pixel-reco-time",
        str(DEFAULT_MAX_PIXEL_RECO_TIME),
        "--reco-algo",
        reco_algo,
        "--scanner-server-memory",
        DEFAULT_MEMORY,
    ]

    logging.info(f"Launching subprocess {i} for {test}")
    with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
        proc = subprocess.Popen(cmd, stdout=out, stderr=err)
        processes.append((proc, test, stdout_log, stderr_log))

# Monitor subprocesses
for proc, test, stdout_log, stderr_log in processes:
    proc.wait()  # Wait for the subprocess to finish
    if proc.returncode == 0:
        logging.info(
            f"Subprocess succeeded for {test} (logs: {stdout_log}, {stderr_log})"
        )
        with open(stdout_log, "r") as log:
            for line in log:
                if "scan_id" in line:  # Parse scan_id from logs
                    success_scan_ids.append(line.strip())
    else:
        logging.error(
            f"Subprocess failed for {test} (logs: {stdout_log}, {stderr_log})"
        )
        failed_combos.append(test)

# Final results
if success_scan_ids:
    logging.info(f"Successfully completed scans with scan_ids: {success_scan_ids}")
else:
    logging.error("All subprocesses failed. No successful scan IDs.")

if failed_combos:
    logging.warning(f"Failed combinations: {failed_combos}")
else:
    logging.info("No failed subprocesses.")
