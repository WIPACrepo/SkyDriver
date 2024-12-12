import asyncio
import logging
import os
from asyncio.subprocess import PIPE
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


# Async function to run a single test
async def run_test(event_file, reco_algo, task_id):
    stdout_log = Path(LOG_DIR) / f"task_{task_id}_stdout.log"
    stderr_log = Path(LOG_DIR) / f"task_{task_id}_stderr.log"

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

    logging.info(
        f"Task {task_id}: Starting test for event {event_file} with reco_algo {reco_algo}"
    )

    # Open log files for writing
    async with stdout_log.open("w") as out, stderr_log.open("w") as err:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=PIPE,
            stderr=PIPE,
        )
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            out.write(line.decode())
            out.flush()

        stderr = await process.stderr.read()
        err.write(stderr.decode())
        err.flush()

        returncode = await process.wait()

        if returncode == 0:
            logging.info(
                f"Task {task_id}: Completed successfully (logs: {stdout_log}, {stderr_log})"
            )
            # Parse `scan_id` from stdout logs
            with stdout_log.open("r") as log:
                for line in log:
                    if "scan_id" in line:
                        return line.strip()
        else:
            logging.error(f"Task {task_id}: Failed (logs: {stdout_log}, {stderr_log})")
            return None


# Main coroutine to manage tasks using TaskGroup
async def main():
    test_combos = list(test_getter.setup_tests())

    success_scan_ids = []
    failed_tasks = []

    async with asyncio.TaskGroup() as tg:
        task_map = {}
        for task_id, (event_file, reco_algo) in enumerate(test_combos):
            task_map[tg.create_task(run_test(event_file, reco_algo, task_id))] = (
                event_file,
                reco_algo,
            )

        # Await results collectively
        for task, (event_file, reco_algo) in task_map.items():
            if task.exception():
                logging.error(
                    f"Task for {event_file} and {reco_algo} raised an exception: {task.exception()}"
                )
                failed_tasks.append((event_file, reco_algo))
            elif (result := task.result()) is not None:
                success_scan_ids.append(result)
            else:
                failed_tasks.append((event_file, reco_algo))

    # Final Results
    if success_scan_ids:
        logging.info(f"Successfully completed scans with scan_ids: {success_scan_ids}")
    else:
        logging.error("All tasks failed. No successful scan IDs.")

    if failed_tasks:
        logging.warning(f"Failed tasks: {failed_tasks}")
    else:
        logging.info("No failed tasks.")


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
