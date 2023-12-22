"""Util functions wrapping common htcondor actions."""


import logging

import htcondor  # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)


def get_schedd_obj(collector: str, schedd: str) -> htcondor.Schedd:
    """Get object for talking with HTCondor schedd.

    Examples:
        `collector = "foo-bar.icecube.wisc.edu"`
        `schedd = "baz.icecube.wisc.edu"`
    """
    schedd_ad = htcondor.Collector(collector).locate(  # ~> exception
        htcondor.DaemonTypes.Schedd, schedd
    )
    schedd_obj = htcondor.Schedd(schedd_ad)
    LOGGER.info(f"Connected to Schedd {collector=} {schedd=}")
    return schedd_obj


IDLE = 1
RUNNING = 2
REMOVED = 3
COMPLETED = 4
HELD = 5
TRANSFERRING_OUTPUT = 6
SUSPENDED = 7

_STATUS_MAPPING = {
    IDLE: "Idle",
    RUNNING: "Running",
    REMOVED: "Removed",
    COMPLETED: "Completed",
    HELD: "Held",
    TRANSFERRING_OUTPUT: "Transferring Output",
    SUSPENDED: "Suspended",
}


def job_status_to_str(status_code: int) -> str:
    """Get the human-readable string for the job status int."""
    return _STATUS_MAPPING.get(status_code, f"Invalid status code: {status_code}")
