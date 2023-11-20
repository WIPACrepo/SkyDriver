"""Config settings."""


import dataclasses as dc
import logging
import os

from wipac_dev_tools import from_environment_as_dataclass

LOGGER = logging.getLogger("clientmanager")

LOCAL_K8S_HOST = "local"

_FORWARDED_ENV_VAR_PREFIXES = ["SKYSCAN_", "EWMS_"]
_NONFORWARDED_ENV_VAR_PREFIXES = ["EWMS_TMS_"]
FORWARDED_ENV_VARS = [
    var
    for var in os.environ
    if not any(var.startswith(p) for p in _NONFORWARDED_ENV_VAR_PREFIXES)
    and any(var.startswith(p) for p in _FORWARDED_ENV_VAR_PREFIXES)
]
SECRET_FORWARDED_ENV_VARS = ["SKYSCAN_SKYDRIVER_AUTH", "SKYSCAN_BROKER_AUTH"]

WATCHER_INTERVAL = 60
WATCHER_MAX_RUNTIME = 60 * 60 * 24
WATCHER_N_TOP_TASK_ERRORS = 10


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    # pylint:disable=invalid-name
    CLIENT_STARTER_WAIT_FOR_STARTUP_JSON: int = 60
    CONDOR_TOKEN: str = ""
    #
    WORKER_K8S_TOKEN: str = ""
    WORKER_K8S_CACERT: str = ""
    WORKER_K8S_CONFIG_FILE_BASE64: str = ""
    # local k8s
    WORKER_K8S_LOCAL_APPLICATION_NAME: str = ""
    WORKER_K8S_LOCAL_WORKERS_MAX: int = 3  # don't want too many *local* workers
    #
    EWMS_PILOT_QUARANTINE_TIME: int = 0
    #
    EWMS_TMS_S3_ACCESS_KEY_ID: str = ""
    EWMS_TMS_S3_BUCKET: str = ""
    EWMS_TMS_S3_EXPIRATION: int = 60 * 60 * 24  # seconds / 1 day
    EWMS_TMS_S3_SECRET_KEY: str = ""
    EWMS_TMS_S3_URL: str = ""

    # piggy-back scanner env vars
    SKYSCAN_LOG_THIRD_PARTY: str = "WARNING"
    SKYSCAN_SKYDRIVER_ADDRESS: str = ""
    SKYSCAN_SKYDRIVER_AUTH: str = ""
    SKYSCAN_SKYDRIVER_SCAN_ID: str = ""


ENV = from_environment_as_dataclass(EnvConfig)
