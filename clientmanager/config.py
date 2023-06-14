"""Config settings."""


import dataclasses as dc
import logging

from wipac_dev_tools import from_environment_as_dataclass

LOGGER = logging.getLogger("clientmanager")


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    # pylint:disable=invalid-name
    CLIENT_STARTER_WAIT_FOR_STARTUP_JSON: int = 60
    CONDOR_TOKEN: str = ""
    WORKER_K8S_TOKEN: str = ""
    EWMS_PILOT_QUARANTINE_TIME: int = 0
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
