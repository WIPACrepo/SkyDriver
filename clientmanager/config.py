"""Config settings."""


import dataclasses as dc
import logging

from wipac_dev_tools import from_environment_as_dataclass

LOGGER = logging.getLogger("clientmanager")


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    # pylint:disable=invalid-name
    SKYSCAN_LOG_THIRD_PARTY: str = "WARNING"
    EWMS_TMS_S3_URL: str = ""
    EWMS_TMS_S3_ACCESS_KEY: str = ""
    EWMS_TMS_S3_SECRET_KEY: str = ""
    SKYSCAN_SKYDRIVER_SCAN_ID: str = ""
    CONDOR_TOKEN: str = ""
    EWMS_PILOT_QUARANTINE_TIME: int = 0
    CLIENT_STARTER_WAIT_FOR_STARTUP_JSON: int = 60
    SKYSCAN_SKYDRIVER_ADDRESS: str = ""


ENV = from_environment_as_dataclass(EnvConfig)
