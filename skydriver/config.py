"""Config settings."""

import dataclasses as dc
import logging

import coloredlogs  # type: ignore[import]
from wipac_dev_tools import from_environment_as_dataclass

LOGGER = logging.getLogger("skydriver")

# --------------------------------------------------------------------------------------
# Constants


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    # pylint:disable=invalid-name
    AUTH_AUDIENCE: str = "skydriver"
    AUTH_OPENID_URL: str = ""
    MONGODB_AUTH_PASS: str = ""  # empty means no authentication required
    MONGODB_AUTH_USER: str = ""  # None means required to specify
    MONGODB_HOST: str = "localhost"
    MONGODB_PORT: int = 27017
    REST_HOST: str = "localhost"
    REST_PORT: int = 8080
    CI_TEST: bool = False
    LOG_LEVEL: str = "DEBUG"
    K8S_TTL_SECONDS_AFTER_FINISHED: int = 600
    # skyscan (meta)
    SKYSCAN_DOCKER_IMAGE_NO_TAG: str = "icecube/skymap_scanner"
    SKYSCAN_SINGULARITY_IMAGE_PATH_NO_TAG: str = (
        "/cvmfs/icecube.opensciencegrid.org/containers/realtime/skymap_scanner"
    )
    # skyscan (forwarded)
    SKYSCAN_BROKER_ADDRESS: str = "localhost"
    SKYSCAN_BROKER_AUTH: str = ""
    SKYSCAN_SKYDRIVER_AUTH: str = ""
    SKYSCAN_PROGRESS_INTERVAL_SEC: int | None = None
    SKYSCAN_RESULT_INTERVAL_SEC: int | None = None
    SKYSCAN_MQ_TIMEOUT_TO_CLIENTS: int | None = None
    SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS: int | None = None
    SKYSCAN_LOG: str | None = None
    SKYSCAN_LOG_THIRD_PARTY: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "LOG_LEVEL", self.LOG_LEVEL.upper())  # b/c frozen
        if (
            self.SKYSCAN_DOCKER_IMAGE_NO_TAG.split("/")[-1]
            != self.SKYSCAN_SINGULARITY_IMAGE_PATH_NO_TAG.split("/")[-1]
        ):
            raise RuntimeError(
                f"Image Mismatch: "
                f"'SKYSCAN_DOCKER_IMAGE_NO_TAG' ({self.SKYSCAN_DOCKER_IMAGE_NO_TAG}) and "
                f"'SKYSCAN_SINGULARITY_IMAGE_PATH_NO_TAG' ({self.SKYSCAN_SINGULARITY_IMAGE_PATH_NO_TAG}) "
                f"do not reference the same image"
            )


ENV = from_environment_as_dataclass(EnvConfig)


USER_ACCT = "skydriver-service-account"
SKYMAP_SCANNER_ACCT = "skymap-scanner-service-account"


def is_testing() -> bool:
    """Return true if this is the test environment.

    Note: this needs to run on import.
    """
    return ENV.CI_TEST


def config_logging(level: str) -> None:
    """Configure the logging level and format.

    This is separated into a function for consistency between app and
    testing environments.
    """
    coloredlogs.install(
        fmt="%(asctime)s.%(msecs)03d [%(levelname)8s] %(hostname)s %(name)s[%(process)d] %(message)s <%(filename)s:%(lineno)s/%(funcName)s()>",
        level=level.upper(),
    )
