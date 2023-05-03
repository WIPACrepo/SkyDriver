"""Config settings."""

import dataclasses as dc
import logging
from typing import Optional

import coloredlogs  # type: ignore[import]
from wipac_dev_tools import from_environment_as_dataclass

LOGGER = logging.getLogger("skydriver")

# --------------------------------------------------------------------------------------
# Constants


KNOWN_CONDORS = {
    "sub-2": {
        "collector": "glidein-cm.icecube.wisc.edu",
        "schedd": "sub-2.icecube.wisc.edu",
    }
}


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
    SCAN_BACKLOG_RUNNER_DELAY: int = 5 * 60

    CLIENTMANAGER_IMAGE_WITH_TAG: str = ""

    # k8s
    K8S_NAMESPACE: str = ""
    K8S_SECRET_NAME: str = ""
    K8S_BACKOFF_LIMIT: int = 1
    K8S_APPLICATION_NAME: str = ""
    K8S_TTL_SECONDS_AFTER_FINISHED: int = 600
    K8S_CONTAINER_MEMORY_DEFAULT: str = "64M"
    K8S_CONTAINER_MEMORY_TMS_STARTER: str = "256M"
    K8S_CONTAINER_MEMORY_SKYSCAN_SERVER: str = "512M"

    # keycloak
    KEYCLOAK_OIDC_URL: str = ""
    KEYCLOAK_CLIENT_ID_BROKER: str = ""
    KEYCLOAK_CLIENT_SECRET_BROKER: str = ""
    KEYCLOAK_CLIENT_ID_SKYDRIVER_REST: str = ""
    KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST: str = ""

    # skyscan (forwarded)
    SKYSCAN_BROKER_ADDRESS: str = "localhost"
    # TODO: see https://github.com/WIPACrepo/wipac-dev-tools/pull/69
    SKYSCAN_PROGRESS_INTERVAL_SEC: Optional[int] = None
    SKYSCAN_RESULT_INTERVAL_SEC: Optional[int] = None
    SKYSCAN_MQ_TIMEOUT_TO_CLIENTS: Optional[int] = None
    SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS: Optional[int] = None
    SKYSCAN_LOG: Optional[str] = None
    SKYSCAN_LOG_THIRD_PARTY: Optional[str] = None

    # EWMS (forwarded)
    EWMS_PILOT_SUBPROC_TIMEOUT: Optional[int] = None  # max work time (stop slow worker)
    EWMS_PILOT_QUARANTINE_TIME: int = 0  # wait-time after error (stop greedy worker)
    EWMS_TMS_S3_BUCKET: str = ""
    EWMS_TMS_S3_URL: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "LOG_LEVEL", self.LOG_LEVEL.upper())  # b/c frozen

        # check missing env var(s)
        if not self.CLIENTMANAGER_IMAGE_WITH_TAG:
            raise RuntimeError(
                "Missing required environment variable: 'CLIENTMANAGER_IMAGE_WITH_TAG'"
            )


ENV = from_environment_as_dataclass(EnvConfig)


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
