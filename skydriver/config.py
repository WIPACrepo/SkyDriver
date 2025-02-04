"""Config settings."""

import dataclasses as dc
import enum
import logging
from typing import Any, Optional

from wipac_dev_tools import from_environment_as_dataclass, logging_tools

sdict = dict[str, Any]

# --------------------------------------------------------------------------------------
# Constants


K8S_MIN_MEM_LIMIT = "100M"
K8S_MIN_MEM_REQUEST = "10M"

SCAN_MIN_PRIORITY_TO_START_ASAP = 100

# WARNING: these values must remain constant, they are cross-referenced in the db
QUEUE_ALIAS_TOCLIENT = "to-client-queue"  # ''
QUEUE_ALIAS_FROMCLIENT = "from-client-queue"  # ''


@enum.unique
class DebugMode(enum.Enum):
    """Various debug modes."""

    CLIENT_LOGS = "client-logs"


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    # EWMS connections
    EWMS_ADDRESS: str
    EWMS_TOKEN_URL: str
    EWMS_CLIENT_ID: str
    EWMS_CLIENT_SECRET: str

    # s3
    S3_URL: str
    S3_ACCESS_KEY_ID: str  # the actual value
    S3_ACCESS_KEY_ID__K8S_SECRET_KEY: str  # the key used in the k8s secrets.yml
    S3_SECRET_KEY: str  # the actual value
    S3_SECRET_KEY__K8S_SECRET_KEY: str  # the key used in the k8s secrets.yml
    S3_BUCKET: str

    # misc
    AUTH_AUDIENCE: str = "skydriver"
    AUTH_OPENID_URL: str = ""
    MONGODB_AUTH_PASS: str = ""  # empty means no authentication required
    MONGODB_AUTH_USER: str = ""  # None means required to specify
    MONGODB_HOST: str = "localhost"
    MONGODB_PORT: int = 27017
    REST_HOST: str = "localhost"
    REST_PORT: int = 8080

    CI: bool = False  # github actions sets this to 'true'
    LOG_LEVEL: str = "DEBUG"
    LOG_LEVEL_THIRD_PARTY: str = "WARNING"

    SCAN_BACKLOG_MAX_ATTEMPTS: int = 3
    SCAN_BACKLOG_RUNNER_SHORT_DELAY: int = 15
    SCAN_BACKLOG_RUNNER_DELAY: int = 5 * 60
    SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE: int = 5 * 60  # entry is revived after N secs

    THIS_IMAGE_WITH_TAG: str = ""
    MIN_SKYMAP_SCANNER_TAG: str = "v4.0.0"

    # k8s
    K8S_NAMESPACE: str = ""
    K8S_SECRET_NAME: str = ""
    K8S_SKYSCAN_JOBS_SERVICE_ACCOUNT: str = ""
    K8S_APPLICATION_NAME: str = ""
    K8S_TTL_SECONDS_AFTER_FINISHED: int = 10 * 60
    K8S_ACTIVE_DEADLINE_SECONDS: int = 24 * 60 * 60
    #
    K8S_SCANNER_MEM_REQUEST__DEFAULT: str = "1024M"  # note: also used as the limit def.
    K8S_SCANNER_CPU_LIMIT: float = 1.0
    K8S_SCANNER_CPU_REQUEST: float = 0.10
    #
    K8S_SCANNER_INIT_MEM_LIMIT: str = K8S_MIN_MEM_LIMIT
    K8S_SCANNER_INIT_CPU_LIMIT: float = 0.10
    K8S_SCANNER_INIT_MEM_REQUEST: str = K8S_MIN_MEM_REQUEST
    K8S_SCANNER_INIT_CPU_REQUEST: float = 0.05
    #
    K8S_SCANNER_SIDECAR_S3_MEM_LIMIT: str = K8S_MIN_MEM_LIMIT
    K8S_SCANNER_SIDECAR_S3_CPU_LIMIT: float = 0.10
    K8S_SCANNER_SIDECAR_S3_MEM_REQUEST: str = K8S_MIN_MEM_REQUEST
    K8S_SCANNER_SIDECAR_S3_CPU_REQUEST: float = 0.05

    # EWMS optional config
    EWMS_WORKER_MEMORY__DEFAULT: str = "8GB"
    EWMS_WORKER_DISK__DEFAULT: str = "1GB"
    EWMS_MAX_WORKER_RUNTIME__DEFAULT: int = 4 * 60 * 60  # 4 hours
    # note: other EWMS vars at top of class

    # keycloak
    KEYCLOAK_OIDC_URL: str = ""
    KEYCLOAK_CLIENT_ID_SKYDRIVER_REST: str = ""
    KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST: str = ""

    # skyscan (forwarded)
    SKYSCAN_PROGRESS_INTERVAL_SEC: Optional[int] = None
    SKYSCAN_RESULT_INTERVAL_SEC: Optional[int] = None
    SKYSCAN_MQ_TIMEOUT_TO_CLIENTS: Optional[int] = None
    SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS: Optional[int] = None
    SKYSCAN_LOG: Optional[str] = None
    SKYSCAN_LOG_THIRD_PARTY: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "LOG_LEVEL", self.LOG_LEVEL.upper())  # b/c frozen

        # check missing env var(s)
        if not self.THIS_IMAGE_WITH_TAG:
            raise RuntimeError(
                "Missing required environment variable: 'THIS_IMAGE_WITH_TAG'"
            )

        if self.SCAN_BACKLOG_RUNNER_SHORT_DELAY > self.SCAN_BACKLOG_RUNNER_DELAY:
            raise RuntimeError(
                "'SCAN_BACKLOG_RUNNER_SHORT_DELAY' cannot be greater than 'SCAN_BACKLOG_RUNNER_DELAY'"
            )
        if self.SCAN_BACKLOG_RUNNER_DELAY > self.SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE:
            raise RuntimeError(
                "'SCAN_BACKLOG_RUNNER_DELAY' cannot be greater than 'SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE'"
            )


ENV = from_environment_as_dataclass(EnvConfig)

LOCAL_K8S_HOST = "local"

# known cluster locations
KNOWN_CLUSTERS: dict[str, dict[str, Any]] = {
    "sub-2": {
        "max_n_clients_during_debug_mode": 100,
    },
    LOCAL_K8S_HOST: {
        "max_n_clients_during_debug_mode": 5,
    },
}


def is_testing() -> bool:
    """Return true if this is the test environment.

    Note: this needs to run on import.
    """
    return ENV.CI


def config_logging() -> None:
    """Configure the logging level and format.

    This is separated into a function for consistency between app and
    testing environments.
    """
    hand = logging.StreamHandler()
    hand.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)8s] %(name)s[%(process)d] %(message)s <%(filename)s:%(lineno)s/%(funcName)s()>",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(hand)
    logging_tools.set_level(
        ENV.LOG_LEVEL,  # type: ignore[arg-type]
        first_party_loggers=__name__.split(".", maxsplit=1)[0],
        third_party_level=ENV.LOG_LEVEL_THIRD_PARTY,  # type: ignore[arg-type]
        future_third_parties=[],
        specialty_loggers={"rest_tools": "INFO"},
    )
