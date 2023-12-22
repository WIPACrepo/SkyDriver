"""Config settings."""


import dataclasses as dc
import enum
import logging
from typing import Any, Optional

import humanfriendly
import kubernetes.client  # type: ignore[import-untyped]
from wipac_dev_tools import from_environment_as_dataclass, logging_tools

# --------------------------------------------------------------------------------------
# Constants


DEFAULT_K8S_CONTAINER_MEMORY_SKYSCAN_SERVER_BYTES: int = humanfriendly.parse_size(
    "1024M"
)
DEFAULT_WORKER_MEMORY_BYTES: int = humanfriendly.parse_size("8GB")
DEFAULT_WORKER_DISK_BYTES: int = humanfriendly.parse_size("1GB")

K8S_CONTAINER_MEMORY_DEFAULT_BYTES: int = humanfriendly.parse_size("64M")
K8S_CONTAINER_MEMORY_CLUSTER_STOPPER_BYTES: int = humanfriendly.parse_size("256M")
K8S_CONTAINER_MEMORY_CLUSTER_STARTER_BYTES: int = humanfriendly.parse_size("256M")

CLUSTER_STOPPER_K8S_TTL_SECONDS_AFTER_FINISHED = 1 * 60 * 60
CLUSTER_STOPPER_K8S_JOB_N_RETRIES = 6

SCAN_MIN_PRIORITY_TO_START_NOW = 10


@enum.unique
class DebugMode(enum.Enum):
    """Various debug modes."""

    CLIENT_LOGS = "client-logs"


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
    LOG_LEVEL_THIRD_PARTY: str = "WARNING"

    SCAN_BACKLOG_MAX_ATTEMPTS: int = 3
    SCAN_BACKLOG_RUNNER_SHORT_DELAY: int = 15
    SCAN_BACKLOG_RUNNER_DELAY: int = 5 * 60
    SCAN_BACKLOG_PENDING_ENTRY_TTL_REVIVE: int = 5 * 60  # entry is revived after N secs

    THIS_IMAGE_WITH_TAG: str = ""

    # k8s
    K8S_NAMESPACE: str = ""
    K8S_SECRET_NAME: str = ""
    K8S_SKYSCAN_JOBS_SERVICE_ACCOUNT: str = ""
    K8S_APPLICATION_NAME: str = ""
    K8S_TTL_SECONDS_AFTER_FINISHED: int = 600

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
    EWMS_PILOT_QUARANTINE_TIME: Optional[int] = None
    EWMS_TMS_S3_BUCKET: str = ""
    EWMS_TMS_S3_URL: str = ""

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
        "orchestrator": "condor",
        "location": {
            "collector": "glidein-cm.icecube.wisc.edu",
            "schedd": "sub-2.icecube.wisc.edu",
        },
        "v1envvars": [
            kubernetes.client.V1EnvVar(
                name="CONDOR_TOKEN",
                value_from=kubernetes.client.V1EnvVarSource(
                    secret_key_ref=kubernetes.client.V1SecretKeySelector(
                        name=ENV.K8S_SECRET_NAME,
                        key="condor_token_sub2",
                    )
                ),
            )
        ],
        "max_n_clients_during_debug_mode": 10,
    },
    LOCAL_K8S_HOST: {
        "orchestrator": "k8s",
        "location": {
            "host": LOCAL_K8S_HOST,
            "namespace": ENV.K8S_NAMESPACE,
        },
        "v1envvars": [],
    },
    "gke-2306": {
        "orchestrator": "k8s",
        "location": {
            "host": "https://34.171.167.119:443",
            "namespace": "icecube-skymap-scanner",
        },
        "v1envvars": [
            kubernetes.client.V1EnvVar(
                name="WORKER_K8S_CACERT",
                value_from=kubernetes.client.V1EnvVarSource(
                    secret_key_ref=kubernetes.client.V1SecretKeySelector(
                        name=ENV.K8S_SECRET_NAME,
                        key="worker_k8s_cacert_gke",
                    )
                ),
            ),
            kubernetes.client.V1EnvVar(
                name="WORKER_K8S_TOKEN",
                value_from=kubernetes.client.V1EnvVarSource(
                    secret_key_ref=kubernetes.client.V1SecretKeySelector(
                        name=ENV.K8S_SECRET_NAME,
                        key="worker_k8s_token_gke",
                    )
                ),
            ),
        ],
    },
}


def is_testing() -> bool:
    """Return true if this is the test environment.

    Note: this needs to run on import.
    """
    return ENV.CI_TEST


def config_logging() -> None:
    """Configure the logging level and format.

    This is separated into a function for consistency between app and
    testing environments.
    """
    hand = logging.StreamHandler()
    hand.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(levelname)8s] %(hostname)s %(name)s[%(process)d] %(message)s <%(filename)s:%(lineno)s/%(funcName)s()>",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(hand)
    logging_tools.set_level(
        ENV.LOG_LEVEL,  # type: ignore[arg-type]
        first_party_loggers=__name__.split(".", maxsplit=1)[0],
        third_party_level=ENV.LOG_LEVEL_THIRD_PARTY,  # type: ignore[arg-type]
        future_third_parties=[],
    )
