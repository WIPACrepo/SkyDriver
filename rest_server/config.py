"""Config settings."""

import dataclasses as dc
import os

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


USER_ACCT = "skydriver-service-account"
SKYMAP_SCANNER_ACCT = "skymap-scanner-service-account"

EXCLUDE_DBS = [
    "system.indexes",
    "production",
    "local",
    "config",
    "token_service",
    "admin",
]

EXCLUDE_COLLECTIONS = ["system.indexes"]


def is_testing() -> bool:
    """Return true if this is the test environment.

    Note: this needs to run on import.
    """
    return bool(os.environ.get("CI_TEST_ENV", False))
