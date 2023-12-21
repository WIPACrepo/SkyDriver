"""Entry-point to start up EWMS Sidecar."""


import logging

from . import ewms_sidecar

LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    ewms_sidecar.main()
    LOGGER.info("Done.")
