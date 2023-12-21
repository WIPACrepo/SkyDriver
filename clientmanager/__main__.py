"""Entry-point to start up clientmanager service."""


import logging

from . import clientmanager

LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    clientmanager.main()
    LOGGER.info("Done.")
