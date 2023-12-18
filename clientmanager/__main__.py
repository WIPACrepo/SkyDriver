"""Entry-point to start up clientmanager service."""

from . import clientmanager, config

if __name__ == "__main__":
    clientmanager.main()
    config.LOGGER.info("Done.")
