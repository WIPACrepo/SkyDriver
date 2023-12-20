"""Entry-point to start up EWMS Sidecar."""

from . import config, ewms_sidecar

if __name__ == "__main__":
    ewms_sidecar.main()
    config.LOGGER.info("Done.")
