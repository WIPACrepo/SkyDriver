"""Start server as application."""

import asyncio
import logging

from kubernetes.client import CoreV1Api
from rest_tools.client import ClientCredentialsAuth, RestClient

from . import background_runners, database, k8s, server
from .config import ENV, config_logging

LOGGER = logging.getLogger(__name__)


def setup_ewms_client() -> RestClient:
    """Connect to EWMS rest server."""
    if ENV.CI:
        return RestClient(
            ENV.EWMS_ADDRESS,
            logger=LOGGER,
        )
    else:
        return ClientCredentialsAuth(
            ENV.EWMS_ADDRESS,
            ENV.EWMS_TOKEN_URL,
            ENV.EWMS_CLIENT_ID,
            ENV.EWMS_CLIENT_SECRET,
            logger=LOGGER,
        )


async def main() -> None:
    """Establish connections and start components."""

    # Mongo client
    LOGGER.info("Setting up Mongo client...")
    mongo_client = await database.create_mongodb_client()
    indexing_task = asyncio.create_task(database.utils.ensure_indexes(mongo_client))
    await asyncio.sleep(0)  # start up previous task
    LOGGER.info("Mongo client connected.")

    # K8s client
    LOGGER.info("Setting up k8s client...")
    k8s_batch_api = k8s.setup_k8s_batch_api()
    LOGGER.info("K8s client connected.")

    # EWMS rest client
    LOGGER.info("Setting up EWMS client...")
    ewms_rc = setup_ewms_client()
    LOGGER.info("EWMS client connected.")

    # Scan Launcher
    LOGGER.info("Starting scan launcher...")
    launcher_task = asyncio.create_task(
        background_runners.scan_launcher.run(mongo_client, k8s_batch_api)
    )
    await asyncio.sleep(0)  # start up previous task

    # Scan Pod Watchdog
    LOGGER.info("Starting scan pod watchdog...")
    watchdog_task = asyncio.create_task(
        background_runners.scan_pod_watchdog.run(
            mongo_client, CoreV1Api(k8s_batch_api.api_client)
        )
    )
    await asyncio.sleep(0)  # start up previous task

    # REST Server
    LOGGER.info("Setting up REST server...")
    rs = await server.make(mongo_client, k8s_batch_api, ewms_rc)
    rs.startup(address=ENV.REST_HOST, port=ENV.REST_PORT)  # type: ignore[no-untyped-call]
    try:
        await asyncio.Event().wait()
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]
        indexing_task.cancel()
        launcher_task.cancel()
        watchdog_task.cancel()


if __name__ == "__main__":
    config_logging()
    asyncio.run(main())
