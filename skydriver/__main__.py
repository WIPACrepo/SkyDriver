"""Start server as application."""

import asyncio

from . import database, k8s, server
from .config import ENV, LOGGER, config_logging


async def main() -> None:
    """Establish connections and start components."""

    # Mongo client
    LOGGER.info("Setting up Mongo client...")
    mongo_client = await database.create_mongodb_client()
    LOGGER.info("Mongo client connected.")

    # K8s client
    LOGGER.info("Setting up k8s client...")
    k8s_api = k8s.setup_k8s_client()
    LOGGER.info("K8s client connected.")

    LOGGER.info("Setting up REST server...")
    rs = await server.make(mongo_client, k8s_api)
    rs.startup(address=ENV.REST_HOST, port=ENV.REST_PORT)  # type: ignore[no-untyped-call]
    try:
        await asyncio.Event().wait()
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]


if __name__ == "__main__":
    config_logging(ENV.LOG_LEVEL)
    asyncio.run(main())
