"""Start server as application."""


import asyncio
import logging

from wipac_dev_tools import logging_tools

from . import database, k8s, server
from .config import ENV

LOGGER = logging.getLogger(__name__)


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

    # Scan Backlog Runner
    LOGGER.info("Starting scan backlog runner...")
    backlog_task = asyncio.create_task(
        k8s.scan_backlog.startup(mongo_client, k8s_batch_api)
    )
    await asyncio.sleep(0)  # start up previous task

    # REST Server
    LOGGER.info("Setting up REST server...")
    rs = await server.make(mongo_client, k8s_batch_api)
    rs.startup(address=ENV.REST_HOST, port=ENV.REST_PORT)  # type: ignore[no-untyped-call]
    try:
        await asyncio.Event().wait()
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]
        indexing_task.cancel()
        backlog_task.cancel()


if __name__ == "__main__":
    # "%(asctime)s.%(msecs)03d [%(levelname)8s] %(hostname)s %(name)s[%(process)d] %(message)s <%(filename)s:%(lineno)s/%(funcName)s()>"
    logging_tools.set_level(
        ENV.LOG_LEVEL,  # type: ignore[arg-type]
        first_party_loggers=LOGGER,
        third_party_level=ENV.LOG_LEVEL_THIRD_PARTY,  # type: ignore[arg-type]
        use_coloredlogs=True,  # for formatting
        future_third_parties=[],
    )
    asyncio.run(main())
