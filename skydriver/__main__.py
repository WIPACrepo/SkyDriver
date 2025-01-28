"""Start server as application."""

import asyncio
import logging

import boto3
import botocore.client
from rest_tools.client import ClientCredentialsAuth, RestClient

from . import database, k8s, server
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


def setup_s3_client() -> botocore.client.BaseClient:
    """Connect to S3 server."""
    return boto3.client(
        "s3",
        "us-east-1",
        endpoint_url=ENV.S3_URL,
        aws_access_key_id=ENV.S3_ACCESS_KEY_ID,
        aws_secret_access_key=ENV.S3_SECRET_KEY,
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

    # S3 client
    LOGGER.info("Setting up s3 client...")
    s3_client = setup_s3_client()
    LOGGER.info("S3 client connected.")

    # Scan Backlog Runner
    LOGGER.info("Starting scan backlog runner...")
    backlog_task = asyncio.create_task(
        k8s.scan_backlog.run(mongo_client, k8s_batch_api, ewms_rc, s3_client)
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
        backlog_task.cancel()


if __name__ == "__main__":
    config_logging()
    asyncio.run(main())
