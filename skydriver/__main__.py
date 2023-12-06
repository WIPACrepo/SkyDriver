"""Start server as application."""

import asyncio

from . import database, k8s, server
from .config import ENV, LOGGER, config_logging


async def database_schema_migration(motor_client) -> None:  # type: ignore[no-untyped-def]
    from dacite import from_dict
    from motor.motor_asyncio import AsyncIOMotorCollection

    collection = AsyncIOMotorCollection(
        motor_client[database.utils._DB_NAME],
        database.utils._MANIFEST_COLL_NAME,
    )

    m = 0
    for i, doc in enumerate([d async for d in collection.find({})]):
        if "ewms_task" in doc:
            continue
        m += 1
        LOGGER.info(f"migrating {doc}...")
        doc["ewms_task"] = {
            "tms_args": doc.pop("tms_args"),
            "env_vars": doc.pop("env_vars"),
            "clusters": doc.pop("clusters"),
            "complete": doc.pop("complete"),
        }
        _id = doc.pop("_id")
        from_dict(database.schema.Manifest, doc)  # validate
        await collection.find_one_and_replace({"_id": _id}, doc)
        LOGGER.info(f"migrated {doc}")
    LOGGER.info(f"total migrated: {m} (looked at {i+1} docs)")


async def main() -> None:
    """Establish connections and start components."""

    # Mongo client
    LOGGER.info("Setting up Mongo client...")
    mongo_client = await database.create_mongodb_client()
    indexing_task = asyncio.create_task(database.utils.ensure_indexes(mongo_client))
    await asyncio.sleep(0)  # start up previous task
    LOGGER.info("Mongo client connected.")

    try:
        await database_schema_migration(mongo_client)
    except Exception as e:  # noqa: E722
        # if this triggers, Ric is working on a fix :-)
        LOGGER.exception(e)
        LOGGER.error("failed to migrate database data...")

        import time

        time.sleep(60 * 60 * 24)  # one day

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
    config_logging(ENV.LOG_LEVEL)
    asyncio.run(main())
