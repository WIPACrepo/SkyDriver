"""Root python script for SkyDriver REST API server interface."""


from typing import Any

import kubernetes.client  # type: ignore[import]
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
from rest_tools.server import RestHandlerSetup, RestServer

from . import rest_handlers
from .config import ENV, LOGGER, is_testing


async def make(
    mongo_client: AsyncIOMotorClient,
    k8s_api: kubernetes.client.BatchV1Api,
) -> RestServer:
    """Make a SkyDriver REST service (does not start up automatically)."""
    debug = is_testing()

    rhs_config: dict[str, Any] = {"debug": debug}
    if ENV.AUTH_OPENID_URL:
        rhs_config["auth"] = {
            "audience": ENV.AUTH_AUDIENCE,
            "openid_url": ENV.AUTH_OPENID_URL,
        }
    args = RestHandlerSetup(rhs_config)

    #
    # Setup clients/apis
    args["mongo_client"] = mongo_client
    args["k8s_api"] = k8s_api

    # Configure REST Routes
    rs = RestServer(debug=debug)

    for klass in [
        rest_handlers.RunEventMappingHandler,
        rest_handlers.MainHandler,
        rest_handlers.ScanHandler,
        rest_handlers.ScanManifestHandler,
        rest_handlers.ScanResultHandler,
        rest_handlers.ScanLauncherHandler,
    ]:
        try:
            rs.add_route(getattr(klass, "ROUTE"), klass, args)
            LOGGER.info(f"Added handler: {klass.__name__}")
        except AttributeError:
            continue

    return rs
