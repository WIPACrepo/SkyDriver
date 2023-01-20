"""Root python script for SkyDriver REST API server interface."""


import dataclasses as dc
from typing import Any
from urllib.parse import quote_plus

import kubernetes.client  # type: ignore[import]
from kubernetes import config
from kubernetes.client.rest import ApiException  # type: ignore[import]
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
from rest_tools.server import RestHandlerSetup, RestServer

from . import database, rest_handlers
from .config import ENV, LOGGER, is_testing


def mongodb_url() -> str:
    """Construct the MongoDB URL."""
    auth_user = quote_plus(ENV.MONGODB_AUTH_USER)
    auth_pass = quote_plus(ENV.MONGODB_AUTH_PASS)

    url = f"mongodb://{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"
    if auth_user and auth_pass:
        url = f"mongodb://{auth_user}:{auth_pass}@{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"

    return url


def kube_test_credentials(api_instance: kubernetes.client.BatchV1Api) -> None:
    """Testing function.

    If you get an error on this call don't proceed. Something is wrong on your connectivty to
    Google API.
    Check Credentials, permissions, keys, etc.
    Docs: https://cloud.google.com/docs/authentication/
    """
    try:
        api_response = api_instance.get_api_resources()
        LOGGER.info(api_response)
    except ApiException as e:
        LOGGER.error(e)
        raise


def setup_k8s_client() -> kubernetes.client.BatchV1Api:
    """Load Kubernetes config, check connection, and return API instance."""
    config.load_kube_config()
    k8s_api = kubernetes.client.BatchV1Api(
        kubernetes.client.ApiClient(kubernetes.client.Configuration())
    )
    kube_test_credentials(k8s_api)
    return k8s_api


async def make(debug: bool = False) -> RestServer:
    """Make a SkyDriver REST service (does not start up automatically)."""
    for field in dc.fields(ENV):
        LOGGER.info(
            f"{field.name}\t{getattr(ENV, field.name)}\t({type(getattr(ENV, field.name)).__name__})"
        )

    rhs_config: dict[str, Any] = {"debug": debug or is_testing()}
    if ENV.AUTH_OPENID_URL:
        rhs_config["auth"] = {
            "audience": ENV.AUTH_AUDIENCE,
            "openid_url": ENV.AUTH_OPENID_URL,
        }
    args = RestHandlerSetup(rhs_config)

    # Setup clients/apis
    args["mongo_client"] = AsyncIOMotorClient(mongodb_url())
    args["k8s_api"] = setup_k8s_client()

    # Configure REST Routes
    rs = RestServer(debug=debug)

    for klass in [
        rest_handlers.EventMappingHandler,
        rest_handlers.MainHandler,
        rest_handlers.ManifestHandler,
        rest_handlers.ResultsHandler,
        rest_handlers.ScanLauncherHandler,
    ]:
        try:
            rs.add_route(getattr(klass, "ROUTE"), klass, args)
            LOGGER.info(f"Added handler: {klass.__name__}")
        except AttributeError:
            continue

    await database.interface.ensure_indexes(AsyncIOMotorClient(args["mongodb_url"]))
    return rs
