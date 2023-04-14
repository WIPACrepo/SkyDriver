"""Root python script for SkyDriver REST API server interface."""


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


def _kube_test_credentials(api_instance: kubernetes.client.BatchV1Api) -> None:
    """Testing function.

    If you get an error on this call don't proceed. Something is wrong on your connectivity to
    Google API.
    Check Credentials, permissions, keys, etc.
    Docs: https://cloud.google.com/docs/authentication/
    """
    LOGGER.debug("testing k8s credentials")
    try:
        api_response = api_instance.get_api_resources()
        LOGGER.debug(api_response)
    except ApiException as e:
        LOGGER.error(e)
        raise


def setup_k8s_client() -> kubernetes.client.BatchV1Api:
    """Load Kubernetes config, check connection, and return API instance."""
    LOGGER.debug("loading k8s api")

    configuration = kubernetes.client.Configuration()

    config.load_incluster_config(configuration)  # uses pod's service account
    # config.load_kube_config()  # looks for 'KUBECONFIG' or '~/.kube/config'

    k8s_api = kubernetes.client.BatchV1Api(kubernetes.client.ApiClient(configuration))
    _kube_test_credentials(k8s_api)

    return k8s_api


async def make(debug: bool = False) -> RestServer:
    """Make a SkyDriver REST service (does not start up automatically)."""
    rhs_config: dict[str, Any] = {"debug": debug or is_testing()}
    if ENV.AUTH_OPENID_URL:
        rhs_config["auth"] = {
            "audience": ENV.AUTH_AUDIENCE,
            "openid_url": ENV.AUTH_OPENID_URL,
        }
    args = RestHandlerSetup(rhs_config)

    #
    # Setup clients/apis

    LOGGER.info("Setting up Mongo client...")
    args["mongo_client"] = AsyncIOMotorClient(mongodb_url())
    LOGGER.info("Mongo client connected.")

    LOGGER.info("Setting up k8s client...")
    args["k8s_api"] = setup_k8s_client()
    LOGGER.info("K8s client connected.")

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

    await database.interface.ensure_indexes(args["mongo_client"])
    return rs
