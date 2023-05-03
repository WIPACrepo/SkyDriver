"""Start server as application."""

import asyncio
from urllib.parse import quote_plus

import kubernetes.client  # type: ignore[import]
from kubernetes import config
from kubernetes.client.rest import ApiException  # type: ignore[import]
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore

from . import server
from .config import ENV, LOGGER, config_logging


def create_mongodb_client() -> AsyncIOMotorClient:
    """Construct the MongoDB client."""
    auth_user = quote_plus(ENV.MONGODB_AUTH_USER)
    auth_pass = quote_plus(ENV.MONGODB_AUTH_PASS)

    url = f"mongodb://{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"
    if auth_user and auth_pass:
        url = f"mongodb://{auth_user}:{auth_pass}@{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"

    return AsyncIOMotorClient(url)


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


async def main() -> None:
    """Establish connections and start components."""
    LOGGER.info("Setting up Mongo client...")
    mongo_client = create_mongodb_client()
    LOGGER.info("Mongo client connected.")

    LOGGER.info("Setting up k8s client...")
    k8s_api = setup_k8s_client()
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
