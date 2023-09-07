"""Init."""

import logging

import kubernetes.client  # type: ignore[import]
from kubernetes import config
from kubernetes.client.rest import ApiException  # type: ignore[import]

from . import scanner_instance, utils  # noqa: F401  # export

LOGGER = logging.getLogger(__name__)


def _kube_test_credentials(batch_api: kubernetes.client.BatchV1Api) -> None:
    """Testing function.

    If you get an error on this call don't proceed. Something is wrong on your connectivity to
    Google API.
    Check Credentials, permissions, keys, etc.
    Docs: https://cloud.google.com/docs/authentication/
    """
    LOGGER.debug("testing k8s credentials")
    try:
        api_response = batch_api.get_api_resources()
        LOGGER.debug(api_response)
    except ApiException as e:
        LOGGER.exception(e)
        raise


def setup_k8s_client() -> kubernetes.client.BatchV1Api:
    """Load Kubernetes config, check connection, and return API instance."""
    LOGGER.debug("loading k8s api")

    configuration = kubernetes.client.Configuration()

    config.load_incluster_config(configuration)  # uses pod's service account
    # config.load_kube_config()  # looks for 'KUBECONFIG' or '~/.kube/config'

    batch_api = kubernetes.client.BatchV1Api(kubernetes.client.ApiClient(configuration))
    _kube_test_credentials(batch_api)

    return batch_api
