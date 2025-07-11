"""Init."""

import logging

import kubernetes.client  # type: ignore[import-untyped]
from kubernetes import config
from kubernetes.client.rest import ApiException  # type: ignore[import-untyped]

from . import scanner_instance, utils  # noqa: F401  # export

LOGGER = logging.getLogger(__name__)


def _kube_test_credentials(k8s_batch_api: kubernetes.client.BatchV1Api) -> None:
    """Testing function.

    If you get an error on this call don't proceed. Something is wrong on your connectivity to
    Google API.
    Check Credentials, permissions, keys, etc.
    Docs: https://cloud.google.com/docs/authentication/
    """
    LOGGER.debug("testing k8s credentials")
    try:
        api_response = k8s_batch_api.get_api_resources()
        LOGGER.debug(api_response)
    except ApiException as e:
        LOGGER.exception(e)
        raise


def setup_k8s_batch_api() -> kubernetes.client.BatchV1Api:
    """Load Kubernetes config, check connection, and return API instance."""
    LOGGER.debug("loading k8s batch api")

    configuration = kubernetes.client.Configuration()

    config.load_incluster_config(configuration)  # uses pod's service account
    # config.load_kube_config()  # looks for 'KUBECONFIG' or '~/.kube/config'

    k8s_batch_api = kubernetes.client.BatchV1Api(
        kubernetes.client.ApiClient(configuration)
    )
    _kube_test_credentials(k8s_batch_api)

    return k8s_batch_api
