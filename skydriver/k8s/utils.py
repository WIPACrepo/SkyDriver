"""An interface to the Kubernetes cluster."""

import logging
from typing import Any, Iterator

import kubernetes.client  # type: ignore[import-untyped]

from ..config import ENV, sdict

LOGGER = logging.getLogger(__name__)


class KubeAPITools:
    """A convenience wrapper around `kubernetes.client`."""

    @staticmethod
    def start_job(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_dict: sdict,
    ) -> Any:
        """Start the k8s job.

        Returns REST response.
        """
        if not job_dict:
            raise ValueError("Job object not created")

        LOGGER.info(job_dict)

        try:
            resp = kubernetes.utils.create_from_dict(
                k8s_batch_api.api_client,
                job_dict,
                namespace=ENV.K8S_NAMESPACE,
            )
        except Exception:  # broad b/c re-raising
            LOGGER.error("request to make k8s job failed above job_dict")
            raise
        else:
            return resp

    @staticmethod
    def get_pods(
        k8s_core_api: kubernetes.client.CoreV1Api,
        job_name: str,
        namespace: str,
    ) -> Iterator[str]:
        """Get each pod corresponding to the job.

        Raises `ValueError` if there are no pods for the job.
        """
        pods: kubernetes.client.V1PodList = k8s_core_api.list_namespaced_pod(
            namespace=namespace, label_selector=f"job-name={job_name}"
        )
        if not pods.items:
            raise ValueError(f"Job {job_name} has no pods")
        for pod in pods.items:
            yield pod.metadata.name
