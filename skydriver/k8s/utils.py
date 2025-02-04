"""An interface to the Kubernetes cluster."""

import json
import logging
from typing import Any, Iterator

import kubernetes.client  # type: ignore[import-untyped]
from kubernetes.client.rest import ApiException  # type: ignore[import-untyped]

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
        try:
            api_response = kubernetes.utils.create_from_dict(
                k8s_batch_api.api_client,
                job_dict,
                namespace=ENV.K8S_NAMESPACE,
            )
            LOGGER.info(api_response)
        except ApiException:
            LOGGER.error("request to make k8s job failed using:")
            LOGGER.error(json.dumps(job_dict, indent=4))
            raise
        else:
            return api_response

    @staticmethod
    def get_pods(
        k8s_core_api: kubernetes.client.CoreV1Api,
        job_name: str,
        namespace: str,
    ) -> Iterator[kubernetes.client.V1Pod]:
        """Get each pod corresponding to the job.

        Raises `ValueError` if there are no pods for the job.
        """
        pods: kubernetes.client.V1PodList = k8s_core_api.list_namespaced_pod(
            namespace=namespace, label_selector=f"job-name={job_name}"
        )
        if not pods.items:
            raise ValueError(f"Job {job_name} has no pods")
        for pod in pods.items:
            yield pod

    @staticmethod
    def get_pod_status(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_name: str,
        namespace: str,
    ) -> dict[str, dict[str, Any]]:
        """Get the status of the k8s pod(s) and their containers.

        Raises `ValueError` if there are no pods for the job.
        """
        LOGGER.info(f"getting pod status for {job_name=} {namespace=}")
        status = {}

        k8s_core_api = kubernetes.client.CoreV1Api(api_client=k8s_batch_api.api_client)

        for pod in KubeAPITools.get_pods(k8s_core_api, job_name, namespace):
            pod_status: kubernetes.client.V1PodStatus = pod.status
            # pod status has non-serializable things like datetime objects
            serializable = json.loads(json.dumps(pod_status.to_dict(), default=str))
            status[pod.metadata.name] = serializable

        return status

    @staticmethod
    def get_container_logs(
        k8s_batch_api: kubernetes.client.BatchV1Api,
        job_name: str,
        namespace: str,
    ) -> dict[str, dict[str, str]]:
        """Grab the logs for all containers.

        Raises `ValueError` if there are no pods for the job.
        """
        LOGGER.info(f"getting logs for {job_name=} {namespace=}")
        logs = {}

        k8s_core_api = kubernetes.client.CoreV1Api(api_client=k8s_batch_api.api_client)

        for pod in KubeAPITools.get_pods(k8s_core_api, job_name, namespace):
            these_logs = {}
            for container in pod.spec.containers:
                these_logs[container.name] = k8s_core_api.read_namespaced_pod_log(
                    pod.metadata.name,
                    namespace,
                    container=container.name,
                    timestamps=True,
                )
            logs[pod.metadata.name] = these_logs

        return logs
