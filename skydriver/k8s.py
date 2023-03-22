"""An interface to the Kubernetes cluster."""


from pathlib import Path
from typing import Any

import kubernetes.client  # type: ignore[import]
from kubernetes.client.rest import ApiException  # type: ignore[import]
from rest_tools.client import ClientCredentialsAuth

from . import images, types
from .config import ENV, LOGGER
from .database import schema


class KubeAPITools:
    """A convenience wrapper around `kubernetes.client`."""

    @staticmethod
    def patch_or_create_namespaced_secret(
        api_instance: kubernetes.client.BatchV1Api,
        namespace: str,
        secret_name: str,
        secret_type: str,
        encoded_secret_data: schema.StrDict,
    ) -> None:
        """Patch secret and if not exist create."""
        # Instantiate the Secret object
        body = kubernetes.client.V1Secret(
            data=encoded_secret_data,
            type=secret_type,
            metadata=kubernetes.client.V1ObjectMeta(name=secret_name),
        )

        # try to patch first
        try:
            api_instance.patch_namespaced_secret(secret_name, namespace, body)
            LOGGER.info(
                "Secret  {} in namespace {} has been patched".format(
                    secret_name, namespace
                )
            )
        except ApiException as e:
            # a (None or 404) means we can create secret instead, see below
            if e.status and e.status != 404:
                LOGGER.exception(e)
                raise

        # create if patch failed
        try:
            api_instance.create_namespaced_secret(namespace=namespace, body=body)
            LOGGER.info(
                "Created secret {} of type {} in namespace {}".format(
                    secret_name, secret_type, namespace
                )
            )
        except ApiException as e:
            LOGGER.exception(e)
            raise

    @staticmethod
    def kube_create_job_object(
        name: str,
        containers: list[kubernetes.client.V1Container],
        namespace: str,
        volumes: list[str] | None = None,  # volume names
    ) -> kubernetes.client.V1Job:
        """Create a k8 Job Object Minimum definition of a job object.

        Based on https://blog.pythian.com/how-to-create-kubernetes-jobs-with-python/

        {'api_version': None, - Str
        'kind': None,     - Str
        'metadata': None, - Metada Object
        'spec': None,     -V1JobSpec
        'status': None}   - V1Job Status
        Docs: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
        Docs2: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#writing-a-job-spec

        Also docs are pretty pretty bad. Best way is to ´pip install kubernetes´ and go via the autogenerated code
        And figure out the chain of objects that you need to hold a final valid object So for a job object you need:
        V1Job -> V1ObjectMeta
              -> V1JobStatus
              -> V1JobSpec -> V1PodTemplate -> V1PodTemplateSpec -> V1Container

        Now the tricky part, is that V1Job.spec needs a .template, but not a PodTemplateSpec, as such
        you need to build a PodTemplate, add a template field (template.template) and make sure
        template.template.spec is now the PodSpec.
        Then, the V1Job.spec needs to be a JobSpec which has a template the template.template field of the PodTemplate.
        Failure to do so will trigger an API error.

        Also Containers must be a list!

        Docs3: https://github.com/kubernetes-client/python/issues/589
        """
        if not volumes:
            volumes = []

        # Body is the object Body
        body = kubernetes.client.V1Job(api_version="batch/v1", kind="Job")
        # Body needs Metadata
        # Attention: Each JOB must have a different name!
        body.metadata = kubernetes.client.V1ObjectMeta(
            namespace=namespace,
            name=name,
            labels={
                # https://argo-cd.readthedocs.io/en/stable/user-guide/resource_tracking/
                "app.kubernetes.io/instance": ENV.K8S_APPLICATION_NAME,
            },
            annotations={
                "argocd.argoproj.io/sync-options": "Prune=false"  # don't want argocd to prune this job
            },
        )
        # And a Status
        body.status = kubernetes.client.V1JobStatus()
        # Now we start with the Template...
        template = kubernetes.client.V1PodTemplate()
        template.template = kubernetes.client.V1PodTemplateSpec(
            metadata=kubernetes.client.V1ObjectMeta(
                labels={
                    "app": "scanner-instance",
                },
            ),
        )
        # Make Pod Spec
        template.template.spec = kubernetes.client.V1PodSpec(
            containers=containers,
            restart_policy="Never",
            volumes=[
                kubernetes.client.V1Volume(
                    name=n, empty_dir=kubernetes.client.V1EmptyDirVolumeSource()
                )
                for n in volumes
            ],
        )
        # And finaly we can create our V1JobSpec!
        body.spec = kubernetes.client.V1JobSpec(
            ttl_seconds_after_finished=ENV.K8S_TTL_SECONDS_AFTER_FINISHED,
            template=template.template,
            backoff_limit=ENV.K8S_BACKOFF_LIMIT,
        )
        return body

    @staticmethod
    def create_container(
        name: str,
        image: str,
        env: list[kubernetes.client.V1EnvVar],
        args: list[str],
        volumes: dict[str, Path] | None = None,
    ) -> kubernetes.client.V1Container:
        """Make a Container instance."""
        if not volumes:
            volumes = {}
        return kubernetes.client.V1Container(
            name=name,
            image=image,
            env=env,
            args=args,
            volume_mounts=[
                kubernetes.client.V1VolumeMount(name=vol, mount_path=str(mnt))
                for vol, mnt in volumes.items()
            ],
        )

    @staticmethod
    def start_job(
        api_instance: kubernetes.client.BatchV1Api,
        job_obj: kubernetes.client.V1Job,
    ) -> Any:
        """Start the k8s job.

        Returns REST response.
        """
        if not job_obj:
            raise ValueError("Job object not created")
        try:
            api_response = api_instance.create_namespaced_job(
                ENV.K8S_NAMESPACE, job_obj
            )
            LOGGER.info(api_response)
        except ApiException as e:
            LOGGER.error(e)
            raise
        return api_response


def get_condor_token_v1envvar() -> kubernetes.client.V1EnvVar:
    """Get the `V1EnvVar` for `CONDOR_TOKEN`."""
    return kubernetes.client.V1EnvVar(
        name="CONDOR_TOKEN",
        value_from=kubernetes.client.V1EnvVarSource(
            secret_key_ref=kubernetes.client.V1SecretKeySelector(
                name=ENV.K8S_SECRET_NAME,
                key="condor_token",
            )
        ),
    )


class SkymapScannerStarterJob:
    """Wraps a Skymap Scanner Kubernetes job with tools to start and manage."""

    def __init__(
        self,
        api_instance: kubernetes.client.BatchV1Api,
        docker_tag: str,
        scan_id: str,
        # server
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
        # clientmanager
        memory: str,
        request_clusters: list[types.RequestorInputCluster],
        # env
        rest_address: str,
    ):
        self.api_instance = api_instance
        common_space_volume_path = Path("/common-space")

        # store some data for public access
        self.server_args = self.get_server_args(
            common_space_volume_path=common_space_volume_path,
            reco_algo=reco_algo,
            nsides=nsides,
            is_real_event=is_real_event,
        )
        self.clientmanager_args = self.get_clientmanager_starter_args(
            common_space_volume_path=common_space_volume_path,
            singularity_image=images.get_skyscan_cvmfs_singularity_image(docker_tag),
            memory=memory,
            request_clusters=request_clusters,
        )
        env = self.get_env(
            rest_address=rest_address,
            scan_id=scan_id,
        )
        self.env_dict = {  # promote `e.name` to a key of a dict (instead of an attr in list element)
            e.name: {k: v for k, v in e.to_dict().items() if k != "name"} for e in env
        }

        # job
        server = KubeAPITools.create_container(
            f"server-{scan_id}",
            images.get_skyscan_docker_image(docker_tag),
            env,
            self.server_args.split(),
            {common_space_volume_path.name: common_space_volume_path},
        )
        condor_clientmanager_start = KubeAPITools.create_container(
            f"clientmanager-start-{scan_id}",
            ENV.CLIENTMANAGER_IMAGE_WITH_TAG,
            env,
            self.clientmanager_args.split(),
            {common_space_volume_path.name: common_space_volume_path},
        )
        self.job_obj = KubeAPITools.kube_create_job_object(
            f"skyscan-{scan_id}",
            [server, condor_clientmanager_start],
            ENV.K8S_NAMESPACE,
            volumes=[common_space_volume_path.name],
        )

    @staticmethod
    def get_server_args(
        common_space_volume_path: Path,
        reco_algo: str,
        nsides: dict[int, int],
        is_real_event: bool,
    ) -> str:
        """Make the server container args."""
        args = (
            f"python -m skymap_scanner.server "
            f" --reco-algo {reco_algo}"
            f" --cache-dir {common_space_volume_path} "
            f" --output-dir {common_space_volume_path} "
            f" --client-startup-json {common_space_volume_path/'startup.json'} "
            f" --nsides {' '.join(f'{n}:{x}' for n,x in nsides.items())} "  # k1:v1 k2:v2
            f" {'--real-event' if is_real_event else '--simulated-event'} "
        )
        return args

    @staticmethod
    def get_clientmanager_starter_args(
        common_space_volume_path: Path,
        singularity_image: Path,
        memory: str,
        request_clusters: list[types.RequestorInputCluster],
    ) -> str:
        """Make the clientmanager container args.

        This also includes any client args not added by the
        clientmanager.
        """
        clusters_args = " ".join(
            ",".join([x.collector, x.schedd, str(x.njobs)]) for x in request_clusters
        )  # Ex: "collectorA,scheddA,123 collectorB,scheddB,345 collectorC,scheddC,989"

        args = (
            f"python -m clientmanager start "
            f" --cluster {clusters_args} "
            # f" --dryrun"
            f" --logs-directory {common_space_volume_path} "
            # f" --accounting-group "
            f" --memory {memory} "
            f" --singularity-image {singularity_image} "
            f" --client-startup-json {common_space_volume_path/'startup.json'} "
            # f" --client-args {client_args} " # only potentially relevant arg is --debug-directory
        )

        return args

    @staticmethod
    def _get_token_from_keycloak(
        token_url: str,
        client_id: str,
        client_secret: str,
    ) -> str:
        if not token_url:  # would only be falsy in test
            return ""
        cca = ClientCredentialsAuth(
            "",
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
        )
        token = cca.make_access_token()
        return token

    @staticmethod
    def get_env(
        rest_address: str,
        scan_id: str,
    ) -> list[kubernetes.client.V1EnvVar]:
        """Get the environment variables provided to all containers.

        Also, get the secrets' keys & their values.
        """
        env = []

        # 1. start w/ secrets
        # NOTE: the values come from an existing secret in the current namespace
        env.append(get_condor_token_v1envvar())

        # 2. add required env vars
        required = {
            # broker/mq vars
            "SKYSCAN_BROKER_ADDRESS": ENV.SKYSCAN_BROKER_ADDRESS,
            # skydriver vars
            "SKYSCAN_SKYDRIVER_ADDRESS": rest_address,
            "SKYSCAN_SKYDRIVER_SCAN_ID": scan_id,
        }
        env.extend(
            [kubernetes.client.V1EnvVar(name=k, value=v) for k, v in required.items()]
        )

        # 3. add extra env vars, if present
        prefiltered = {
            "SKYSCAN_PROGRESS_INTERVAL_SEC": ENV.SKYSCAN_PROGRESS_INTERVAL_SEC,
            "SKYSCAN_RESULT_INTERVAL_SEC": ENV.SKYSCAN_RESULT_INTERVAL_SEC,
            "SKYSCAN_MQ_TIMEOUT_TO_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_TO_CLIENTS,
            "SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS": ENV.SKYSCAN_MQ_TIMEOUT_FROM_CLIENTS,
            "SKYSCAN_LOG": ENV.SKYSCAN_LOG,
            "SKYSCAN_LOG_THIRD_PARTY": ENV.SKYSCAN_LOG_THIRD_PARTY,
            "RABBITMQ_HEARTBEAT": ENV.RABBITMQ_HEARTBEAT,
        }
        env.extend(
            [
                kubernetes.client.V1EnvVar(name=k, value=v)
                for k, v in prefiltered.items()
                if v  # skip any env vars that are Falsy
            ]
        )

        # 4. generate & add auth tokens
        tokens = {
            "SKYSCAN_BROKER_AUTH": SkymapScannerStarterJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_BROKER,
                ENV.KEYCLOAK_CLIENT_SECRET_BROKER,
            ),
            "SKYSCAN_SKYDRIVER_AUTH": SkymapScannerStarterJob._get_token_from_keycloak(
                ENV.KEYCLOAK_OIDC_URL,
                ENV.KEYCLOAK_CLIENT_ID_SKYDRIVER_REST,
                ENV.KEYCLOAK_CLIENT_SECRET_SKYDRIVER_REST,
            ),
        }
        env.extend(
            [kubernetes.client.V1EnvVar(name=k, value=v) for k, v in tokens.items()]
        )

        return env

    def start_job(self) -> Any:
        """Start the k8s job."""
        KubeAPITools.start_job(self.api_instance, self.job_obj)


class SkymapScannerStopperJob:
    """Wraps a Kubernetes job to stop condor cluster(s) w/ Scanner clients."""

    def __init__(
        self,
        api_instance: kubernetes.client.BatchV1Api,
        scan_id: str,
        condor_clusters: list[schema.CondorClutser],
    ):
        self.api_instance = api_instance

        # make a container per cluster
        containers = []
        for i, cluster in enumerate(condor_clusters):
            args = (
                f"python -m clientmanager stop "
                f"--collector {cluster.collector} "
                f"--schedd {cluster.schedd} "
                f"--cluster-id {cluster.cluster_id} "
            )
            containers.append(
                KubeAPITools.create_container(
                    f"clientmanager-stop-{i}-{scan_id}",
                    ENV.CLIENTMANAGER_IMAGE_WITH_TAG,
                    env=[get_condor_token_v1envvar()],
                    args=args.split(),
                )
            )

        self.job_obj = KubeAPITools.kube_create_job_object(
            f"clientmanager-stop-{scan_id}",
            containers,
            ENV.K8S_NAMESPACE,
        )

    def start_job(self) -> Any:
        """Start the k8s job."""

        # TODO: stop first k8s job (server & clientmanager-starter)

        KubeAPITools.start_job(self.api_instance, self.job_obj)
