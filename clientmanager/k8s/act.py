"""The post-argparse entry point for k8s actions."""


import argparse
import base64
import logging
import time
from tempfile import NamedTemporaryFile

import kubernetes  # type: ignore[import-untyped]

from .. import utils
from ..config import ENV, LOCAL_K8S_HOST
from . import starter, stopper

LOGGER = logging.getLogger(__name__)


def act(args: argparse.Namespace) -> None:
    """Do the action."""
    k8s_client_config = kubernetes.client.Configuration()

    # Creating K8S cluster client
    # Local
    if args.host == LOCAL_K8S_HOST:
        LOGGER.info("connecting to local k8s...")
        # use *this* pod's service account
        kubernetes.config.load_incluster_config(k8s_client_config)
    # Using config file + token
    elif ENV.WORKER_K8S_CONFIG_FILE_BASE64 and ENV.WORKER_K8S_TOKEN:
        LOGGER.info("connecting to remote k8s via config file + token...")
        # connect to remote host
        with NamedTemporaryFile(delete=False) as tempf:
            tempf.write(base64.b64decode(ENV.WORKER_K8S_CONFIG_FILE_BASE64))
            LOGGER.info("loading k8s configuration...")
            kubernetes.config.load_kube_config(
                config_file=tempf.name,
                client_configuration=k8s_client_config,
            )
        k8s_client_config.host = args.host
        k8s_client_config.api_key["authorization"] = ENV.WORKER_K8S_TOKEN
    # Using CA cert + token
    elif ENV.WORKER_K8S_CACERT and ENV.WORKER_K8S_TOKEN:
        # https://medium.com/@jankrynauw/run-a-job-on-google-kubernetes-engine-using-the-python-client-library-and-not-kubectl-4ee8bdd55b1b
        LOGGER.info("connecting to remote k8s via ca cert + token...")
        with NamedTemporaryFile(delete=False) as tempf:
            tempf.write(base64.b64decode(ENV.WORKER_K8S_CACERT))
            k8s_client_config.ssl_ca_cert = tempf.name
        k8s_client_config.host = args.host
        k8s_client_config.verify_ssl = True
        k8s_client_config.debug = True  # remove?
        k8s_client_config.api_key = {"authorization": "Bearer " + ENV.WORKER_K8S_TOKEN}
        k8s_client_config.assert_hostname = False
        kubernetes.client.Configuration.set_default(k8s_client_config)
    else:
        raise RuntimeError(
            f"Did not provide sufficient configuration to connect to {args.host}"
        )

    # connect & go
    with kubernetes.client.ApiClient(k8s_client_config) as k8s_api:
        try:
            LOGGER.debug("testing k8s credentials")
            resp = kubernetes.client.BatchV1Api(k8s_api).get_api_resources()
            LOGGER.debug(resp)
        except kubernetes.client.rest.ApiException as e:
            LOGGER.exception(e)
            raise
        _act(args, k8s_api)


def _act(args: argparse.Namespace, k8s_api: kubernetes.client.ApiClient) -> None:
    match args.action:
        case "start":
            cluster_id = f"skyscan-worker-{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}-{int(time.time())}"  # TODO: make more unique
            LOGGER.info(
                f"Starting {args.n_workers} Skymap Scanner client workers on "
                f"{args.host}/{args.namespace}/{cluster_id}"
            )
            # make connections -- do now so we don't have any surprises downstream
            skydriver_rc = utils.connect_to_skydriver()
            # start
            k8s_job_dict = starter.prep(
                cluster_id=cluster_id,
                # k8s CL args
                cpu_arch=args.cpu_arch,
                host=args.host,
                job_config_stub=args.job_config_stub,
                namespace=args.namespace,
                # starter CL args -- worker
                worker_memory_bytes=args.worker_memory_bytes,
                worker_disk_bytes=args.worker_disk_bytes,
                n_cores=args.n_cores,
                n_workers=args.n_workers,
                # starter CL args -- client
                client_args=args.client_args if args.client_args else [],
                client_startup_json_s3=utils.s3ify(args.client_startup_json),
                container_image=args.image,
            )
            # final checks
            if args.dryrun:
                LOGGER.critical("Script Aborted: dryrun enabled")
                return
            if utils.skydriver_aborted_scan(skydriver_rc):
                LOGGER.critical("Script Aborted: SkyDriver aborted scan")
                return
            # start
            k8s_job_dict = starter.start(
                k8s_api,
                k8s_job_dict,
                cluster_id,
                args.host,
                args.namespace,
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                "k8s",
                location={
                    "host": args.host,
                    "namespace": args.namespace,
                },
                uuid=args.uuid,
                cluster_id=cluster_id,
                n_workers=args.n_workers,
                starter_info=k8s_job_dict,
            )
            LOGGER.info("Sent cluster info to SkyDriver")
        case "stop":
            LOGGER.info(
                f"Stopping Skymap Scanner client workers on "
                f"{args.host}/{args.namespace}/{args.cluster_id}"
            )
            stopper.stop(
                args.namespace,
                args.cluster_id,
                k8s_api,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
