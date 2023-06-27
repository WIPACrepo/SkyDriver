"""The post-argparse entry point for k8s actions."""


import argparse
import base64
import time
from tempfile import NamedTemporaryFile

import kubernetes  # type: ignore[import]

from .. import utils
from ..config import ENV, LOCAL_K8S_HOST, LOGGER
from . import starter, stopper


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
        LOGGER.info("connecting to remote k8s via ca cert + token...")
        with NamedTemporaryFile(delete=False) as tempf:
            tempf.write(base64.b64decode(ENV.WORKER_K8S_CACERT))
            k8s_client_config.ssl_ca_cert = tempf.name
        k8s_client_config.host = args.host
        k8s_client_config.verify_ssl = True
        k8s_client_config.debug = False
        k8s_client_config.api_key = {"authorization": "Bearer " + ENV.WORKER_K8S_TOKEN}
        kubernetes.client.Configuration.set_default(k8s_client_config)
    else:
        raise RuntimeError(
            f"Did not provide sufficient configuration to connect to {args.host}"
        )

    # connect & go
    with kubernetes.client.ApiClient(k8s_client_config) as k8s_client:
        try:
            LOGGER.debug("testing k8s credentials")
            api_response = kubernetes.client.BatchV1Api(k8s_client).get_api_resources()
            LOGGER.debug(api_response)
        except kubernetes.client.rest.ApiException as e:
            LOGGER.exception(e)
            raise
        _act(args, k8s_client)


def _act(args: argparse.Namespace, k8s_client: kubernetes.client.ApiClient) -> None:
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
            starter.start(
                k8s_client=k8s_client,
                job_config_stub=args.job_config_stub,
                host=args.host,
                namespace=args.namespace,
                cluster_id=cluster_id,
                n_workers=args.n_workers,
                n_cores=args.n_cores,
                client_args=args.client_args if args.client_args else [],
                memory=args.memory,
                container_image=args.image,
                # put client_startup_json in S3 bucket
                client_startup_json_s3=utils.s3ify(args.client_startup_json),
                dryrun=args.dryrun,
                cpu_arch=args.cpu_arch,
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                "k8s",
                location={
                    "host": args.host,
                    "namespace": args.namespace,
                },
                cluster_id=cluster_id,
                n_workers=args.n_workers,
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
                k8s_client,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
