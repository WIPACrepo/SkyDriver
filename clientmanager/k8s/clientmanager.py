"""The central module."""


import argparse

import kubernetes  # type: ignore[import]
from wipac_dev_tools import logging_tools

from . import k8s_tools, starter, stopper, utils
from ..config import ENV, LOGGER


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Manage Skymap Scanner clients as K8S jobs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        required=True,
        dest="action",
        help="clientmanager action",
    )
    parser.add_argument(
        "--cluster-config",
        default="",
        help="k8s cluster config to connect as a yaml file",
    )
    parser.add_argument(
        "--job-config",
        default="",
        help="k8s cluster config to run as a yaml file",
    )
    parser.add_argument(
        "--namespace",
        default="",
        help="namespace to use",
    )
    starter.attach_sub_parser_args(subparsers.add_parser("start", help="start jobs"))
    stopper.attach_sub_parser_args(subparsers.add_parser("stop", help="stop jobs"))

    # parse args & set up logging
    args = parser.parse_args()
    logging_tools.set_level(
        "DEBUG",  # os.getenv("SKYSCAN_LOG", "INFO"),  # type: ignore[arg-type]
        first_party_loggers=LOGGER,
        third_party_level=ENV.SKYSCAN_LOG_THIRD_PARTY,  # type: ignore[arg-type]
        use_coloredlogs=True,  # for formatting
        future_third_parties=["boto3", "botocore"],
    )
    logging_tools.log_argparse_args(args, logger=LOGGER, level="WARNING")

    ####################################################################################

    # Creating K8S cluster client
    k8s_client_config = kubernetes.client.Configuration()
    k8s_client_config.host = ENV.K8S_SERVER
    k8s_client_config.api_key['authorization'] = ENV.K8S_TOKEN
    with kubernetes.client.ApiClient(k8s_client_config) as k8s_api_client:
        # Go!
        act(args, k8s_api_client, namespace)

def act(args: argparse.Namespace, 
        k8s_client: kubernetes.client.ApiClient, 
        k8s_namespace: str) -> None:
    """Do the action."""
    match args.action:
        case "start":
            LOGGER.info(
                f"Starting {args.n_jobs} Skymap Scanner client jobs on {args.collector} / {args.schedd}"
            )
            # make connections -- do now so we don't have any surprises downstream
            skydriver_rc = utils.connect_to_skydriver()
            # start
            submit_result_obj = starter.start(
                k8s_client,
                args.namespace,
                args.name,
                args.n_jobs,
                args.client_args,
                args.memory,
                args.accounting_group,
                args.singularity_image,
                # put client_startup_json in S3 bucket
                utils.s3ify(args.client_startup_json),
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                submit_result_obj,
                args.collector,
                args.schedd,
            )
            LOGGER.info("Sent cluster info to SkyDriver")
        case "stop":
            stopper.stop(
                args,
                k8s_client,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
