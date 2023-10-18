"""The post-argparse entry point for condor actions."""


import argparse

import htcondor  # type: ignore[import]

from .. import utils
from ..config import ENV, LOGGER
from . import condor_tools, starter, stopper, watcher


def act(args: argparse.Namespace) -> None:
    """Do the action."""
    htcondor.set_subsystem("TOOL")
    htcondor.param["TOOL_DEBUG"] = "D_FULLDEBUG"
    # htcondor.param["TOOL_LOG"] = "log.txt"
    # htcondor.enable_log()
    htcondor.enable_debug()

    # condor auth & go
    with htcondor.SecMan() as secman:
        secman.setToken(htcondor.Token(ENV.CONDOR_TOKEN))
        schedd_obj = condor_tools.get_schedd_obj(args.collector, args.schedd)
        _act(args, schedd_obj)


def _act(args: argparse.Namespace, schedd_obj: htcondor.Schedd) -> None:
    match args.action:
        case "start":
            LOGGER.info(
                f"Starting {args.n_workers} Skymap Scanner client workers on {args.collector} / {args.schedd}"
            )
            # make connections -- do now so we don't have any surprises downstream
            skydriver_rc = utils.connect_to_skydriver()
            # start
            submit_dict, submit_result_obj = starter.start(
                schedd_obj=schedd_obj,
                # starter CL args -- helper
                dryrun=args.dryrun,
                spool_logs_directory=args.spool_logs_directory
                if args.spool_logs_directory
                else None,
                # starter CL args -- worker
                memory=args.memory,
                n_cores=args.n_cores,
                n_workers=args.n_workers,
                # starter CL args -- client
                client_args=args.client_args,
                client_startup_json_s3=utils.s3ify(args.client_startup_json),
                image=args.image,
            )
            # report to SkyDriver
            utils.update_skydriver(
                skydriver_rc,
                "condor",
                location={
                    "collector": args.collector,
                    "schedd": args.schedd,
                },
                cluster_id=submit_result_obj.cluster(),
                n_workers=submit_result_obj.num_procs(),
                starter_info=submit_dict,
            )
            LOGGER.info("Sent cluster info to SkyDriver")
        case "watch":
            watcher.watch(
                args.collector,
                args.schedd,
                args.cluster_id,
                schedd_obj,
            )
        case "stop":
            stopper.stop(
                args.collector,
                args.schedd,
                args.cluster_id,
                schedd_obj,
            )
        case _:
            raise RuntimeError(f"Unknown action: {args.action}")
