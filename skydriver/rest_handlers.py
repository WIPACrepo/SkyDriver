"""Handlers for the SkyDriver REST API server interface."""

import argparse
import asyncio
import dataclasses as dc
import json
import logging
import pickle
import re
import time
import uuid
from typing import Any, Type, TypeVar

import humanfriendly
import kubernetes.client  # type: ignore[import-untyped]
from dacite import from_dict
from dacite.exceptions import DaciteError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ReturnDocument
from rest_tools.client import RestClient
from rest_tools.server import (
    ArgumentHandler,
    ArgumentSource,
    RestHandler,
    token_attribute_role_mapping_auth,
)
from tornado import web
from wipac_dev_tools import argparse_tools

from . import database, ewms, images, k8s, utils
from .config import (
    DEFAULT_K8S_CONTAINER_MEMORY_SKYSCAN_SERVER_BYTES,
    DEFAULT_MAX_WORKER_RUNTIME,
    DEFAULT_WORKER_DISK_BYTES,
    DEFAULT_WORKER_MEMORY_BYTES,
    DebugMode,
    ENV,
    KNOWN_CLUSTERS,
    is_testing,
)
from .database import schema
from .database.schema import PENDING_EWMS_WORKFLOW
from .ewms import request_stop_on_ewms
from .k8s.scan_backlog import put_on_backlog
from .k8s.scanner_instance import SkyScanK8sJobFactory
from .utils import get_scan_state

LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# constants


REAL_CHOICES = ["real", "real_event"]
SIM_CHOICES = ["sim", "simulated", "simulated_event"]

MAX_CLASSIFIERS_LEN = 25

WAIT_BEFORE_TEARDOWN = 60

# -----------------------------------------------------------------------------
# REST requestor auth


USER_ACCT = "user"
SKYMAP_SCANNER_ACCT = "system"

if is_testing():

    def service_account_auth(roles: list[str], **kwargs):  # type: ignore
        def make_wrapper(method):  # type: ignore[no-untyped-def]
            async def wrapper(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                LOGGER.warning("TESTING: auth disabled")
                self.auth_roles = [roles[0]]  # make as a list containing just 1st role
                return await method(self, *args, **kwargs)

            return wrapper

        return make_wrapper

else:
    service_account_auth = token_attribute_role_mapping_auth(  # type: ignore[no-untyped-call]
        role_attrs={
            USER_ACCT: ["groups=/institutions/IceCube.*"],
            SKYMAP_SCANNER_ACCT: ["skydriver_role=system"],
        }
    )


# -----------------------------------------------------------------------------
# utils


def all_dc_fields(class_or_instance: Any) -> set[str]:
    """Get all the field names for a dataclass (instance or class)."""
    return set(f.name for f in dc.fields(class_or_instance))


def dict_projection(dicto: dict, projection: set[str] | list[str]) -> dict:
    """Keep only the keys in the `projection`.

    If `projection` is empty or includes '*', return all fields.
    """
    if "*" in projection:
        return dicto
    if not projection:
        return dicto
    return {k: v for k, v in dicto.items() if k in projection}


def _arg_dict_strict(val: Any) -> dict:
    if not isinstance(val, dict):
        raise argparse.ArgumentTypeError("arg must be a dict")
    return val


# -----------------------------------------------------------------------------
# handlers


class BaseSkyDriverHandler(RestHandler):  # pylint: disable=W0223
    """BaseSkyDriverHandler is a RestHandler for all SkyDriver routes."""

    def initialize(  # type: ignore  # pylint: disable=W0221
        self,
        mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
        k8s_batch_api: kubernetes.client.BatchV1Api,
        ewms_rc: RestClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)  # type: ignore[no-untyped-call]
        # pylint: disable=W0201
        self.manifests = database.interface.ManifestClient(mongo_client)
        self.results = database.interface.ResultClient(mongo_client)
        self.scan_backlog = database.interface.ScanBacklogClient(mongo_client)
        self.scan_request_coll = (
            AsyncIOMotorCollection(  # in contrast, this one is accessed directly
                mongo_client[database.interface._DB_NAME],  # type: ignore[index]
                database.utils._SCAN_REQUEST_COLL_NAME,
            )
        )
        self.i3_event_coll = (
            AsyncIOMotorCollection(  # in contrast, this one is accessed directly
                mongo_client[database.interface._DB_NAME],  # type: ignore[index]
                database.utils._I3_EVENT_COLL_NAME,
            )
        )
        self.skyscan_k8s_job_coll = (
            AsyncIOMotorCollection(  # in contrast, this one is accessed directly
                mongo_client[database.interface._DB_NAME],  # type: ignore[index]
                database.utils._SKYSCAN_K8S_JOB_COLL_NAME,
            )
        )
        self.k8s_batch_api = k8s_batch_api
        self.ewms_rc = ewms_rc


# ----------------------------------------------------------------------------


class MainHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """MainHandler is a BaseSkyDriverHandler that handles the root route."""

    ROUTE = r"/$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self) -> None:
        """Handle GET."""
        self.write({})


# -----------------------------------------------------------------------------


class ScansFindHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles finding scans by attributes."""

    ROUTE = r"/scans/find$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def post(self) -> None:
        """Get matching scan manifest(s) for the given search."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        arghand.add_argument(
            "filter",
            type=_arg_dict_strict,
        )
        arghand.add_argument(
            "include_deleted",
            default=False,
            type=bool,
        )
        # response args
        arghand.add_argument(
            "manifest_projection",
            default=all_dc_fields(database.schema.Manifest),
            type=str,
        )
        args = arghand.parse_args()

        if "is_deleted" not in args.filter and not args.include_deleted:
            args.filter["is_deleted"] = False

        manifests = [
            dict_projection(dc.asdict(m), args.manifest_projection)
            async for m in self.manifests.find_all(args.filter)
        ]

        self.write({"manifests": manifests})

    #
    # NOTE - handler needs to stay user-read-only
    #


# -----------------------------------------------------------------------------


class ScanBacklogHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles looking at backlog."""

    ROUTE = r"/scans/backlog$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self) -> None:
        """Get all scan id(s) in the backlog."""
        entries = [e async for e in self.scan_backlog.get_all()]

        self.write({"entries": entries})

    #
    # NOTE - 'ScanBacklogHandler' needs to stay user-read-only b/c
    #         it's indirectly updated by the launching of a new scan
    #


# -----------------------------------------------------------------------------


def _json_to_dict(val: Any) -> dict:
    _error = argparse.ArgumentTypeError("must be JSON-string or JSON-friendly dict")
    # str -> json-dict
    if isinstance(val, str):
        try:
            obj = json.loads(val)
        except:  # noqa: E722
            raise _error
        if not isinstance(obj, dict):  # loaded object must be dict
            raise _error
        return obj
    # dict -> check if json-friendly
    elif isinstance(val, dict):
        try:
            json.dumps(val)
            return val
        except:  # noqa: E722
            raise _error
    # fall-through
    raise _error


def _validate_request_clusters(
    val: dict | list,
) -> list[tuple[str, int]]:
    _error = argparse.ArgumentTypeError(
        "must be a dict of cluster location and number of workers, Ex: {'sub-2': 1500, ...}"
        " (to request a cluster location more than once, provide a list of 2-lists instead)"
        # TODO: make n_workers optional when using "EWMS smart starter"
    )
    if isinstance(val, dict):
        # {'a': 1, 'b': 2} -> [('a', 1), ('b', 2)}
        list_tups: list[tuple[str, int]] = list(val.items())
    else:
        list_tups = val
    del val

    # validate
    if not list_tups:
        raise _error
    if not isinstance(list_tups, list):
        raise _error
    # check all entries are 2-lists (or tuple)
    if not all(isinstance(a, list | tuple) and len(a) == 2 for a in list_tups):
        raise _error
    # check that all locations are known (this validates sooner than ewms)
    for name, n_workers in list_tups:
        if name not in KNOWN_CLUSTERS:
            raise argparse.ArgumentTypeError(
                f"requested unknown cluster: {name} (available:"
                f" {', '.join(KNOWN_CLUSTERS.keys())})"
            )

    return list_tups


def _classifiers_validator(val: Any) -> dict[str, str | bool | float | int]:
    # type checks
    if not isinstance(val, dict):
        raise argparse.ArgumentTypeError("must be a dict")
    if any(v for v in val.values() if not isinstance(v, str | bool | float | int)):
        raise argparse.ArgumentTypeError("entry must be 'str | bool | float | int'")

    # size check
    if len(val) > MAX_CLASSIFIERS_LEN:
        raise argparse.ArgumentTypeError(
            f"must be at most {MAX_CLASSIFIERS_LEN} entries long"
        )
    for key, subval in val.items():
        if len(key) > MAX_CLASSIFIERS_LEN:
            raise argparse.ArgumentTypeError(
                f"key must be at most {MAX_CLASSIFIERS_LEN} characters long"
            )
        try:
            if len(subval) > MAX_CLASSIFIERS_LEN:
                raise argparse.ArgumentTypeError(
                    f"str-field must be at most {MAX_CLASSIFIERS_LEN} characters long"
                )
        except TypeError:
            pass  # not a str

    return val


def _debug_mode(val: Any) -> list[DebugMode]:
    if not isinstance(val, list):
        val = [val]
    return [DebugMode(v) for v in val]  # -> ValueError


def _data_size_parse(val: Any) -> int:
    try:
        return humanfriendly.parse_size(str(val))  # type: ignore[no-any-return]
    except humanfriendly.InvalidSize:
        raise argparse.ArgumentTypeError("invalid data size")


class ScanLauncherHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles starting new scans."""

    ROUTE = r"/scan$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def post(self) -> None:
        """Start a new scan."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        # docker args
        arghand.add_argument(
            # any tag on docker hub (including 'latest') -- must also be on CVMFS (but not checked here)
            "docker_tag",
            type=images.resolve_docker_tag,
        )
        # scanner server args
        arghand.add_argument(
            "scanner_server_memory",
            type=_data_size_parse,
            default=DEFAULT_K8S_CONTAINER_MEMORY_SKYSCAN_SERVER_BYTES,
        )
        # client worker args
        arghand.add_argument(
            "worker_memory",
            type=_data_size_parse,
            default=DEFAULT_WORKER_MEMORY_BYTES,
        )
        arghand.add_argument(  # NOTE - DEPRECATED
            "memory",
            type=lambda x: argparse_tools.validate_arg(
                x,
                not bool(x),  # False if given
                argparse.ArgumentTypeError(
                    "argument is deprecated--use 'worker_memory'"
                ),
            ),
            default=None,
        )
        arghand.add_argument(
            "worker_disk",
            type=_data_size_parse,
            default=DEFAULT_WORKER_DISK_BYTES,
        )
        arghand.add_argument(
            "cluster",
            type=_validate_request_clusters,
        )
        # scanner args
        arghand.add_argument(
            "reco_algo",
            type=lambda x: argparse_tools.validate_arg(
                x,
                bool(re.match(r"\S", x)),  # no empty string / whitespace
                argparse.ArgumentTypeError("cannot be empty string / whitespace"),
            ),
        )
        arghand.add_argument(
            "event_i3live_json",
            type=_json_to_dict,  # JSON-string/JSON-friendly dict -> dict
        )
        arghand.add_argument(
            "nsides",
            type=_arg_dict_strict,
        )
        arghand.add_argument(
            "real_or_simulated_event",  # as opposed to simulation
            type=str,
            choices=REAL_CHOICES + SIM_CHOICES,
        )
        arghand.add_argument(
            "predictive_scanning_threshold",
            type=float,
            default=1.0,
        )
        arghand.add_argument(
            "max_pixel_reco_time",
            type=int,
        )
        arghand.add_argument(
            "max_worker_runtime",
            type=int,
            default=DEFAULT_MAX_WORKER_RUNTIME,
        )
        arghand.add_argument(
            # TODO - remove when TMS is handling workforce-scaling
            "skyscan_mq_client_timeout_wait_for_first_message",
            type=int,
            default=-1,  # elephant in Cairo, see below
        )
        arghand.add_argument(
            "debug_mode",
            type=_debug_mode,
            default=[],
        )
        # other args
        arghand.add_argument(
            "classifiers",
            type=_classifiers_validator,
            default={},
        )
        arghand.add_argument(
            "priority",
            type=int,
            default=0,
        )
        arghand.add_argument(
            "scanner_server_env",
            type=_classifiers_validator,  # piggy-back this validator
            default={},
        )
        # response args
        arghand.add_argument(
            "manifest_projection",
            default=all_dc_fields(database.schema.Manifest),
            type=str,
        )
        args = arghand.parse_args()

        # more arg validation
        if DebugMode.CLIENT_LOGS in args.debug_mode:
            for cname, cworkers in args.cluster:
                if cworkers > (
                    val := KNOWN_CLUSTERS[cname].get(
                        "max_n_clients_during_debug_mode", float("inf")
                    )
                ):
                    raise web.HTTPError(
                        400,
                        log_message=(
                            f"Too many workers: Cluster '{cname}' can only have "
                            f"{val} "
                            f"workers when 'debug_mode' "
                            f"includes '{DebugMode.CLIENT_LOGS.value}'"
                        ),
                    )

        # generate unique scan_id
        scan_id = uuid.uuid4().hex

        # Before doing anything else, persist in DB
        # -> store the event in its own collection to reduce redundancy
        i3_event_id = uuid.uuid4().hex
        await self.i3_event_coll.insert_one(
            {
                "i3_event_id": i3_event_id,
                "json_dict": args.event_i3live_json,  # this was transformed into dict
            }
        )
        # -> store scan_request_obj in db
        scan_request_obj = dict(
            scan_id=scan_id,
            rescan_ids=[],
            #
            docker_tag=args.docker_tag,
            #
            # skyscan server config
            scanner_server_memory_bytes=args.scanner_server_memory,  # already in bytes
            reco_algo=args.reco_algo,
            nsides=args.nsides,
            real_or_simulated_event=args.real_or_simulated_event,
            predictive_scanning_threshold=args.predictive_scanning_threshold,
            #
            classifiers=args.classifiers,
            #
            # cluster (condor) config
            request_clusters=args.cluster,  # a list
            worker_memory_bytes=args.worker_memory,
            worker_disk_bytes=args.worker_disk,  # already in bytes
            max_pixel_reco_time=args.max_pixel_reco_time,
            max_worker_runtime=args.max_worker_runtime,
            priority=args.priority,
            debug_mode=[d.value for d in args.debug_mode],
            #
            # misc
            skyscan_mq_client_timeout_wait_for_first_message=(
                args.skyscan_mq_client_timeout_wait_for_first_message
                if args.skyscan_mq_client_timeout_wait_for_first_message != -1
                else None
            ),
            i3_event_id=i3_event_id,  # foreign key to i3_event collection
            rest_address=self.request.full_url().rstrip(self.request.uri),
            scanner_server_env_from_user=args.scanner_server_env,
        )
        await self.scan_request_coll.insert_one(scan_request_obj)

        # go!
        manifest = await _start_scan(
            self.manifests,
            self.scan_backlog,
            self.skyscan_k8s_job_coll,
            scan_request_obj,
        )
        self.write(
            dict_projection(dc.asdict(manifest), args.manifest_projection),
        )


async def _start_scan(
    manifests: database.interface.ManifestClient,
    scan_backlog: database.interface.ScanBacklogClient,
    skyscan_k8s_job_coll: AsyncIOMotorCollection,  # type: ignore[valid-type]
    scan_request_obj: dict,
    new_scan_id: str = "",  # don't use scan_request_obj.scan_id--this could be a rescan
) -> schema.Manifest:
    scan_id = new_scan_id or scan_request_obj["scan_id"]

    # get the container info ready
    skyscan_k8s_job_dict, scanner_server_args = SkyScanK8sJobFactory.make(
        docker_tag=scan_request_obj["docker_tag"],
        scan_id=scan_id,
        # server
        scanner_server_memory_bytes=scan_request_obj["scanner_server_memory_bytes"],
        reco_algo=scan_request_obj["reco_algo"],
        nsides=scan_request_obj["nsides"],
        is_real_event=scan_request_obj["real_or_simulated_event"] in REAL_CHOICES,
        predictive_scanning_threshold=scan_request_obj["predictive_scanning_threshold"],
        # universal
        debug_mode=_debug_mode(scan_request_obj["debug_mode"]),
        # env
        rest_address=scan_request_obj["rest_address"],
        skyscan_mq_client_timeout_wait_for_first_message=scan_request_obj[
            "skyscan_mq_client_timeout_wait_for_first_message"
        ],
        scanner_server_env_from_user=scan_request_obj["scanner_server_env_from_user"],
    )

    # put in db (do before k8s start so if k8s fail, we can debug using db's info)
    LOGGER.debug("creating new manifest")
    manifest = schema.Manifest(
        scan_id=scan_id,
        timestamp=time.time(),
        is_deleted=False,
        i3_event_id=scan_request_obj["i3_event_id"],
        scanner_server_args=scanner_server_args,
        ewms_workflow_id=schema.PENDING_EWMS_WORKFLOW,
        # ^^^ set once the workflow request has been sent to EWMS (see backlogger)
        classifiers=scan_request_obj["classifiers"],
        priority=scan_request_obj["priority"],
    )
    manifest = await manifests.put(manifest)
    await skyscan_k8s_job_coll.insert_one(  # type: ignore[attr-defined]
        {
            "scan_id": scan_id,
            "skyscan_k8s_job_dict": skyscan_k8s_job_dict,
        }
    )

    # place on backlog
    await put_on_backlog(
        scan_id,
        scan_backlog,
        scan_request_obj["priority"],
    )

    return manifest


# -----------------------------------------------------------------------------


class ScanRescanHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on copying a scan's manifest and starting that."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/actions/rescan$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def post(self, scan_id: str) -> None:
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        # response args
        arghand.add_argument(
            "manifest_projection",
            default=all_dc_fields(database.schema.Manifest),
            type=str,
        )
        args = arghand.parse_args()

        # generate unique scan_id
        new_scan_id = uuid.uuid4().hex

        # grab the original requester's 'scan_request_obj'
        doc = await self.scan_request_coll.find_one_and_update(
            {"scan_id": scan_id},
            {"$push": {"rescan_ids": new_scan_id}},
            return_document=ReturnDocument.AFTER,
        )
        # -> backup plan: was this scan_id actually a rescan itself?
        if not doc:
            doc = await self.scan_request_coll.find_one_and_update(
                {"rescan_ids": scan_id},  # one in a list
                {"$push": {"rescan_ids": new_scan_id}},
                return_document=ReturnDocument.AFTER,
            )
        # -> error: couldn't find it anywhere
        if not doc:
            raise web.HTTPError(
                404,
                log_message="Could not find original scan-request information to start a rescan",
            )
        scan_request_obj = pickle.loads(doc["scan_request_obj_pkl"])

        # add to 'classifiers' so the user has provenance info
        scan_request_obj["classifiers"].update(
            {"rescan": True, "origin_scan_id": scan_id}
        )

        # go!
        manifest = await _start_scan(
            self.manifests,
            self.scan_backlog,
            self.skyscan_k8s_job_coll,
            scan_request_obj,
            new_scan_id=new_scan_id,
        )
        self.write(
            dict_projection(dc.asdict(manifest), args.manifest_projection),
        )


# -----------------------------------------------------------------------------


async def stop_skyscan_workers(
    manifests: database.interface.ManifestClient,
    scan_id: str,
    ewms_rc: RestClient,
    abort: bool,
) -> database.schema.Manifest:
    """Stop all parts of the Scanner instance (if running) and mark in DB."""
    manifest = await manifests.get(scan_id, True)

    # request to ewms
    if manifest.ewms_workflow_id:
        if manifest.ewms_workflow_id == schema.PENDING_EWMS_WORKFLOW:
            LOGGER.info(
                "OK: attempted to stop skyscan workers but scan has not been sent to EWMS"
            )
        else:
            await request_stop_on_ewms(ewms_rc, manifest.ewms_workflow_id, abort=abort)
    else:
        raise web.HTTPError(
            400,
            log_message="Could not stop scanner workers since this is a non-EWMS scan.",
        )

    return manifest


# -----------------------------------------------------------------------------


async def get_result_safely(
    manifests: database.interface.ManifestClient,
    results: database.interface.ResultClient,
    scan_id: str,
    incl_del: bool,
) -> tuple[None | database.schema.Result, database.schema.Manifest]:
    """Get the Result (and Manifest) using the incl_del/is_deleted logic.

    Returns objects as dicts
    """
    manifest = await manifests.get(scan_id, incl_del)  # 404 if missing

    # check if requestor allows a deleted scan's result
    if (not incl_del) and manifest.is_deleted:
        raise web.HTTPError(
            404,
            log_message=f"Requested result with deleted manifest: {manifest.scan_id}",
        )

    # if we don't have a result yet, return {}
    try:
        result = await results.get(scan_id)
    except web.HTTPError as e:
        if e.status_code != 404:
            raise
        result = None

    return result, manifest


# -----------------------------------------------------------------------------


class ScanHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Abort a scan and/or mark manifest & result as "deleted"."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        arghand.add_argument(
            "delete_completed_scan",
            default=False,
            type=bool,
        )
        # response args
        arghand.add_argument(
            "manifest_projection",
            default=all_dc_fields(database.schema.Manifest),
            type=str,
        )
        args = arghand.parse_args()

        # check DB states
        manifest = await self.manifests.get(scan_id, True)
        if (
            manifest.progress
            and manifest.progress.processing_stats.finished
            and not args.delete_completed_scan
        ):
            msg = "Attempted to delete a completed scan (must use `delete_completed_scan=True`)"
            raise web.HTTPError(
                400,
                log_message=msg,
                reason=msg,
            )

        # mark as deleted -> also stops backlog from starting
        manifest = await self.manifests.mark_as_deleted(scan_id)
        # abort
        await stop_skyscan_workers(self.manifests, scan_id, self.ewms_rc, abort=True)

        try:
            result_dict = dc.asdict(await self.results.get(scan_id))
        except web.HTTPError as e:
            if e.status_code != 404:
                raise
            result_dict = {}

        self.write(
            {
                "manifest": dict_projection(
                    dc.asdict(manifest), args.manifest_projection
                ),
                "result": result_dict,
            }
        )

    @service_account_auth(roles=[USER_ACCT, SKYMAP_SCANNER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get manifest & result."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        arghand.add_argument(
            "include_deleted",
            default=False,
            type=bool,
        )
        args = arghand.parse_args()

        result, manifest = await get_result_safely(
            self.manifests,
            self.results,
            scan_id,
            args.include_deleted,
        )

        self.write(
            {
                "manifest": dc.asdict(manifest),
                "result": dc.asdict(result) if result else {},
            }
        )


# -----------------------------------------------------------------------------


class ScanManifestHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/manifest$"

    @service_account_auth(roles=[USER_ACCT, SKYMAP_SCANNER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get scan progress."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        arghand.add_argument(
            "include_deleted",
            default=False,
            type=bool,
        )
        # response args
        arghand.add_argument(
            "projection",
            default=all_dc_fields(database.schema.Manifest),
            type=str,
        )
        args = arghand.parse_args()

        # get manifest from db
        manifest = await self.manifests.get(scan_id, args.include_deleted)

        # Backward Compatibility for Skymap Scanner:
        #   Include the whole event dict in the response like the 'old' manifest.
        #   This overrides the manifest's field which should be an id.
        if (
            self.auth_roles[0] == SKYMAP_SCANNER_ACCT  # type: ignore
            and "event_i3live_json_dict" in args.projection
            and manifest.i3_event_id  # if no id, then event already in manifest
        ):
            if i3event_doc := await self.i3_event_coll.find_one(
                {"i3_event_id": manifest.i3_event_id}
            ):
                manifest.event_i3live_json_dict = i3event_doc["json_dict"]
            else:  # this would mean the event was removed from the db
                error_msg = (
                    f"No i3 event document found with id '{manifest.i3_event_id}'"
                    f"--if other fields are wanted, re-request using 'projection'"
                )
                raise web.HTTPError(
                    404,
                    log_message=error_msg,
                    reason=error_msg,
                )

        resp = dict_projection(dc.asdict(manifest), args.projection)
        self.write(resp)

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def patch(self, scan_id: str) -> None:
        """Update scan progress."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)

        T = TypeVar("T")

        def from_dict_wrapper_or_none(data_class: Type[T], val: Any) -> T | None:
            if not val:
                return None
            try:
                return from_dict(data_class, val)
            except DaciteError as e:
                raise argparse.ArgumentTypeError(str(e))

        arghand.add_argument(
            "progress",
            type=lambda x: from_dict_wrapper_or_none(database.schema.Progress, x),
            default=None,
        )
        arghand.add_argument(
            "event_metadata",
            type=lambda x: from_dict_wrapper_or_none(database.schema.EventMetadata, x),
            default=None,
        )
        arghand.add_argument(
            "scan_metadata",
            type=dict,
            default={},
        )
        args = arghand.parse_args()

        manifest = await self.manifests.patch(
            scan_id,
            args.progress,
            args.event_metadata,
            args.scan_metadata,
        )

        self.write(dc.asdict(manifest))  # don't use a projection


# -----------------------------------------------------------------------------


class ScanI3EventHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles grabbing i3 events using scan ids."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/i3-event$"

    @service_account_auth(roles=[USER_ACCT, SKYMAP_SCANNER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get scan's i3 event."""
        manifest = await self.manifests.get(scan_id, True)

        # look up event in collection
        if manifest.i3_event_id:
            doc = await self.i3_event_coll.find_one(
                {"i3_event_id": manifest.i3_event_id}
            )
            if doc:
                i3_event = doc["json_dict"]
            else:  # this would mean the event was removed from the db
                error_msg = (
                    f"No i3 event document found with id '{manifest.i3_event_id}'"
                )
                raise web.HTTPError(
                    404,
                    log_message=error_msg,
                    reason=error_msg,
                )
        # unless, this is an old scan -- where the whole dict was stored w/ the manifest
        else:
            i3_event = manifest.event_i3live_json_dict

        self.write({"i3_event": i3_event})

    #
    # NOTE - handler needs to stay user-read-only
    #
    # FUTURE - add delete?
    #


# -----------------------------------------------------------------------------


class ScanResultHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on persisted scan results."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/result$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's persisted result."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        arghand.add_argument(
            "include_deleted",
            default=False,
            type=bool,
        )
        args = arghand.parse_args()

        result, _ = await get_result_safely(
            self.manifests,
            self.results,
            scan_id,
            args.include_deleted,
        )

        self.write(dc.asdict(result) if result else {})

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def put(self, scan_id: str) -> None:
        """Put (persist) a scan's result."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        arghand.add_argument(
            "skyscan_result",
            type=_arg_dict_strict,
        )
        arghand.add_argument(
            "is_final",
            type=bool,
        )
        args = arghand.parse_args()

        if not args.skyscan_result:
            self.write({})
            return

        result_dc = await self.results.put(
            scan_id,
            args.skyscan_result,
            args.is_final,
        )
        self.write(dc.asdict(result_dc))

        # END #
        self.finish()
        # AFTER RESPONSE #

        # when we get the final result, it's time to tear down
        if args.is_final:
            await asyncio.sleep(
                WAIT_BEFORE_TEARDOWN
            )  # regular time.sleep() sleeps the entire server
            await stop_skyscan_workers(
                self.manifests,
                scan_id,
                self.ewms_rc,
                abort=False,
            )


# -----------------------------------------------------------------------------


class ScanStatusHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles relying statuses for scans."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/status$"

    @service_account_auth(roles=[USER_ACCT, SKYMAP_SCANNER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's status."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        arghand.add_argument(
            "include_pod_statuses",
            type=bool,
            default=False,
        )
        args = arghand.parse_args()

        manifest = await self.manifests.get(scan_id, incl_del=True)

        # get pod status
        pods_411: dict[str, Any] = {}
        if args.include_pod_statuses:
            try:
                pods_411["pod_status"] = k8s.utils.KubeAPITools.get_pod_status(
                    self.k8s_batch_api,
                    SkyScanK8sJobFactory.get_job_name(scan_id),
                    ENV.K8S_NAMESPACE,
                )
                pods_411["pod_message"] = "retrieved"
            except (kubernetes.client.rest.ApiException, ValueError) as e:
                if await self.scan_backlog.is_in_backlog(scan_id):
                    pods_411["pod_status"] = {}
                    pods_411["pod_message"] = "in backlog"
                else:
                    pods_411["pod_status"] = {}
                    pods_411["pod_message"] = "pod(s) not found"
                    LOGGER.exception(e)

        # scan state
        scan_state = await get_scan_state(manifest, self.ewms_rc, self.results)

        # ewms
        if (
            manifest.ewms_workflow_id
            and manifest.ewms_workflow_id != PENDING_EWMS_WORKFLOW
        ):
            clusters = await ewms.get_taskforce_phases(
                self.ewms_rc, manifest.ewms_workflow_id
            )
        else:
            clusters = []

        # respond
        resp = {
            "scan_state": scan_state,
            "is_deleted": manifest.is_deleted,
            "scan_complete": bool(
                scan_state == utils._ScanState.SCAN_FINISHED_SUCCESSFULLY.name
            ),
            "pods": pods_411,
            "clusters": clusters,
        }
        if not args.include_pod_statuses:
            resp.pop("pods")
        self.write(resp)

    #
    # NOTE - handler needs to stay user-read-only
    #


# -----------------------------------------------------------------------------


class ScanLogsHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles relaying logs for scans."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/logs$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's logs."""
        try:
            pod_container_logs = k8s.utils.KubeAPITools.get_container_logs(
                self.k8s_batch_api,
                SkyScanK8sJobFactory.get_job_name(scan_id),
                ENV.K8S_NAMESPACE,
            )
            pod_container_logs_message = "retrieved"
        except (kubernetes.client.rest.ApiException, ValueError) as e:
            if await self.scan_backlog.is_in_backlog(scan_id):
                pod_container_logs = {}
                pod_container_logs_message = "in backlog"
            else:
                pod_container_logs = {}
                pod_container_logs_message = "pod(s) not found"
                LOGGER.exception(e)

        self.write(
            {
                "pod_container_logs": pod_container_logs,
                "pod_container_logs_message": pod_container_logs_message,
            }
        )

    #
    # NOTE - handler needs to stay user-read-only
    #


# -----------------------------------------------------------------------------
