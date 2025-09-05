"""Handlers for the SkyDriver REST API server interface."""

import argparse
import asyncio
import dataclasses as dc
import json
import logging
import re
import time
import uuid
from typing import Any, Type, TypeVar, cast

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
)
from tornado import web
from wipac_dev_tools import argparse_tools

from . import database, ewms, images
from .background_runners.scan_launcher import put_on_backlog
from .config import (
    DebugMode,
    ENV,
    EWMS_URL_V_PREFIX,
    INTERNAL_ACCT,
    KNOWN_CLUSTERS,
    USER_ACCT,
)
from .database import schema
from .database.mongodc import DocumentNotFoundException
from .database.schema import (
    _NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
    has_skydriver_requested_ewms_workflow,
)
from .ewms import get_deactivated_type, request_stop_on_ewms
from .k8s.scanner_instance import LogWrangler, SkyScanK8sJobFactory
from .rest_decorators import maybe_redirect_scan_id, service_account_auth
from .utils import (
    does_scan_state_indicate_final_result_received,
    get_scan_request_obj_filter,
    get_scan_state,
    get_scan_state_if_final_result_received,
    make_scan_id,
)

LOGGER = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# constants


REAL_CHOICES = ["real", "real_event"]
SIM_CHOICES = ["sim", "simulated", "simulated_event"]

MAX_CLASSIFIERS_LEN = 25

WAIT_BEFORE_TEARDOWN = 60


# -----------------------------------------------------------------------------
# utils


def _add_no_redirect_arg(arghand) -> None:
    # used only by @maybe_redirect_scan_id
    arghand.add_argument("no_redirect", default=False, type=bool)


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


class BaseSkyDriverHandler(RestHandler):
    """BaseSkyDriverHandler is a RestHandler for all SkyDriver routes."""

    def initialize(  # type: ignore
        self,
        mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
        k8s_batch_api: kubernetes.client.BatchV1Api,
        ewms_rc: RestClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)  # type: ignore[no-untyped-call]

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


class MainHandler(BaseSkyDriverHandler):
    """MainHandler is a BaseSkyDriverHandler that handles the root route."""

    ROUTE = r"/$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self) -> None:
        """Handle GET."""
        self.write({})


# -----------------------------------------------------------------------------


class ScansFindHandler(BaseSkyDriverHandler):
    """Handles finding scans by attributes."""

    ROUTE = r"/scans/find$"

    DISALLOWED_FIELDS = {
        # these can be so large that it can blow up memory
        "ewms_task",
        "event_i3live_json_dict",
        "event_i3live_json_dict__hash",
    }
    DEFAULT_FIELDS = all_dc_fields(database.schema.Manifest) - DISALLOWED_FIELDS

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
            default=self.DEFAULT_FIELDS,
            type=str,
        )
        args = arghand.parse_args()

        if args.manifest_projection == "*":  # can't use star here, too dangerous
            args.manifest_projection = self.DEFAULT_FIELDS
        if (
            args.manifest_projection - self.DISALLOWED_FIELDS
            != args.manifest_projection
        ):
            raise web.HTTPError(
                400,
                log_message=f"'manifest_projection' cannot include any of the following: {self.DISALLOWED_FIELDS}",
            )

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


class ScanBacklogHandler(BaseSkyDriverHandler):
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


def _classifiers_validator(val: Any) -> dict[str, str | bool | float | int | None]:
    # type checks
    if not isinstance(val, dict):
        raise argparse.ArgumentTypeError("must be a dict")
    if any(
        v for v in val.values() if not isinstance(v, str | bool | float | int | None)
    ):
        raise argparse.ArgumentTypeError(
            "entry must be 'str | bool | float | int | None'"
        )

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


class ScanLauncherHandler(BaseSkyDriverHandler):
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
            type=str,  # validated below
        )
        # scanner server args
        arghand.add_argument(
            "scanner_server_memory",
            type=_data_size_parse,
            default=humanfriendly.parse_size(ENV.K8S_SCANNER_MEM_REQUEST__DEFAULT),
        )
        # client worker args
        arghand.add_argument(
            "worker_memory",
            type=_data_size_parse,
            default=humanfriendly.parse_size(ENV.EWMS_WORKER_MEMORY__DEFAULT),
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
            default=humanfriendly.parse_size(ENV.EWMS_WORKER_DISK__DEFAULT),
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
        try:
            args.docker_tag = await images.resolve_docker_tag(args.docker_tag)
        except ValueError as e:
            raise web.HTTPError(
                400,
                reason=f"argument docker_tag: {e}",
                log_message=str(e),
            )

        # generate unique scan_id
        scan_id = make_scan_id()

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
            scanner_server_memory_bytes=args.scanner_server_memory,  # already in bytes # (note: name change)
            reco_algo=args.reco_algo,
            nsides=args.nsides,
            real_or_simulated_event=args.real_or_simulated_event,
            predictive_scanning_threshold=args.predictive_scanning_threshold,
            #
            classifiers=args.classifiers,
            #
            # cluster (condor) config
            request_clusters=args.cluster,  # a list # (note: name change)
            worker_memory_bytes=args.worker_memory,  # (note: name change)
            worker_disk_bytes=args.worker_disk,  # already in bytes # (note: name change)
            max_pixel_reco_time=args.max_pixel_reco_time,
            priority=args.priority,
            debug_mode=[d.value for d in args.debug_mode],
            #
            # misc
            i3_event_id=i3_event_id,  # foreign key to i3_event collection
            scanner_server_env_from_user=args.scanner_server_env,  # (note: name change)
        )

        # go!
        manifest = await _start_scan(
            self.manifests,
            self.scan_backlog,
            self.skyscan_k8s_job_coll,
            scan_request_obj,
            insert_scan_request_obj=True,
            scan_request_coll=self.scan_request_coll,
        )
        self.write(
            dict_projection(dc.asdict(manifest), args.manifest_projection),
        )


async def _start_scan(
    manifests: database.interface.ManifestClient,
    scan_backlog: database.interface.ScanBacklogClient,
    skyscan_k8s_job_coll: AsyncIOMotorCollection,  # type: ignore[valid-type]
    scan_request_obj: dict,
    /,
    insert_scan_request_obj: bool,  # False for rescans
    scan_request_coll: AsyncIOMotorCollection | None = None,
) -> schema.Manifest:
    scan_id = scan_request_obj["scan_id"]

    # persist the scan request obj in db?
    if insert_scan_request_obj:
        if not scan_request_coll:
            raise RuntimeError(
                "'scan_request_coll' instance must be provided for 'insert_scan_request_obj=True'"
            )
        await scan_request_coll.insert_one(scan_request_obj)

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
        scanner_server_env_from_user=scan_request_obj["scanner_server_env_from_user"],
        request_clusters=scan_request_obj["request_clusters"],
        max_pixel_reco_time=scan_request_obj["max_pixel_reco_time"],
        priority=scan_request_obj["priority"],
        worker_disk_bytes=scan_request_obj["worker_disk_bytes"],
        worker_memory_bytes=scan_request_obj["worker_memory_bytes"],
    )

    # put in db (do before k8s start so if k8s fail, we can debug using db's info)
    LOGGER.debug("creating new manifest")
    manifest = schema.Manifest(
        scan_id=scan_id,
        timestamp=time.time(),
        is_deleted=False,
        i3_event_id=scan_request_obj["i3_event_id"],
        scanner_server_args=scanner_server_args,
        ewms_workflow_id=schema._NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
        # ^^^ set once the workflow request has been sent to EWMS (see scan launcher)
        classifiers=scan_request_obj["classifiers"],
        priority=scan_request_obj["priority"],
    )
    manifest = await manifests.put(manifest)
    await skyscan_k8s_job_coll.insert_one(  # type: ignore[attr-defined]
        {
            "scan_id": scan_id,
            "skyscan_k8s_job_dict": skyscan_k8s_job_dict,
            "k8s_started_ts": None,
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


class ScanRescanHandler(BaseSkyDriverHandler):
    """Handles actions on copying a scan's manifest and starting that."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/actions/rescan$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def post(self, scan_id: str) -> None:
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
        arghand.add_argument(
            "abort_first",
            default=False,
            type=bool,
        )
        arghand.add_argument(
            "replace_scan",
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

        if args.abort_first:
            await abort_scan(self.manifests, scan_id, self.ewms_rc)

        # generate unique scan_id
        new_scan_id = make_scan_id()

        # grab the 'scan_request_obj'
        scan_request_obj = await self.scan_request_coll.find_one_and_update(
            get_scan_request_obj_filter(scan_id),
            {
                # record linkage (discoverability)
                # NOTE: must preserve order here for redirects -- so push
                "$push": {"rescan_ids": new_scan_id},
            },
            return_document=ReturnDocument.AFTER,
        )
        # -> error: couldn't find it anywhere
        if not scan_request_obj:
            raise web.HTTPError(
                404,
                log_message="Could not find original scan-request information to start a rescan",
            )

        # go!
        manifest = await _start_scan(
            self.manifests,
            self.scan_backlog,
            self.skyscan_k8s_job_coll,
            {
                **scan_request_obj,
                **{
                    # set id
                    "scan_id": new_scan_id,
                    # add to 'classifiers' so the user has provenance info
                    "classifiers": {
                        **scan_request_obj["classifiers"],
                        **{"rescan": True, "origin_scan_id": scan_id},
                    },
                },
            },
            insert_scan_request_obj=False,
        )

        if args.replace_scan:
            await self.manifests.collection.find_one_and_update(
                {"scan_id": scan_id},
                {"$set": {"replaced_by_scan_id": new_scan_id}},
                return_dclass=dict,
            )

        self.write(
            dict_projection(dc.asdict(manifest), args.manifest_projection),
        )


# -----------------------------------------------------------------------------


class ScanRemixHandler(BaseSkyDriverHandler):
    """Handles actions on copying a scan's request obj, tweaking it, then starting that."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/actions/remix$"

    ILLEGAL_REMIX_FIELDS = {"scan_id", "rescan_ids", "i3_event_id"}

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def post(self, scan_id: str) -> None:
        """POST."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
        arghand.add_argument(
            "abort_first",
            default=False,
            type=bool,
        )
        # NOTE: unlike a rescan, when remixing a scan, you cannot also set up a replacement redirect
        arghand.add_argument(
            "changes",
            type=dict,  # each changed field will be replaced wholesale
            required=True,
        )
        # response args
        arghand.add_argument(
            "manifest_projection",
            default=all_dc_fields(database.schema.Manifest),
            type=str,
        )
        args = arghand.parse_args()

        if args.abort_first:
            await abort_scan(self.manifests, scan_id, self.ewms_rc)

        # ensure we actually have changes (avoid accidental identical remix)
        if not args.changes:
            msg = "Remix requires a non-empty 'changes' object -- to duplicate, request a rescan"
            raise web.HTTPError(
                400,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

        # generate unique scan_id
        new_scan_id = make_scan_id()

        # fetch original request and record linkage (discoverability)
        orig_scan_req_obj = await self.scan_request_coll.find_one_and_update(
            get_scan_request_obj_filter(scan_id),
            {"$push": {"remix_ids": new_scan_id}},
            return_document=ReturnDocument.AFTER,
        )
        if not orig_scan_req_obj:
            raise web.HTTPError(
                404,
                log_message="Could not find original scan-request information to remix",
            )

        # build the new remix doc (validation + application in one place)
        new_scan_req_obj = await self._build_remix_doc(
            orig_doc=orig_scan_req_obj,
            changes=args.changes,
            origin_scan_id=scan_id,
            new_scan_id=new_scan_id,
        )

        # start the remix scan
        manifest = await _start_scan(
            self.manifests,
            self.scan_backlog,
            self.skyscan_k8s_job_coll,
            new_scan_req_obj,
            insert_scan_request_obj=True,  # persist the new scan-request
            scan_request_coll=self.scan_request_coll,
        )

        self.write(
            dict_projection(dc.asdict(manifest), args.manifest_projection),
        )

    @staticmethod
    async def _build_remix_doc(
        orig_doc: dict,
        changes: dict,
        origin_scan_id: str,
        new_scan_id: str,
    ) -> dict:
        """Return a new scan-request document with `changes` applied and provenance set."""

        # sanitize fields copied from the source doc
        doc = dict(orig_doc)
        doc.pop("_id", None)
        doc.pop("remix_ids", None)
        doc.pop("replaced_by_scan_id", None)

        # validate requested changes
        for k in ScanRemixHandler.ILLEGAL_REMIX_FIELDS:
            if k in changes:
                msg = f"scan cannot be remixed with changed field {k}"
                raise web.HTTPError(
                    422,
                    log_message=msg + f" for origin_scan_id={origin_scan_id!r}",
                    reason=msg,
                )
        for k in changes:
            if k not in doc:
                msg = (
                    f"scan cannot be remixed with non-existent changed field {k} "
                    f"(available fields: {set(doc.keys()) - ScanRemixHandler.ILLEGAL_REMIX_FIELDS})"
                )
                raise web.HTTPError(
                    422,
                    log_message=msg + f" for origin_scan_id={origin_scan_id!r}",
                    reason=msg,
                )
            elif type(doc[k]) is not type(changes[k]):
                # gerry-rigged type checker -- remove once we have openapi
                msg = (
                    f"scan cannot be remixed with mistyped changed field: {k} "
                    f"(should be {type(doc[k])} not {type(changes[k])})"
                )
                raise web.HTTPError(
                    422,
                    log_message=msg + f" for origin_scan_id={origin_scan_id!r}",
                    reason=msg,
                )

        # apply changes (wholesale replace per key)
        doc.update(changes)

        # provenance (kept inside existing classifiers; no new top-level attrs)
        doc.setdefault("classifiers", {})
        doc["classifiers"].update(
            {
                "remix_from_scan_id": origin_scan_id,
                "remix_changes": sorted(list(changes.keys())),
            }
        )

        # generated fields
        doc["scan_id"] = new_scan_id
        doc["rescan_ids"] = []

        # resolve docker_tag (if provided)
        if "docker_tag" in changes:
            try:
                doc["docker_tag"] = await images.resolve_docker_tag(
                    changes["docker_tag"]
                )
            except ValueError as e:
                raise web.HTTPError(
                    400,
                    reason=f"invalid remix field 'docker_tag': {e}",
                    log_message=str(e),
                )

        return doc


# -----------------------------------------------------------------------------


class ScanMoreWorkersHandler(BaseSkyDriverHandler):
    """Handles actions on increasing the number of workers for an ongoing scan."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/actions/add-workers$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def post(self, scan_id: str) -> None:
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
        # response args
        arghand.add_argument(
            "n_workers",
            type=int,
        )
        arghand.add_argument(
            "cluster_location",
            type=str,
        )
        args = arghand.parse_args()

        manifest = await self.manifests.get(scan_id, True)

        # has it been deleted?
        if manifest.is_deleted:
            msg = "this scan has been deleted--cannot add workers"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        # is this in EWMS?
        if not has_skydriver_requested_ewms_workflow(manifest.ewms_workflow_id):
            msg = "an EWMS workflow has not been assigned--cannot add workers"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )
        # is scan done?
        if await get_scan_state_if_final_result_received(
            manifest.scan_id, self.results
        ):
            msg = "this scan has a final result--cannot add workers"
            raise web.HTTPError(
                422,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

        # OK -- ready to add workers...

        # talk with EWMS
        try:
            resp = await self.ewms_rc.request(
                "POST",
                f"/{EWMS_URL_V_PREFIX}/query/task-directives",
                {
                    "query": {"workflow_id": manifest.ewms_workflow_id},
                    "projection": ["task_id"],
                },
            )
            task_id = resp["task_directives"][0]["task_id"]
            new_tf = await self.ewms_rc.request(
                "POST",
                f"/{EWMS_URL_V_PREFIX}/task-directives/{task_id}/actions/add-workers",
                {
                    "cluster_location": args.cluster_location,
                    "n_workers": args.n_workers,
                },
            )
        except Exception as e:  # broad b/c ewms is abstracted from user
            LOGGER.exception(e)
            msg = "failed to request additional workers on EWMS--contact admins"
            raise web.HTTPError(
                500,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

        self.write(
            {k: v for k, v in new_tf.items() if k in ["taskforce_uuid", "n_workers"]}
        )


# -----------------------------------------------------------------------------


async def abort_scan(
    manifests: database.interface.ManifestClient,
    scan_id: str,
    ewms_rc: RestClient,
) -> database.schema.Manifest:
    """Stop all parts of the Scanner instance (if running) and mark in DB."""
    # mark as deleted -> also stops backlog from starting
    manifest = await manifests.mark_as_deleted(scan_id)
    # stop ewms
    await stop_skyscan_workers(
        manifests,
        scan_id,
        ewms_rc,
        abort=True,
    )
    return manifest


async def stop_skyscan_workers(
    manifests: database.interface.ManifestClient,
    scan_id: str,
    ewms_rc: RestClient,
    abort: bool,
) -> database.schema.Manifest:
    """Stop the scanner instance's workers on EWMS."""
    manifest = await manifests.get(scan_id, True)
    LOGGER.info(f"stopping (ewms) workers for {scan_id=}...")

    # request to ewms
    if has_skydriver_requested_ewms_workflow(manifest.ewms_workflow_id):
        await request_stop_on_ewms(
            ewms_rc,
            cast(str, manifest.ewms_workflow_id),  # not None b/c above if-condition
            abort=abort,
        )
    else:
        LOGGER.info(
            "OK: attempted to stop skyscan workers but scan has not been sent to EWMS"
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


class ScanHandler(BaseSkyDriverHandler):
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def delete(self, scan_id: str) -> None:
        """Abort a scan and/or mark manifest & result as "deleted"."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
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
        if (not args.delete_completed_scan) and (
            await get_scan_state_if_final_result_received(scan_id, self.results)
        ):
            msg = "Attempted to delete a completed scan (must use `delete_completed_scan=True`)"
            raise web.HTTPError(
                400,
                log_message=msg,
                reason=msg,
            )

        manifest = await abort_scan(self.manifests, scan_id, self.ewms_rc)

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

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get manifest & result."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
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


class ScanManifestHandler(BaseSkyDriverHandler):
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/manifest$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get scan progress."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
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
            self.auth_roles[0] == INTERNAL_ACCT  # type: ignore
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

    @service_account_auth(roles=[INTERNAL_ACCT])  # type: ignore
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


class ScanI3EventHandler(BaseSkyDriverHandler):
    """Handles grabbing i3 events using scan ids."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/i3-event$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get scan's i3 event."""
        # _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id

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


class ScanResultHandler(BaseSkyDriverHandler):
    """Handles actions on persisted scan results."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/result$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get a scan's persisted result."""
        arghand = ArgumentHandler(ArgumentSource.QUERY_ARGUMENTS, self)
        _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id
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

    @service_account_auth(roles=[INTERNAL_ACCT])  # type: ignore
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
        await self.finish()
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


class ScanStatusHandler(BaseSkyDriverHandler):
    """Handles relying statuses for scans."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/status$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get a scan's status."""
        # _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id

        manifest = await self.manifests.get(scan_id, incl_del=True)

        # scan state
        scan_state = await get_scan_state(manifest, self.ewms_rc, self.results)

        # respond
        resp = {
            "scan_state": scan_state,
            "is_deleted": manifest.is_deleted,
            #
            # the scan is in a state where it cannot proceed further -- successful or otherwise
            # -> used by the scanner to prematurely quit in case of an abort (w/ 'is_deleted')
            "ewms_deactivated": await get_deactivated_type(
                self.ewms_rc, manifest.ewms_workflow_id
            ),
            #
            # the scan in effectively done, the physics has been finished
            # -> although, there may be lingering compute which will quit shortly
            "scan_complete": does_scan_state_indicate_final_result_received(scan_state),
            #
            # same as '/scan/<scan_id>/logs'
            "scanner_server_logs": {
                "url": await LogWrangler.assemble_scanner_server_logs_url(manifest),
            },
            #
            # same as '/scan/<scan_id>/ewms/workforce'
            "ewms_workforce": await ewms.get_workforce_statuses(
                self.ewms_rc, manifest.ewms_workflow_id
            ),
        }
        self.write(resp)

    #
    # NOTE - handler needs to stay user-read-only
    #


# -----------------------------------------------------------------------------


class ScanLogsHandler(BaseSkyDriverHandler):
    """Handles relaying logs for scans."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/logs$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get a scan's logs."""
        # _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id

        manifest = await self.manifests.get(scan_id, incl_del=True)

        self.write(
            {
                "scanner_server": {
                    "url": await LogWrangler.assemble_scanner_server_logs_url(manifest),
                }
            }
        )

    #
    # NOTE - handler needs to stay user-read-only
    #


# -----------------------------------------------------------------------------


class ScanEWMSWorkflowIDHandler(BaseSkyDriverHandler):
    """Handles actions on scan's ewms workflow id."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/ewms/workflow-id$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """Get the ewms workflow_id."""
        # _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id

        manifest = await self.manifests.get(scan_id, incl_del=True)
        self.write(
            {
                "workflow_id": manifest.ewms_workflow_id,
                "requested_ewms_workflow": has_skydriver_requested_ewms_workflow(
                    manifest.ewms_workflow_id
                ),
                "eligible_for_ewms": manifest.ewms_workflow_id is not None,
                "ewms_address": manifest.ewms_address,
            }
        )

    @service_account_auth(roles=[INTERNAL_ACCT])  # type: ignore
    async def post(self, scan_id: str) -> None:
        """Update the ewms workflow_id."""
        arghand = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        arghand.add_argument(
            "workflow_id",
            required=True,
            type=str,
        )
        arghand.add_argument(
            "ewms_address",
            required=True,
            type=str,
        )
        args = arghand.parse_args()

        try:
            manifest = await self.manifests.collection.find_one_and_update(
                {
                    "scan_id": scan_id,
                    "ewms_workflow_id": _NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
                    "is_deleted": False,
                },
                {
                    "$set": {
                        "ewms_workflow_id": args.workflow_id,
                        "ewms_address": args.ewms_address,
                    }
                },
                return_document=ReturnDocument.AFTER,
                return_dclass=dict,
            )
            manifest.pop("_id")
        except DocumentNotFoundException:
            raise web.HTTPError(
                404,
                log_message=(
                    "Could not find a scan manifest to update "
                    "(either the scan_id does not exist or its EWMS workflow_id cannot be updated)"
                ),
            )
        else:
            self.write(manifest)


# -----------------------------------------------------------------------------


class ScanEWMSWorkforceHandler(BaseSkyDriverHandler):
    """Handles actions for a scan's ewms workforce (condor workers)."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/ewms/workforce$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """GET.

        This is a high-level utility, which removes unnecessary EWMS semantics.
        """
        # _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id

        manifest = await self.manifests.get(scan_id, incl_del=True)

        self.write(
            await ewms.get_workforce_statuses(
                self.ewms_rc,
                manifest.ewms_workflow_id,
            )
        )


# -----------------------------------------------------------------------------


class ScanEWMSTaskforcesHandler(BaseSkyDriverHandler):
    """Handles actions for a scan's ewms taskforces (condor job submissions/clusters)."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/ewms/taskforces$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    async def get(self, scan_id: str) -> None:
        """GET.

        This is useful for debugging by seeing what was sent to condor.
        """
        # _add_no_redirect_arg(arghand)  # used only by @maybe_redirect_scan_id

        manifest = await self.manifests.get(scan_id, incl_del=True)

        self.write(
            {
                "taskforces": await ewms.get_taskforce_infos(
                    self.ewms_rc,
                    manifest.ewms_workflow_id,
                )
            }
        )
