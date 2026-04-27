"""Handlers for the SkyDriver REST API server interface."""

import asyncio
import dataclasses as dc
import json
import logging
import time
import uuid
from typing import Any, Type, TypeVar, cast

import humanfriendly  # type: ignore[import-untyped]
import kubernetes.client  # type: ignore[import-untyped]
from dacite import from_dict
from dacite.exceptions import DaciteError
from pymongo import ReturnDocument
from rest_tools import openapi_tools
from rest_tools.client import RestClient
from rest_tools.server import RestHandler
from tornado import web
from wipac_dev_tools.container_registry_tools import ImageNotFoundException
from wipac_dev_tools.mongo_jsonschema_tools import MongoJSONSchemaValidatedCollection

from . import config, database, ewms, images
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
from .database.schema import (
    _NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
    has_skydriver_requested_ewms_workflow,
)
from .ewms import get_deactivated_type, request_stop_on_ewms
from .images import ImageTooOldException
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


def all_dc_fields(class_or_instance: Any) -> set[str]:
    """Get all the field names for a dataclass (instance or class)."""
    return set(f.name for f in dc.fields(class_or_instance))


def dict_projection(dicto: dict, projection: list[str] | str) -> dict:
    """Keep only the keys in the `projection`.

    Passing the field names as a list or pipe-delimited string (not both).

    Including a `"*"` as a field or an empty list, will keep all fields.
    """
    if not projection:  # any empty: str or list
        return dicto

    # case: string
    if isinstance(projection, str):
        if projection == "*":
            return dicto
        else:
            return {k: v for k, v in dicto.items() if k in projection.split("|")}

    # case: list
    if "*" in projection:  # it doesn't matter what the other keys are, "*" trumps
        return dicto
    else:
        return {k: v for k, v in dicto.items() if k in projection}


# -----------------------------------------------------------------------------
# handlers


class BaseSkyDriverHandler(RestHandler):
    """BaseSkyDriverHandler is a RestHandler for all SkyDriver routes."""

    def initialize(  # type: ignore
        self,
        db: database.SkyDriverMongoValidatedDatabase,
        k8s_batch_api: kubernetes.client.BatchV1Api,
        ewms_rc: RestClient,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)  # type: ignore[no-untyped-call]
        self.db = db
        self.k8s_batch_api = k8s_batch_api
        self.ewms_rc = ewms_rc


# ----------------------------------------------------------------------------


class MainHandler(BaseSkyDriverHandler):
    """MainHandler is a BaseSkyDriverHandler that handles the root route."""

    ROUTE = r"/$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def post(self) -> None:
        """Get matching scan manifest(s) for the given search."""
        manifest_projection = self.get_argument("manifest_projection", "*")
        filter_ = self.get_argument("filter")
        include_deleted = self.get_argument("include_deleted", False)

        # "*" means "all allowed fields", which == DEFAULT_FIELDS for this endpoint
        # -- the full Manifest set would include too-large fields (data), aka DISALLOWED_FIELDS
        if manifest_projection == "*":
            manifest_projection = list(self.DEFAULT_FIELDS)
        # reject any explicit requests for disallowed (too-large) fields
        if set(manifest_projection) & self.DISALLOWED_FIELDS:
            raise web.HTTPError(
                400,
                log_message=f"'manifest_projection' cannot include any of the following: {self.DISALLOWED_FIELDS}",
            )

        # query
        if "is_deleted" not in filter_ and not include_deleted:
            filter_["is_deleted"] = False
        manifests = [
            dict_projection(dc.asdict(m), manifest_projection)
            async for m in self.db.manifests.find_all(filter_)
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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self) -> None:
        """Get all scan id(s) in the backlog."""
        entries = [e async for e in self.db.scan_backlog.get_all()]

        self.write({"entries": entries})

    #
    # NOTE - 'ScanBacklogHandler' needs to stay user-read-only b/c
    #         it's indirectly updated by the launching of a new scan
    #


# -----------------------------------------------------------------------------


def _debug_mode(val: Any) -> list[DebugMode]:
    """Normalize scalar-or-list input to a list of DebugMode enum values."""
    if not isinstance(val, list):
        val = [val]
    return [DebugMode(v) for v in val]  # -> ValueError


def _to_bytes(val: str | int) -> int:
    """Convert a humanfriendly size string (e.g. `'4GB'`) to int bytes, or passthrough int."""
    if isinstance(val, int):
        return val
    try:
        return humanfriendly.parse_size(val)  # type: ignore[no-any-return]
    except humanfriendly.InvalidSize:
        raise web.HTTPError(400, reason=f"invalid size: {val}")


def _validate_debug_mode_with_clusters(debug_mode: list, request_clusters: list):
    """Cross-field check: client-logs debug mode caps worker counts per cluster."""
    if DebugMode.CLIENT_LOGS in debug_mode:
        for cname, cworkers in request_clusters:
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


def _validate_all_known_request_clusters(
    request_clusters: list | dict,
) -> list[tuple[str, int]]:
    """Normalize dict-form to list of 2-tuples and validate cluster names against known list."""
    # spec allows {'osg': 1500} OR [['osg', 1500], ...]; handler needs list form
    if isinstance(request_clusters, dict):
        request_clusters = list(request_clusters.items())
    for name, _ in request_clusters:
        if name not in KNOWN_CLUSTERS:
            msg = f"requested unknown cluster: {name} (available: {', '.join(KNOWN_CLUSTERS.keys())})"
            raise web.HTTPError(400, reason=msg, log_message=msg)
    return request_clusters


def _validate_classifiers_fields(classifiers: dict) -> dict:
    """Validate that the fields of `classifiers` are valid."""
    for x in ["rescan", "origin_scan_id"]:  # these denote rescan relations
        if x in classifiers:
            msg = f"classifier cannot contain '{x}' sub-field"
            raise web.HTTPError(
                400,
                log_message=msg,
                reason=msg,
            )
    return classifiers


def _raise_missing_argument_400(arg_name: str) -> list:
    """Raise a 400 error with a helpful message.

    NOTE: This is for special cases where the OpenAPI spec cannot detect
    the missing argument -- like when the required argument has an alias.
    """
    _msg = f"'{arg_name}' is a required property"
    raise web.HTTPError(400, reason=_msg, log_message=_msg)


class ScanLauncherHandler(BaseSkyDriverHandler):
    """Handles starting new scans."""

    ROUTE = r"/scan$"

    async def _get_i3_event_id(
        self,
        i3_event_id: str | None,
        event_i3live_json: dict | str | None,
    ) -> str:
        if bool(i3_event_id) == bool(event_i3live_json):  # XOR: only one allowed
            msg = "Must provide either 'event_i3live_json' or 'i3_event_id' (xor)"
            raise web.HTTPError(
                400,
                reason=msg,
                log_message=msg + f" for {i3_event_id=} {event_i3live_json=}",
            )

        if isinstance(event_i3live_json, str):
            try:
                event_i3live_json = json.loads(event_i3live_json)
            except json.JSONDecodeError as e:
                msg = f"event_i3live_json is not valid JSON: {e}"
                raise web.HTTPError(400, reason=msg, log_message=msg)

        if i3_event_id:
            ret = await self.db.i3_events.find_one({"i3_event_id": i3_event_id})
            if not ret:
                _msg = "i3 event not found"
                raise web.HTTPError(
                    400,
                    reason=_msg,
                    log_message=_msg + f" for {i3_event_id=}",
                )
            else:
                return i3_event_id
        else:
            # -> store the event in its own collection to reduce redundancy
            i3_event_id = uuid.uuid4().hex
            await self.db.i3_events.insert_one(
                {
                    "i3_event_id": i3_event_id,
                    "json_dict": event_i3live_json,  # this was transformed into dict
                }
            )
            return i3_event_id

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def post(self) -> None:
        """Start a new scan."""
        # Reject fully-deprecated 'memory' field (no canonical replacement to fall through to)
        if self.get_argument("memory", None):
            msg = "argument 'memory' is deprecated -- use 'worker_memory_bytes'"
            raise web.HTTPError(400, reason=msg, log_message=msg)

        # 1. Make scan_request_obj while validating -- to go into DB
        #    Note: type validation is done by OpenAPI spec, via '@openapi_tools.validate_request()'
        #    Deprecated aliases are coalesced inline via `or` chains.
        try:
            scan_request_obj = dict(
                scan_id=make_scan_id(),
                rescan_ids=[],
                #
                docker_tag=await images.resolve_docker_tag(
                    self.get_argument("docker_tag")
                ),
                #
                # skyscan server config
                scanner_server_memory_bytes=_to_bytes(
                    self.get_argument("scanner_server_memory_bytes", None)
                    or self.get_argument("scanner_server_memory", None)  # DEPRECATED
                    or ENV.K8S_SCANNER_MEM_REQUEST__DEFAULT  # default
                ),
                reco_algo=self.get_argument("reco_algo"),
                nsides=self.get_argument("nsides"),
                real_or_simulated_event=self.get_argument("real_or_simulated_event"),
                predictive_scanning_threshold=self.get_argument(
                    "predictive_scanning_threshold", 1.0
                ),
                #
                classifiers=_validate_classifiers_fields(
                    self.get_argument("classifiers", {})
                ),
                #
                # cluster (condor) config
                request_clusters=_validate_all_known_request_clusters(
                    self.get_argument("request_clusters", None)  # see last 'or'
                    or self.get_argument("cluster", None)  # DEPRECATED
                    or _raise_missing_argument_400("request_clusters")  # it's required
                ),
                worker_memory_bytes=_to_bytes(
                    self.get_argument("worker_memory_bytes", None)
                    or self.get_argument("worker_memory", None)  # DEPRECATED
                    or ENV.EWMS_WORKER_MEMORY__DEFAULT  # default
                ),
                worker_disk_bytes=_to_bytes(
                    self.get_argument("worker_disk_bytes", None)
                    or self.get_argument("worker_disk", None)  # DEPRECATED
                    or ENV.EWMS_WORKER_DISK__DEFAULT  # default
                ),
                max_pixel_reco_time=self.get_argument("max_pixel_reco_time"),
                priority=self.get_argument("priority", 0),
                debug_mode=[
                    d.value for d in _debug_mode(self.get_argument("debug_mode", []))
                ],
                #
                # misc
                i3_event_id=await self._get_i3_event_id(  # foreign key to i3_event collection
                    self.get_argument("i3_event_id", ""),
                    self.get_argument("event_i3live_json", {}),  # str | object
                ),
                scanner_server_env_from_user=(
                    self.get_argument("scanner_server_env_from_user", None)
                    or self.get_argument("scanner_server_env", None)  # DEPRECATED
                    or {}  # default
                ),
            )
        except ImageNotFoundException as e:
            raise web.HTTPError(
                400,
                reason="argument docker_tag: image not found",
                log_message=repr(e),
            )
        except ImageTooOldException as e:
            raise web.HTTPError(
                400,
                reason=f"argument docker_tag: {e}",
                log_message=repr(e),
            )
        except web.HTTPError as e:
            raise e
        except Exception as e:
            raise web.HTTPError(
                400,
                reason="unspecified validation error",
                log_message=repr(e),
            )

        # 2. Validate args that require accesing multiple fields
        # scan_request_obj values are union-typed from the freeform dict;
        # cast to list since both fields are lists at this point (debug_mode
        # normalized earlier, request_clusters from _validate_all_known_request_clusters)
        _validate_debug_mode_with_clusters(
            cast(list, scan_request_obj["debug_mode"]),
            cast(list, scan_request_obj["request_clusters"]),
        )

        # 3. Do this before we write anything in the off chance that grabbing the arg fails
        manifest_projection = self.get_argument("manifest_projection", "*")

        # 4. Persist in DB
        manifest = await enqueue_scan(
            self.db.manifests,
            self.db.scan_backlog,
            self.db.skyscan_k8s_jobs,
            scan_request_obj,
            insert_scan_request_obj=True,
            scan_request_coll=self.db.scan_requests,
        )

        # 5. Write response
        self.write(dict_projection(dc.asdict(manifest), manifest_projection))


async def enqueue_scan(
    manifests: MongoJSONSchemaValidatedCollection,
    scan_backlog: MongoJSONSchemaValidatedCollection,
    skyscan_k8s_job_coll: MongoJSONSchemaValidatedCollection,
    scan_request_obj: dict,
    /,
    insert_scan_request_obj: bool,  # False for rescans
    scan_request_coll: MongoJSONSchemaValidatedCollection | None = None,  # type: ignore[valid-type]
) -> schema.Manifest:
    """Create all need data for a new scan, then enqueue it on the backlog."""
    scan_id = scan_request_obj["scan_id"]

    # persist the scan request obj in db?
    if insert_scan_request_obj:
        if scan_request_coll is None:
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
    manifest = await manifests.find_one_and_update(
        {"scan_id": manifest.scan_id},
        {"$set": manifest},
        upsert=True,
    )
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


class ScanRequestHandler(BaseSkyDriverHandler):
    """Handles metadata for scan requests."""

    ROUTE = r"/scan-request/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """GET."""
        try:
            scan_request_obj = await self.db.scan_requests.find_one(
                {"scan_id": scan_id}
            )
        except DocumentNotFoundException:
            msg = "Scan request not found"
            raise web.HTTPError(
                404,
                log_message=msg + f" for {scan_id=}",
                reason=msg,
            )

        # motor's find_one returns a generic _DocumentType; narrow to dict so
        # we can call .pop and hand it to self.write
        scan_request_obj = cast(dict, scan_request_obj)
        scan_request_obj.pop("_id", None)

        self.write(scan_request_obj)


# -----------------------------------------------------------------------------


class ScanRescanHandler(BaseSkyDriverHandler):
    """Handles actions on copying a scan's manifest and starting that."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/actions/rescan$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def post(self, scan_id: str) -> None:
        abort_first = self.get_argument("abort_first", False)
        replace_scan = self.get_argument("replace_scan", False)
        # response args
        manifest_projection = self.get_argument("manifest_projection", "*")

        if abort_first:
            await abort_scan(self.db.manifests, scan_id, self.ewms_rc)

        # generate unique scan_id
        new_scan_id = make_scan_id()

        # grab the 'scan_request_obj'
        try:
            scan_request_obj = await self.db.scan_requests.find_one_and_update(
                get_scan_request_obj_filter(scan_id),
                {
                    # record linkage (discoverability)
                    # NOTE: must preserve order here for history -- so push
                    "$push": {"rescan_ids": new_scan_id},
                },
                return_document=ReturnDocument.AFTER,
            )
        except DocumentNotFoundException:
            raise web.HTTPError(
                404,
                log_message="Could not find original scan-request information to start a rescan",
            )

        # go!
        manifest = await enqueue_scan(
            self.db.manifests,
            self.db.scan_backlog,
            self.db.skyscan_k8s_jobs,
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

        if replace_scan:
            await self.db.manifests.collection.find_one_and_update(
                {"scan_id": scan_id},
                {"$set": {"replaced_by_scan_id": new_scan_id}},
                return_dclass=dict,
            )

        self.write(
            dict_projection(dc.asdict(manifest), manifest_projection),
        )


# -----------------------------------------------------------------------------


class ScanMoreWorkersHandler(BaseSkyDriverHandler):
    """Handles actions on increasing the number of workers for an ongoing scan."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/actions/add-workers$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def post(self, scan_id: str) -> None:
        n_workers = self.get_argument("n_workers")  # required
        cluster_location = self.get_argument("cluster_location")  # required

        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

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
            manifest.scan_id, self.db.results
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
                    "cluster_location": cluster_location,
                    "n_workers": n_workers,
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
    manifests: MongoJSONSchemaValidatedCollection,
    scan_id: str,
    ewms_rc: RestClient,
) -> database.schema.Manifest:
    """Stop all parts of the Scanner instance (if running) and mark in DB."""
    # mark as deleted -> also stops backlog from starting
    manifest = await manifests.find_one_and_update(
        {"scan_id": scan_id},
        {"$set": {"is_deleted": True}},
        upsert=True,
    )
    # stop ewms
    await stop_skyscan_workers(
        manifests,
        scan_id,
        ewms_rc,
        abort=True,
    )
    return manifest


async def stop_skyscan_workers(
    manifests: MongoJSONSchemaValidatedCollection,
    scan_id: str,
    ewms_rc: RestClient,
    abort: bool,
) -> database.schema.Manifest:
    """Stop the scanner instance's workers on EWMS."""
    manifest = await manifests.find_one({"scan_id": scan_id})
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
    manifests: MongoJSONSchemaValidatedCollection,
    results: MongoJSONSchemaValidatedCollection,
    scan_id: str,
    incl_del: bool,
) -> tuple[None | database.schema.Result, database.schema.Manifest]:
    """Get the Result (and Manifest) using the incl_del/is_deleted logic.

    Returns objects as dicts
    """
    manifest = await manifests.find_one(
        {"scan_id": scan_id} | ({} if incl_del else {"is_deleted": False})
    )

    # check if requestor allows a deleted scan's result
    if (not incl_del) and manifest.is_deleted:
        raise web.HTTPError(
            404,
            log_message=f"Requested result with deleted manifest: {manifest.scan_id}",
        )

    # if we don't have a result yet, return {}
    try:
        result = await results.find_one({"scan_id": scan_id})
    except DocumentNotFoundException:
        result = None

    return result, manifest


# -----------------------------------------------------------------------------


class ScanHandler(BaseSkyDriverHandler):
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def delete(self, scan_id: str) -> None:
        """Abort a scan and/or mark manifest & result as "deleted"."""
        delete_completed_scan = self.get_argument("delete_completed_scan", False)
        # response args
        manifest_projection = self.get_argument("manifest_projection", "*")

        # check DB states
        if (not delete_completed_scan) and (
            await get_scan_state_if_final_result_received(scan_id, self.db.results)
        ):
            msg = "Attempted to delete a completed scan (must use `delete_completed_scan=True`)"
            raise web.HTTPError(
                400,
                log_message=msg,
                reason=msg,
            )

        manifest = await abort_scan(self.db.manifests, scan_id, self.ewms_rc)

        try:
            result_dict = await self.db.results.find_one({"scan_id": scan_id})
        except DocumentNotFoundException:
            result_dict = {}

        self.write(
            {
                "manifest": dict_projection(dc.asdict(manifest), manifest_projection),
                "result": result_dict,
            }
        )

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get manifest & result."""
        include_deleted = self.get_argument("include_deleted", False)

        result, manifest = await get_result_safely(
            self.db.manifests,
            self.db.results,
            scan_id,
            include_deleted,
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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get scan progress."""
        include_deleted = self.get_argument("include_deleted", False)
        # response args: pipe-delimited string (query params can't natively represent arrays)
        projection = self.get_argument("projection", "*")  # pipe-delimited string

        # get manifest from db
        manifest = await self.db.manifests.find_one(
            {"scan_id": scan_id} | ({} if include_deleted else {"is_deleted": False})
        )

        # Backward Compatibility for Skymap Scanner:
        #   Include the whole event dict in the response like the 'old' manifest.
        #   This overrides the manifest's field which should be an id.
        if (
            self.auth_roles[0] == INTERNAL_ACCT  # type: ignore
            and any(x in projection.split("|") for x in ["*", "event_i3live_json_dict"])
            and manifest.i3_event_id  # if no id, then event already in manifest
        ):
            try:
                i3event_doc = await self.db.i3_events.find_one(
                    {"i3_event_id": manifest.i3_event_id}
                )
                manifest.event_i3live_json_dict = i3event_doc["json_dict"]
            except DocumentNotFoundException:
                # this would mean the event was removed from the db
                error_msg = (
                    f"No i3 event document found with id '{manifest.i3_event_id}'"
                    f"--if other fields are wanted, re-request using 'projection'"
                )
                raise web.HTTPError(
                    404,
                    log_message=error_msg,
                    reason=error_msg,
                )

        self.write(dict_projection(dc.asdict(manifest), projection))

    @service_account_auth(roles=[INTERNAL_ACCT])  # type: ignore
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def patch(self, scan_id: str) -> None:
        """Update scan progress."""
        T = TypeVar("T")

        def from_dict_wrapper_or_none(data_class: Type[T], val: Any) -> T | None:
            if not val:
                return None
            try:
                return from_dict(data_class, val)
            except DaciteError as e:
                raise web.HTTPError(400, reason=str(e), log_message=str(e))

        progress = from_dict_wrapper_or_none(
            database.schema.Progress,
            self.get_argument("progress", None),
        )
        event_metadata = from_dict_wrapper_or_none(
            database.schema.EventMetadata,
            self.get_argument("event_metadata", None),
        )
        scan_metadata = self.get_argument("scan_metadata", {})

        manifest = await self.db.manifests.patch(
            scan_id,
            progress,
            event_metadata,
            scan_metadata,
        )

        self.write(dc.asdict(manifest))  # don't use a projection


# -----------------------------------------------------------------------------


class ScanI3EventHandler(BaseSkyDriverHandler):
    """Handles grabbing i3 events using scan ids."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/i3-event$"

    @service_account_auth(roles=[USER_ACCT, INTERNAL_ACCT])  # type: ignore
    @maybe_redirect_scan_id(roles=[USER_ACCT])
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get scan's i3 event."""
        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

        # look up event in collection
        if manifest.i3_event_id:
            try:
                doc = await self.db.i3_events.find_one(
                    {"i3_event_id": manifest.i3_event_id}
                )
                i3_event = doc["json_dict"]
            except DocumentNotFoundException:
                # this would mean the event was removed from the db
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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get a scan's persisted result."""
        include_deleted = self.get_argument("include_deleted", False)

        result, _ = await get_result_safely(
            self.db.manifests,
            self.db.results,
            scan_id,
            include_deleted,
        )

        self.write(dc.asdict(result) if result else {})

    @service_account_auth(roles=[INTERNAL_ACCT])  # type: ignore
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def put(self, scan_id: str) -> None:
        """Put (persist) a scan's result."""
        skyscan_result = self.get_argument("skyscan_result")  # required
        is_final = self.get_argument("is_final")  # required

        if not skyscan_result:
            self.write({})
            return

        result = await self.db.results.find_one_and_update(
            {"scan_id": scan_id},
            {"$set": {"skyscan_result": skyscan_result, "is_final": is_final}},
            upsert=True,
        )
        self.write(result)

        # END #
        await self.finish()
        # AFTER RESPONSE #

        # when we get the final result, it's time to tear down
        if is_final:
            await asyncio.sleep(
                WAIT_BEFORE_TEARDOWN
            )  # regular time.sleep() sleeps the entire server
            await stop_skyscan_workers(
                self.db.manifests,
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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get a scan's status."""
        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

        # scan state
        scan_state = await get_scan_state(manifest, self.ewms_rc, self.db.results)

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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get a scan's logs."""
        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """Get the ewms workflow_id."""
        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def post(self, scan_id: str) -> None:
        """Update the ewms workflow_id."""
        workflow_id = self.get_argument("workflow_id")  # required
        ewms_address = self.get_argument("ewms_address")  # required

        try:
            manifest = await self.db.manifests.collection.find_one_and_update(
                {
                    "scan_id": scan_id,
                    "ewms_workflow_id": _NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
                    "is_deleted": False,
                },
                {
                    "$set": {
                        "ewms_workflow_id": workflow_id,
                        "ewms_address": ewms_address,
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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """GET.

        This is a high-level utility, which removes unnecessary EWMS semantics.
        """
        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

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
    @openapi_tools.validate_request(config.OPENAPI_SPEC)
    async def get(self, scan_id: str) -> None:
        """GET.

        This is useful for debugging by seeing what was sent to condor.
        """
        manifest = await self.db.manifests.find_one({"scan_id": scan_id})

        self.write(
            {
                "taskforces": await ewms.get_taskforce_infos(
                    self.ewms_rc,
                    manifest.ewms_workflow_id,
                )
            }
        )
