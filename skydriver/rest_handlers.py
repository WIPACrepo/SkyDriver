"""Handlers for the SkyDriver REST API server interface."""


import asyncio
import dataclasses as dc
import json
import uuid
from typing import Any, Type, TypeVar

import kubernetes.client  # type: ignore[import-untyped]
from dacite import from_dict
from dacite.exceptions import DaciteError
from motor.motor_asyncio import AsyncIOMotorClient
from rest_tools.server import RestHandler, token_attribute_role_mapping_auth
from tornado import web

from . import database, images, k8s
from .config import (
    DEFAULT_K8S_CONTAINER_MEMORY_SKYSCAN_SERVER,
    ENV,
    KNOWN_CLUSTERS,
    LOGGER,
    DebugMode,
    is_testing,
)

# -----------------------------------------------------------------------------
# constants


REAL_CHOICES = ["real", "real_event"]
SIM_CHOICES = ["sim", "simulated", "simulated_event"]

MAX_CLASSIFIERS_LEN = 15

WAIT_BEFORE_TEARDOWN = 60

DEFAULT_EXCLUDED_MANIFEST_FIELDS = {
    "event_i3live_json_dict",
    "env_vars",
}


# -----------------------------------------------------------------------------
# REST requestor auth


USER_ACCT = "user"
SKYMAP_SCANNER_ACCT = "system"


if is_testing():

    def service_account_auth(**kwargs):  # type: ignore
        def make_wrapper(method):  # type: ignore[no-untyped-def]
            async def wrapper(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                LOGGER.warning("TESTING: auth disabled")
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


def dict_projection(dicto: dict, projection: set[str]) -> dict:
    """Keep only the keys in the `projection`.

    If `projection` is empty or includes '*', return all fields.
    """
    if "*" in projection:
        return dicto
    if not projection:
        return dicto
    return {k: v for k, v in dicto.items() if k in projection}


# -----------------------------------------------------------------------------
# handlers


class BaseSkyDriverHandler(RestHandler):  # pylint: disable=W0223
    """BaseSkyDriverHandler is a RestHandler for all SkyDriver routes."""

    def initialize(  # type: ignore  # pylint: disable=W0221
        self,
        mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
        k8s_batch_api: kubernetes.client.BatchV1Api,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)  # type: ignore[no-untyped-call]
        # pylint: disable=W0201
        self.manifests = database.interface.ManifestClient(mongo_client)
        self.results = database.interface.ResultClient(mongo_client)
        self.scan_backlog = database.interface.ScanBacklogClient(mongo_client)
        self.k8s_batch_api = k8s_batch_api


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
        mongo_filter: dict[str, Any] = self.get_argument(
            "filter",
            type=dict,
            strict_type=True,
        )
        incl_del = self.get_argument("include_deleted", default=False, type=bool)

        # response args
        manifest_projection = self.get_argument(
            "manifest_projection",
            default=(
                all_dc_fields(database.schema.Manifest)
                - DEFAULT_EXCLUDED_MANIFEST_FIELDS
            ),
            type=set[str],
        )

        if "is_deleted" not in mongo_filter and not incl_del:
            mongo_filter["is_deleted"] = False

        manifests = [
            dict_projection(dc.asdict(m), manifest_projection)
            async for m in self.manifests.find_all(mongo_filter)
        ]

        self.write({"manifests": manifests})

    #
    # NOTE - 'EventMappingHandler' needs to stay user-read-only b/c
    #         it's indirectly updated by the launching of a new scan
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


def cluster_lookup(name: str, n_workers: int) -> database.schema.Cluster:
    """Grab the Cluster object known using `name`."""
    if cluster := KNOWN_CLUSTERS.get(name):
        if cluster["orchestrator"] == "condor":
            return database.schema.Cluster(
                orchestrator=cluster["orchestrator"],
                location=database.schema.HTCondorLocation(**cluster["location"]),
                n_workers=n_workers,
            )
        elif cluster["orchestrator"] == "k8s":
            return database.schema.Cluster(
                orchestrator=cluster["orchestrator"],
                location=database.schema.KubernetesLocation(**cluster["location"]),
                n_workers=n_workers,
            )
    raise TypeError(
        f"requested unknown cluster: {name} (available:"
        f" {', '.join(KNOWN_CLUSTERS.keys())})"
    )


def _json_to_dict(val: Any) -> dict:
    _error = TypeError("must be JSON-string or JSON-friendly dict")
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


def _dict_or_list_to_request_clusters(
    val: dict | list,
) -> list[database.schema.Cluster]:
    _error = TypeError(
        "must be a dict of cluster location and number of workers, Ex: {'sub-2': 1500, ...}"
        " (to request a cluster location more than once, provide a list of 2-lists instead),"
        # TODO: make n_workers optional when using "TMS smart starter"
    )
    if isinstance(val, dict):
        val = list(val.items())  # {'a': 1, 'b': 2} -> [('a', 1), ('b', 2)}
    if not val:
        raise _error
    if not isinstance(val, list):
        raise _error
    # check all entries are 2-lists (or tuple)
    if not all(isinstance(a, list | tuple) and len(a) == 2 for a in val):
        raise _error
    #
    return [cluster_lookup(name, n_workers) for name, n_workers in val]


def _classifiers_validator(val: Any) -> dict[str, str | bool | float | int]:
    # type checks
    if not isinstance(val, dict):
        raise TypeError("must be a dict")
    if any(v for v in val.values() if not isinstance(v, str | bool | float | int)):
        raise TypeError("entry must be 'str | bool | float | int'")

    # size check
    if len(val) > MAX_CLASSIFIERS_LEN:
        raise ValueError(f"must be at most {MAX_CLASSIFIERS_LEN} entries long")
    for key, subval in val.items():
        if len(key) > MAX_CLASSIFIERS_LEN:
            raise ValueError(
                f"key must be at most {MAX_CLASSIFIERS_LEN} characters long"
            )
        try:
            if len(subval) > MAX_CLASSIFIERS_LEN:
                raise ValueError(
                    f"str-field must be at most {MAX_CLASSIFIERS_LEN} characters long"
                )
        except TypeError:
            pass  # not a str

    return val


def _debug_mode(val: Any) -> list[DebugMode]:
    if not isinstance(val, list):
        val = [val]
    return [DebugMode(v) for v in val]  # -> ValueError


class ScanLauncherHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles starting new scans."""

    ROUTE = r"/scan$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def post(self) -> None:
        """Start a new scan."""

        # docker args
        docker_tag = self.get_argument(  # any tag on docker hub (including 'latest') -- must also be on CVMFS (but not checked here)
            "docker_tag",
            type=images.resolve_docker_tag,
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )

        # scanner server args
        scanner_server_memory = self.get_argument(
            "scanner_server_memory",
            type=k8s.utils.KubeAPITools.validate_k8s_memory,
            default=DEFAULT_K8S_CONTAINER_MEMORY_SKYSCAN_SERVER,
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )

        # client worker args
        memory = self.get_argument(
            "memory",
            type=str,
            default="8GB",
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )
        request_clusters = self.get_argument(
            "cluster",
            type=_dict_or_list_to_request_clusters,
        )

        # scanner args
        reco_algo = self.get_argument(
            "reco_algo",
            type=str,
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )
        event_i3live_json_dict = self.get_argument(
            "event_i3live_json",
            type=_json_to_dict,  # JSON-string/JSON-friendly dict -> dict
        )
        nsides: dict[int, int] = self.get_argument(
            "nsides",
            type=dict,
            strict_type=True,
        )
        real_or_simulated_event = self.get_argument(
            "real_or_simulated_event",  # as opposed to simulation
            type=str,
            choices=REAL_CHOICES + SIM_CHOICES,
        )
        predictive_scanning_threshold = self.get_argument(
            "predictive_scanning_threshold",
            type=float,
            default=1.0,
            strict_type=False,  # allow casting from int (1)
        )
        max_pixel_reco_time = self.get_argument(
            "max_pixel_reco_time",
            type=int,
        )
        debug_mode = self.get_argument(
            "debug_mode",
            type=_debug_mode,
            default=[],
        )
        if DebugMode.CLIENT_LOGS in debug_mode:
            for cluster in request_clusters:
                cname, cinfo = cluster.to_known_cluster()
                if cluster.n_workers > cinfo.get(
                    "max_n_clients_during_debug_mode", float("inf")
                ):
                    raise web.HTTPError(
                        400,
                        log_message=(
                            f"Too many workers: Cluster '{cname}' can only have "
                            f"{cinfo.get('max_n_clients_during_debug_mode')} "
                            f"workers when 'debug_mode' "
                            f"includes '{DebugMode.CLIENT_LOGS.value}'"
                        ),
                    )

        # other args
        classifiers = self.get_argument(
            "classifiers",
            type=_classifiers_validator,
            default={},
        )

        # response args
        manifest_projection = self.get_argument(
            "manifest_projection",
            default=(
                all_dc_fields(database.schema.Manifest)
                - DEFAULT_EXCLUDED_MANIFEST_FIELDS
            ),
            type=set[str],
        )

        # generate unique scan_id
        scan_id = uuid.uuid4().hex

        # get the container info ready
        k8s_job = k8s.scanner_instance.SkymapScannerJob(
            k8s_batch_api=self.k8s_batch_api,
            scan_backlog=self.scan_backlog,
            #
            docker_tag=docker_tag,
            scan_id=scan_id,
            # server
            scanner_server_memory=scanner_server_memory,
            reco_algo=reco_algo,
            nsides=nsides,
            is_real_event=real_or_simulated_event in REAL_CHOICES,
            predictive_scanning_threshold=predictive_scanning_threshold,
            # clientmanager
            request_clusters=request_clusters,
            memory=memory,
            max_pixel_reco_time=max_pixel_reco_time,
            # universal
            debug_mode=debug_mode,
            # env
            rest_address=self.request.full_url().rstrip(self.request.uri),
        )

        # put in db (do before k8s start so if k8s fail, we can debug using db's info)
        manifest = await self.manifests.post(
            event_i3live_json_dict,
            scan_id,
            k8s_job.scanner_server_args,
            k8s_job.tms_args_list,
            from_dict(database.schema.EnvVars, k8s_job.env_dict),
            classifiers,
        )

        # enqueue skymap scanner instance to be started in-time
        try:
            await k8s_job.enqueue_job()
        except Exception as e:
            LOGGER.exception(e)
            raise web.HTTPError(
                500,
                log_message="Failed to enqueue Kubernetes job for Scanner instance",
            )

        self.write(
            dict_projection(dc.asdict(manifest), manifest_projection),
        )


# -----------------------------------------------------------------------------


async def stop_scanner_instance(
    manifests: database.interface.ManifestClient,
    scan_id: str,
    k8s_batch_api: kubernetes.client.BatchV1Api,
) -> database.schema.Manifest:
    """Stop all parts of the Scanner instance (if running) and mark in DB."""
    manifest = await manifests.get(scan_id, True)
    if manifest.complete:
        return manifest

    stopper = k8s.scanner_instance.SkymapScannerWorkerStopper(
        k8s_batch_api,
        scan_id,
        manifest.clusters,
    )

    try:
        stopper.go()
    except kubernetes.client.exceptions.ApiException as e:
        LOGGER.exception(e)
        raise web.HTTPError(
            500,
            log_message="Failed to stop Scanner instance",
        )

    return await manifests.patch(scan_id, complete=True)


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
        delete_completed_scan = self.get_argument(
            "delete_completed_scan",
            default=False,
            type=bool,
        )

        # response args
        manifest_projection = self.get_argument(
            "manifest_projection",
            default=(
                all_dc_fields(database.schema.Manifest)
                - DEFAULT_EXCLUDED_MANIFEST_FIELDS
            ),
            type=set[str],
        )

        # check DB states
        manifest = await self.manifests.get(scan_id, True)
        if manifest.complete and not delete_completed_scan:
            msg = "Attempted to delete a completed scan (must use `delete_completed_scan=True`)"
            raise web.HTTPError(
                400,
                log_message=msg,
                reason=msg,
            )

        # mark as deleted -> also stops backlog from starting
        manifest = await self.manifests.mark_as_deleted(scan_id)
        # abort
        await stop_scanner_instance(self.manifests, scan_id, self.k8s_batch_api)

        try:
            result_dict = dc.asdict(await self.results.get(scan_id))
        except web.HTTPError as e:
            if e.status_code != 404:
                raise
            result_dict = {}

        self.write(
            {
                "manifest": dict_projection(dc.asdict(manifest), manifest_projection),
                "result": result_dict,
            }
        )

    @service_account_auth(roles=[USER_ACCT, SKYMAP_SCANNER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get manifest & result."""
        incl_del = self.get_argument(
            "include_deleted",
            default=False,
            type=bool,
        )
        # # response args
        # manifest_projection = self.get_argument(
        #     "manifest_projection",
        #     default=(
        #         all_dc_fields(database.schema.Manifest)
        #         - DEFAULT_EXCLUDED_MANIFEST_FIELDS
        #     ),
        #     type=set[str],
        # )
        manifest_projection = (
            all_dc_fields(database.schema.Manifest) - DEFAULT_EXCLUDED_MANIFEST_FIELDS
        )

        result, manifest = await get_result_safely(
            self.manifests,
            self.results,
            scan_id,
            incl_del,
        )

        self.write(
            {
                "manifest": dict_projection(dc.asdict(manifest), manifest_projection),
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
        incl_del = self.get_argument(
            "include_deleted",
            default=False,
            type=bool,
        )
        # # response args
        # manifest_projection = self.get_argument(
        #     "manifest_projection",
        #     default=all_dc_fields(database.schema.Manifest),
        #     type=set[str],
        # )

        manifest = await self.manifests.get(scan_id, incl_del)

        self.write(
            # dict_projection(dc.asdict(manifest), manifest_projection),
            dc.asdict(manifest)
        )

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def patch(self, scan_id: str) -> None:
        """Update scan progress."""

        T = TypeVar("T")

        def from_dict_wrapper_or_none(data_class: Type[T], val: Any) -> T | None:
            if not val:
                return None
            try:
                return from_dict(data_class, val)
            except DaciteError as e:
                raise ValueError(str(e))

        progress = self.get_argument(
            "progress",
            type=lambda x: from_dict_wrapper_or_none(database.schema.Progress, x),
            default=None,
        )
        event_metadata = self.get_argument(
            "event_metadata",
            type=lambda x: from_dict_wrapper_or_none(database.schema.EventMetadata, x),
            default=None,
        )
        scan_metadata: database.schema.StrDict = self.get_argument(
            "scan_metadata",
            type=dict,
            default={},
        )
        cluster = self.get_argument(
            "cluster",
            type=lambda x: from_dict_wrapper_or_none(database.schema.Cluster, x),
            default=None,
        )

        manifest = await self.manifests.patch(
            scan_id,
            progress,
            event_metadata,
            scan_metadata,
            cluster,
        )

        # NOTE - the following will be moved to TMS, then improved
        # check cluster statuses & stop scan if workers are all failing
        for db_cluster in manifest.clusters:
            # Job-Status -> "Held:*"  &  Pilot-Status -> ANY
            # -- sum the total counts of all job-statuses prefixed with "Held:"
            n_held = sum(
                sum(  # pilot-status counts
                    cts for cts in db_cluster.statuses[job_status].values()
                )
                for job_status in db_cluster.statuses.keys()
                if job_status.startswith("Held:")
            )

            # Job-Status -> ANY  &  Pilot-Status -> "FatalError"
            n_fatal_error = sum(
                sum(  # pilot-status counts
                    cts
                    for pilot_status, cts in db_cluster.statuses[job_status].items()
                    if pilot_status == "FatalError"
                )
                for job_status in db_cluster.statuses.keys()
            )

            if n_held + n_fatal_error >= db_cluster.n_workers:
                manifest = await stop_scanner_instance(
                    self.manifests,
                    scan_id,
                    self.k8s_batch_api,
                )
                break

        self.write(dc.asdict(manifest))  # don't use a projection


# -----------------------------------------------------------------------------


class ScanResultHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on persisted scan results."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/result$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's persisted result."""
        incl_del = self.get_argument(
            "include_deleted",
            default=False,
            type=bool,
        )

        result, _ = await get_result_safely(
            self.manifests,
            self.results,
            scan_id,
            incl_del,
        )

        self.write(dc.asdict(result) if result else {})

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def put(self, scan_id: str) -> None:
        """Put (persist) a scan's result."""
        skyscan_result: dict[str, Any] = self.get_argument(
            "skyscan_result",
            type=dict,
            strict_type=True,
        )
        is_final = self.get_argument("is_final", type=bool)

        if not skyscan_result:
            self.write({})
            return

        result_dc = await self.results.put(
            scan_id,
            skyscan_result,
            is_final,
        )
        self.write(dc.asdict(result_dc))

        # END #
        self.finish()
        # AFTER RESPONSE #

        # when we get the final result, it's time to tear down
        if is_final:
            await asyncio.sleep(
                WAIT_BEFORE_TEARDOWN
            )  # regular time.sleep() sleeps the entire server
            await stop_scanner_instance(self.manifests, scan_id, self.k8s_batch_api)


# -----------------------------------------------------------------------------


class ScanStatusHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles relying statuses for scans."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/status$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's status."""
        include_pod_statuses = self.get_argument(
            "include_pod_statuses",
            type=bool,
            default=False,
        )

        manifest = await self.manifests.get(scan_id, incl_del=True)

        # get pod status
        pods_411: dict[str, Any] = {}
        if include_pod_statuses:
            try:
                pods_411["pod_status"] = k8s.utils.KubeAPITools.get_pod_status(
                    self.k8s_batch_api,
                    k8s.scanner_instance.SkymapScannerJob.get_job_name(scan_id),
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

        # respond
        resp = {
            "scan_state": manifest.get_state().name,
            "is_deleted": manifest.is_deleted,
            "scan_complete": manifest.complete,
            "pods": pods_411,
            "clusters": [dc.asdict(c) for c in manifest.clusters],
        }
        if not include_pod_statuses:
            resp.pop("pods")
        self.write(resp)

    #
    # NOTE - handler needs to stay user-read-only
    #


# -----------------------------------------------------------------------------


class ScanLogsHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles relying logs for scans."""

    ROUTE = r"/scan/(?P<scan_id>\w+)/logs$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's logs."""
        try:
            pod_container_logs = k8s.utils.KubeAPITools.get_container_logs(
                self.k8s_batch_api,
                k8s.scanner_instance.SkymapScannerJob.get_job_name(scan_id),
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
