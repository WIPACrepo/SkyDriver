"""Handlers for the SkyDriver REST API server interface."""


import dataclasses as dc
import json
import uuid
from typing import Any, Type, TypeVar, cast

import kubernetes.client  # type: ignore[import]
from dacite import from_dict  # type: ignore[attr-defined]
from dacite.exceptions import DaciteError
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore[import]
from rest_tools.server import RestHandler, token_attribute_role_mapping_auth
from tornado import web

from . import database, images, k8s, types
from .config import KNOWN_CONDORS, LOGGER, is_testing

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
# misc constants


REAL_CHOICES = ["real", "real_event"]
SIM_CHOICES = ["sim", "simulated", "simulated_event"]


# -----------------------------------------------------------------------------
# handlers


class BaseSkyDriverHandler(RestHandler):  # pylint: disable=W0223
    """BaseSkyDriverHandler is a RestHandler for all SkyDriver routes."""

    def initialize(  # type: ignore  # pylint: disable=W0221
        self,
        mongo_client: AsyncIOMotorClient,
        k8s_api: kubernetes.client.BatchV1Api,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)  # type: ignore[no-untyped-call]
        # pylint: disable=W0201
        self.manifests = database.interface.ManifestClient(mongo_client)
        self.results = database.interface.ResultClient(mongo_client)
        self.k8s_api = k8s_api


# ----------------------------------------------------------------------------


class MainHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """MainHandler is a BaseSkyDriverHandler that handles the root route."""

    ROUTE = r"/$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self) -> None:
        """Handle GET."""
        self.write({})


# -----------------------------------------------------------------------------


class RunEventMappingHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles mapping a run+event to scan(s)."""

    ROUTE = r"/scans$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self) -> None:
        """Get matching scan id(s) for the given event id."""
        run_id = self.get_argument("run_id", type=int)
        event_id = self.get_argument("event_id", type=int)
        is_real_event = self.get_argument("is_real_event", type=bool)

        incl_del = self.get_argument("include_deleted", default=False, type=bool)

        scan_ids = [
            s
            async for s in self.manifests.find_scan_ids(
                run_id, event_id, is_real_event, incl_del
            )
        ]

        self.write({"event_id": event_id, "scan_ids": scan_ids})

    #
    # NOTE - 'EventMappingHandler' needs to stay user-read-only b/c
    #         it's indirectly updated by the launching of a new scan
    #


# -----------------------------------------------------------------------------


class ScanLauncherHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles starting new scans."""

    ROUTE = r"/scan$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def post(self) -> None:
        """Start a new scan."""

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
            val: Any,
        ) -> list[types.RequestorInputCluster]:
            _error = TypeError(
                "must be a dict of schedd name and number of jobs, Ex: {'sub-2': 1500, ...}"
                " (to indicate a schedd more than once, provide a list of 2-lists instead),"
                f" available clusters: {list(KNOWN_CONDORS.keys())}"
                # TODO: make N_JOBS optional when using "TMS smart starter"
            )
            if isinstance(val, dict):
                val = list(val.items())  # {'a': 1, 'b': 2} -> [('a', 1), ('b', 2)}
            if not isinstance(val, list):
                raise _error
            # check all entries are 2-lists (or tuple)
            if not all(isinstance(a, list | tuple) and len(a) == 2 for a in val):
                raise _error
            #
            clusters = []
            for entry in val:
                try:
                    clusters.append(
                        types.RequestorInputCluster(
                            collector=KNOWN_CONDORS[entry[0]]["collector"],
                            schedd=KNOWN_CONDORS[entry[0]]["schedd"],
                            njobs=entry[1],
                        )
                    )
                except KeyError as e:
                    raise TypeError(f"requested unknown schedd: {entry[0]}") from e
            return clusters

        def _optional_int(val: Any) -> int | None:
            if val is None:
                return None
            return int(val)

        # docker args
        docker_tag = self.get_argument(  # any tag on docker hub (including 'latest') -- must also be on CVMFS (but not checked here)
            "docker_tag",
            type=images.resolve_docker_tag,
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )

        # condor args
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
        nsides = self.get_argument(
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
        max_reco_time = self.get_argument(
            "max_reco_time",
            type=_optional_int,
            default=None,
        )
        debug_mode = self.get_argument(
            "debug_mode",
            type=bool,
            default=False,
        )

        # generate unique scan_id
        scan_id = uuid.uuid4().hex

        # get the container info ready
        k8s_job = k8s.SkymapScannerStarterJob(
            api_instance=self.k8s_api,
            docker_tag=docker_tag,
            scan_id=scan_id,
            # server
            reco_algo=reco_algo,
            nsides=nsides,  # type: ignore[arg-type]
            is_real_event=real_or_simulated_event in REAL_CHOICES,
            predictive_scanning_threshold=predictive_scanning_threshold,
            # clientmanager
            request_clusters=request_clusters,
            memory=memory,
            max_reco_time=max_reco_time,
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
            k8s_job.env_dict,
        )

        # start skymap scanner instance
        try:
            k8s_job.start_job()
        except kubernetes.client.exceptions.ApiException as e:
            LOGGER.error(e)
            raise web.HTTPError(
                500,
                log_message="Failed to launch Kubernetes job for Scanner instance",
            )

        self.write(dc.asdict(manifest))


# -----------------------------------------------------------------------------


async def stop_scanner_instance(
    manifests: database.interface.ManifestClient,
    scan_id: str,
    k8s_api: kubernetes.client.BatchV1Api,
) -> None:
    """Stop all parts of the Scanner instance (if running) and mark in DB."""
    manifest = await manifests.get(scan_id, True)
    if manifest.complete:
        return

    # get the container info ready
    k8s_job = k8s.SkymapScannerStopperJob(
        k8s_api,
        scan_id,
        manifest.condor_clusters,
    )

    try:
        k8s_job.start_job()
    except kubernetes.client.exceptions.ApiException as e:
        LOGGER.error(e)
        raise web.HTTPError(
            500,
            log_message="Failed to launch Kubernetes job to stop Scanner instance",
        )

    await manifests.patch(scan_id, complete=True)


# -----------------------------------------------------------------------------


class ManifestHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/manifest/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT, SKYMAP_SCANNER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get scan progress."""
        incl_del = self.get_argument("include_deleted", default=False, type=bool)

        manifest = await self.manifests.get(scan_id, incl_del)

        self.write(dc.asdict(manifest))

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Abort a scan."""
        await stop_scanner_instance(self.manifests, scan_id, self.k8s_api)

        manifest = await self.manifests.mark_as_deleted(scan_id)

        self.write(dc.asdict(manifest))

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def patch(self, scan_id: str) -> None:
        """Update scan progress."""

        T = TypeVar("T")

        def from_dict_wrapper_or_none(data_class: Type[T], val: Any) -> T | None:
            if not val:
                return None
            try:
                return from_dict(data_class, val)  # type: ignore[no-any-return]
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
        condor_cluster = self.get_argument(
            "condor_cluster",
            type=lambda x: from_dict_wrapper_or_none(database.schema.CondorClutser, x),
            default=None,
        )

        manifest = await self.manifests.patch(
            scan_id,
            progress,
            event_metadata,
            scan_metadata,
            condor_cluster,
        )

        self.write(dc.asdict(manifest))


# -----------------------------------------------------------------------------


class ResultsHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on persisted scan results."""

    ROUTE = r"/scan/result/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's persisted result."""
        incl_del = self.get_argument("include_deleted", default=False, type=bool)

        result = await self.results.get(scan_id, incl_del)

        # when we get the final result, it's time to tear down
        if result.is_final:
            await stop_scanner_instance(self.manifests, scan_id, self.k8s_api)

        self.write(dc.asdict(result))

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Delete a scan's persisted result."""
        result = await self.results.mark_as_deleted(scan_id)

        self.write(dc.asdict(result))

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def put(self, scan_id: str) -> None:
        """Put (persist) a scan's result."""
        skyscan_result = self.get_argument(
            "skyscan_result", type=dict, strict_type=True
        )
        is_final = self.get_argument("is_final", type=bool)

        if not skyscan_result:
            self.write({})
            return

        result_dc = await self.results.put(
            scan_id,
            cast(dict[str, Any], skyscan_result),
            is_final,
        )
        self.write(dc.asdict(result_dc))


# -----------------------------------------------------------------------------
