"""Handlers for the SkyDriver REST API server interface."""


import dataclasses as dc
import json
import uuid
from pathlib import Path
from typing import Any, Type, TypeVar, cast

import kubernetes.client  # type: ignore[import]
from dacite import from_dict  # type: ignore[attr-defined]
from dacite.exceptions import DaciteError
from rest_tools.server import RestHandler, decorators

from . import database, k8s
from .config import ENV, LOGGER, SKYMAP_SCANNER_ACCT, USER_ACCT, is_testing

if is_testing():

    def service_account_auth(**kwargs):  # type: ignore
        def make_wrapper(method):
            async def wrapper(self, *args, **kwargs):
                LOGGER.warning("TESTING: auth disabled")
                return await method(self, *args, **kwargs)

            return wrapper

        return make_wrapper

else:
    service_account_auth = decorators.keycloak_role_auth


# -----------------------------------------------------------------------------


REAL_CHOICES = ["real", "real_event"]
SIM_CHOICES = ["sim", "simulated", "simulated_event"]


# -----------------------------------------------------------------------------


class BaseSkyDriverHandler(RestHandler):  # type: ignore  # pylint: disable=W0223
    """BaseSkyDriverHandler is a RestHandler for all SkyDriver routes."""

    def initialize(  # type: ignore  # pylint: disable=W0221
        self,
        mongo_client: str,
        k8s_api: kubernetes.client.BatchV1Api,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)
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

        def json_to_dict(val: Any) -> dict:
            # pylint:disable=W0707
            error = TypeError("must be JSON-string or JSON-friendly dict")
            # str -> json-dict
            if isinstance(val, str):
                try:
                    obj = json.loads(val)
                except:  # noqa: E722
                    raise error
                if not isinstance(obj, dict):  # loaded object must be dict
                    raise error
                return obj
            # dict -> check if json-friendly
            elif isinstance(val, dict):
                try:
                    json.dumps(val)
                    return val
                except:  # noqa: E722
                    raise error
            # fall-through
            raise error

        # docker args
        docker_tag = self.get_argument(
            "docker_tag",
            type=str,
            forbiddens=[r"\s*"],  # no empty string / whitespace
            default="latest",
        )

        # condor args
        njobs = self.get_argument(
            "njobs",
            type=int,
        )
        memory = self.get_argument(
            "memory",
            type=str,
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )
        collector = self.get_argument(
            "collector",
            type=str,
            default="",
        )
        schedd = self.get_argument(
            "schedd",
            type=str,
            default="",
        )

        # scanner args
        reco_algo = self.get_argument(
            "reco_algo",
            type=str,
            forbiddens=[r"\s*"],  # no empty string / whitespace
        )
        event_i3live_json_dict = self.get_argument(
            "event_i3live_json",
            type=json_to_dict,  # JSON-string/JSON-friendly dict -> dict
        )
        gcd_dir = self.get_argument(
            "gcd_dir",
            default=None,
            type=lambda x: Path(x) if x else None,
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

        # generate unique scan_id
        scan_id = uuid.uuid4().hex

        # get the container info ready
        volume_path = k8s.SkymapScannerJob.get_volume_path()
        server_args = k8s.SkymapScannerJob.get_server_args(
            volume_path=volume_path,
            reco_algo=reco_algo,
            nsides=nsides,  # type: ignore[arg-type]
            gcd_dir=gcd_dir,
            is_real_event=real_or_simulated_event in REAL_CHOICES,
        )
        clientmanager_args = k8s.SkymapScannerJob.get_clientmanager_args(
            volume_path=volume_path,
            singularity_image=f"{ENV.SKYSCAN_SINGULARITY_IMAGE_PATH_NO_TAG}:{docker_tag}",
            njobs=njobs,
            memory=memory,
            collector=collector,
            schedd=schedd,
        )
        env = k8s.SkymapScannerJob.get_env(
            rest_address=self.request.full_url().rstrip(self.request.uri),
            auth_token=self.auth_key,  # type: ignore[arg-type]
            scan_id=scan_id,
        )
        job = k8s.SkymapScannerJob(
            api_instance=self.k8s_api,
            docker_image=f"{ENV.SKYSCAN_DOCKER_IMAGE_NO_TAG}:{docker_tag}",
            server_args=server_args,
            clientmanager_args=clientmanager_args,
            env=env,
            scan_id=scan_id,
            volume_path=volume_path,
        )

        # put in db (do before k8s start so if k8s fail, we can debug using db's info)
        manifest = await self.manifests.post(
            event_i3live_json_dict,
            scan_id,
            server_args,
            clientmanager_args,
            {e.name: e.to_dict() for e in env},
        )

        # start k8s job
        job.start()

        self.write(dc.asdict(manifest))


# -----------------------------------------------------------------------------


class ManifestHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on scan's manifest."""

    ROUTE = r"/scan/manifest/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get scan progress."""
        incl_del = self.get_argument("include_deleted", default=False, type=bool)

        manifest = await self.manifests.get(scan_id, incl_del)

        self.write(dc.asdict(manifest))

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Abort a scan."""
        # TODO - call to k8s / kill condor_rm cluster(s)

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

        self.write(dc.asdict(result))

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Delete a scan's persisted result."""
        result = await self.results.mark_as_deleted(scan_id)

        self.write(dc.asdict(result))

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def put(self, scan_id: str) -> None:
        """Put (persist) a scan's result."""
        scan_result = self.get_argument("scan_result", type=dict, strict_type=True)
        is_final = self.get_argument("is_final", type=bool)

        result = await self.results.put(
            scan_id,
            cast(dict[str, Any], scan_result),
            is_final,
        )

        self.write(dc.asdict(result))


# -----------------------------------------------------------------------------
