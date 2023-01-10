"""Routes handlers for the SkyDriver REST API server interface."""


import dataclasses as dc
from pathlib import Path
from typing import Any, cast

import kubernetes.client  # type: ignore[import]
from rest_tools.server import RestHandler, handler

from . import database, k8s
from .config import LOGGER, SKYMAP_SCANNER_ACCT, USER_ACCT, is_testing

if is_testing():

    def service_account_auth(**kwargs):  # type: ignore
        def make_wrapper(method):
            async def wrapper(self, *args, **kwargs):
                LOGGER.warning("TESTING: auth disabled")
                return await method(self, *args, **kwargs)

            return wrapper

        return make_wrapper

else:
    service_account_auth = handler.keycloak_role_auth


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


class EventMappingHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles mapping an event to scan(s)."""

    ROUTE = r"/event/(?P<event_id>\w+)$"

    @service_account_auth(roles=[USER_ACCT])  # type: ignore
    async def get(self, event_id: str) -> None:
        """Get matching scan id(s) for the given event id."""
        incl_del = self.get_argument("include_deleted", default=False, type=bool)

        scan_ids = [s async for s in self.manifests.get_scan_ids(event_id, incl_del)]

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

        def no_empty_str(val: Any) -> str:
            out_val = str(val).strip()
            if not out_val:
                raise TypeError("cannot use empty string")
            return out_val

        # docker args
        docker_tag = self.get_argument(
            "docker_tag",
            type=str,
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
        )

        # scanner args
        progress_interval_sec = self.get_argument(
            "progress_interval_sec",
            type=int,
            default=5 * 60,
        )
        result_interval_sec = self.get_argument(
            "result_interval_sec",
            type=int,
            default=10 * 60,
        )
        reco_algo = self.get_argument(
            "reco_algo",
            type=str,
        )
        event_id = self.get_argument(
            "event_id",
            type=no_empty_str,
        )
        gcd_dir = self.get_argument(
            "gcd_dir",
            default=None,
            type=Path,
        )
        nsides = self.get_argument(
            "nsides",
            type=dict,
        )

        manifest = await self.manifests.post(event_id)  # generates ID

        # start k8s job
        job = k8s.SkymapScannerJob(
            k8s_api=self.k8s_api,
            # docker args
            docker_tag=docker_tag,
            # condor args
            njobs=njobs,
            memory=memory,
            # scanner args
            progress_interval_sec=progress_interval_sec,
            result_interval_sec=result_interval_sec,
            eventfile_b64=eventfile_b64,
            manifest=manifest,
            reco_algo=reco_algo,
            gcd_dir=gcd_dir,
            nsides=nsides,  # type: ignore[arg-type]
        )
        job.start()

        # TODO: update db?

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
        # TODO - call to k8s

        manifest = await self.manifests.mark_as_deleted(scan_id)

        self.write(dc.asdict(manifest))

    @service_account_auth(roles=[SKYMAP_SCANNER_ACCT])  # type: ignore
    async def patch(self, scan_id: str) -> None:
        """Update scan progress."""
        progress = self.get_argument("progress", type=dict, strict_type=True)

        manifest = await self.manifests.patch(
            scan_id,
            cast(dict[str, Any], progress),
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
