"""Routes handlers for the SkyDriver REST API server interface."""


import dataclasses as dc
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

        event_id = self.get_argument(
            "event_id",
            type=no_empty_str,
        )
        docker_tag = self.get_argument(
            "docker_tag",
            type=str,
            default="latest",
        )
        report_interval_sec = self.get_argument(
            "report_interval_sec",
            type=int,
            default=5 * 60,
        )
        plot_interval_sec = self.get_argument(
            "plot_interval_sec",
            type=int,
            default=10 * 60,
        )
        # physics args
        reco_algo = self.get_argument(
            "reco_algo",
            type=str,
        )
        min_nside = self.get_argument(
            "min_nside",
            type=int,
        )
        max_nside = self.get_argument(
            "max_nside",
            type=int,
        )

        manifest = await self.manifests.post(event_id)  # generates ID

        # start k8s job
        job = k8s.SkymapScannerJob(
            self.k8s_api,
            docker_tag,
            manifest.scan_id,
            report_interval_sec,
            plot_interval_sec,
            reco_algo,
            min_nside,
            max_nside,
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
        json_dict = self.get_argument("json_dict", type=dict, strict_type=True)

        result = await self.results.put(
            scan_id,
            cast(dict[str, Any], json_dict),
        )

        self.write(dc.asdict(result))


# -----------------------------------------------------------------------------
