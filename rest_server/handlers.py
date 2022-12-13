"""Routes handlers for the SkyDriver REST API server interface."""


import dataclasses as dc
from typing import Any, cast

from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
from rest_tools.server import RestHandler, handler

from . import database
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
        mongodb_url: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a BaseSkyDriverHandler object."""
        super().initialize(*args, **kwargs)
        # pylint: disable=W0201
        self.manifests = database.interface.ManifestClient(
            AsyncIOMotorClient(mongodb_url)
        )
        self.results = database.interface.ResultClient(AsyncIOMotorClient(mongodb_url))

    def prepare(self):
        # TODO: put this into rest-tools
        LOGGER.debug(
            f"{self.request.path} {self.request.method}("
            f"{self.path_args}, {self.path_kwargs}) [{self.__class__.__name__}]"
        )
        super().prepare()


# -----------------------------------------------------------------------------


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

        def valid_event_id(val: Any) -> str:
            val = str(val)
            if any(x.isspace() for x in val) or not any(x.isalnum() for x in val):
                raise TypeError(
                    f"must contain no spaces and at least one alphanumeric character {val}"
                )
            return val

        event_id = self.get_argument("event_id", type=valid_event_id)
        LOGGER.info(f"{event_id=}")
        # TODO: get more args

        manifest = await self.manifests.post(event_id)  # generates ID

        # TODO: call k8s service
        # self.cluster.launch_scan(manifest)

        # TODO: update db?

        self.write(dc.asdict(manifest))


# -----------------------------------------------------------------------------
def validate_dict_type(val: Any) -> dict:
    if not isinstance(val, dict):
        raise TypeError(f"type mismatch: 'dict' (value is '{type(val)}')")
    return val


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

        # TODO: swap in: type=dict, strict_type=True
        progress = self.get_argument("progress", type=validate_dict_type)

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

        # TODO: swap in: type=dict, strict_type=True
        json_dict = self.get_argument("json_dict", type=validate_dict_type)

        result = await self.results.put(
            scan_id,
            cast(dict[str, Any], json_dict),
        )

        self.write(dc.asdict(result))


# -----------------------------------------------------------------------------
