"""Routes handlers for the SkyDriver REST API server interface."""


import json
import logging
from dataclasses import asdict
from typing import Any

from motor.motor_tornado import MotorClient  # type: ignore
from rest_tools.server import RestHandler, handler  # type: ignore

from . import database
from .config import AUTH_SERVICE_ACCOUNT, is_testing

if is_testing():

    def service_account_auth(**kwargs):  # type: ignore
        def make_wrapper(method):
            async def wrapper(self, *args, **kwargs):
                logging.warning("TESTING: auth disabled")
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
        self.events = database.EventPseudoCollectionClient(MotorClient(mongodb_url))
        self.inflights = database.InflightCollectionClient(MotorClient(mongodb_url))
        self.results = database.ResultsCollectionClient(MotorClient(mongodb_url))


# -----------------------------------------------------------------------------


class MainHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """MainHandler is a BaseSkyDriverHandler that handles the root route."""

    ROUTE = r"/$"

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def get(self) -> None:
        """Handle GET."""
        self.write({})


# -----------------------------------------------------------------------------


class EventMappingHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles mapping an event to scan(s)."""

    ROUTE = r"/event/(?P<event_id>\w+)$"

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def get(self, event_id: str) -> None:
        """Get matching scan id(s) for the given event id."""
        scan_ids = list(self.events.get_scan_ids(event_id))

        self.write({"event_id": event_id, "scan_ids": scan_ids})

    #
    # NOTE - this needs to stay user-read-only b/c
    #         it's updated by the launching of a new scan
    #


# -----------------------------------------------------------------------------


class ScanLauncherHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles starting new scans."""

    ROUTE = r"/scan$"

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def post(self) -> None:
        """Start a new scan."""
        # TODO - get event from JSON body args

        scan_id = self.cluster.launch_scan(event)

        info = self.inflights.get(scan_id)

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )


# -----------------------------------------------------------------------------


class InflightHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on scan's progress."""

    ROUTE = r"/scan/inflight/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get scan progress."""
        info = self.inflights.get(scan_id)

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Abort a scan."""
        info = self.inflights.get(scan_id)

        self.inflights.mark_as_deleted(scan_id)

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )

    # TODO - PATCH: add progress report from scanner


# -----------------------------------------------------------------------------


class ResultsHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on persisted scan results."""

    ROUTE = r"/scan/results/(?P<scan_id>\w+)$"

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def get(self, scan_id: str) -> None:
        """Get a scan's persisted results."""
        results = self.results.get(scan_id)

        self.write(
            {
                "scan_id": scan_id,
                "results": results,  # TODO: replace 'info' with addl keys
            }
        )

    @service_account_auth(roles=[AUTH_SERVICE_ACCOUNT])  # type: ignore
    async def delete(self, scan_id: str) -> None:
        """Delete a scan's persisted results."""
        results = self.results.get(scan_id)

        self.results.mark_as_deleted(scan_id)

        self.write(
            {
                "scan_id": scan_id,
                "results": results,  # TODO: replace 'info' with addl keys
            }
        )

    # TODO - PUT: add results from scanner


# -----------------------------------------------------------------------------
