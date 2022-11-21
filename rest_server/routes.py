"""Routes handlers for the SkyDriver REST API server interface."""


import json
import logging
from dataclasses import asdict
from typing import Any

from motor.motor_tornado import MotorClient  # type: ignore
from rest_tools.server import RestHandler, handler  # type: ignore

from . import db_interface
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
        self.db = db_interface.SkyDriverDatabaseClient(MotorClient(mongodb_url))


# -----------------------------------------------------------------------------


class MainHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """MainHandler is a BaseSkyDriverHandler that handles the root route."""

    ROUTE = r"/$"

    def get(self) -> None:
        """Handle GET."""
        self.write({})


# -----------------------------------------------------------------------------


class EventMappingHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles mapping an event to scan(s)."""

    ROUTE = r"/event/(?P<event_id>\w+)$"

    def get(self, event_id: str) -> None:
        """Get matching scan id(s) for the given event id."""
        scan_ids = []

        self.write({"event_id": event_id, "scan_ids": scan_ids})


# -----------------------------------------------------------------------------


class ScanLauncherHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles starting new scans."""

    ROUTE = r"/scan$"

    def post(self) -> None:
        """Start a new scan."""
        scan_id = "0"
        info = {}

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )


# -----------------------------------------------------------------------------


class ScanInfoHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on an in-progress scan."""

    ROUTE = r"/scan/(?P<scan_id>\w+)$"

    def get(self, scan_id: str) -> None:
        """Get scan progress."""
        info = {}

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )

    def delete(self, scan_id: str) -> None:
        """Abort a scan."""
        info = {}

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )


# -----------------------------------------------------------------------------


class ResultsHandler(BaseSkyDriverHandler):  # pylint: disable=W0223
    """Handles actions on persisted scan results."""

    ROUTE = r"/results/(?P<scan_id>\w+)$"

    def get(self, scan_id: str) -> None:
        """Get a scan's persisted results."""
        info = {}

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )

    def delete(self, scan_id: str) -> None:
        """Delete a scan's persisted results."""
        info = {}

        self.write(
            {
                "scan_id": scan_id,
                "info": info,  # TODO: replace 'info' with addl keys
            }
        )


# -----------------------------------------------------------------------------
