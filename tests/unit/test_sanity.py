"""Test that everything is where we think it is."""

import inspect

from rest_tools.server import RestHandler
from skydriver import rest_handlers


def test_00__rest_handlers() -> None:
    """Dir-check all the REST handlers."""

    known_handlers = {
        rest_handlers.MainHandler: r"/$",
        rest_handlers.RunEventMappingHandler: r"/scans$",
        rest_handlers.ScanLauncherHandler: r"/scan$",
        rest_handlers.ScanHandler: r"/scan/(?P<scan_id>\w+)$",
        rest_handlers.ScanManifestHandler: r"/scan/(?P<scan_id>\w+)/manifest$",
        rest_handlers.ScanResultHandler: r"/scan/(?P<scan_id>\w+)/result$",
    }

    # search for all known handlers
    for handler, route in known_handlers.items():
        assert handler.ROUTE == route  # type: ignore[attr-defined]  # base type does not have ROUTE

    # find
    for _, klass in inspect.getmembers(
        rest_handlers,
        predicate=lambda x: (
            inspect.isclass(x) and issubclass(x, RestHandler) and x != RestHandler
        ),
    ):
        assert klass in known_handlers or klass == rest_handlers.BaseSkyDriverHandler
