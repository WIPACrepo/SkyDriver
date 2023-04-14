"""Test that everything is where we think it is."""

import inspect

from rest_tools.server import RestHandler
from skydriver import rest_handlers

REST_METHODS = {"post", "get", "put", "patch", "delete"}


def test_00__rest_handlers() -> None:
    """Dir-check all the REST handlers."""

    known_handlers = {
        rest_handlers.MainHandler: (
            r"/$",
            ["post"],
        ),
        rest_handlers.RunEventMappingHandler: (
            r"/scans$",
            ["get"],
        ),
        rest_handlers.ScanLauncherHandler: (
            r"/scan$",
            ["post"],
        ),
        rest_handlers.ScanHandler: (
            r"/scan/(?P<scan_id>\w+)$",
            ["get", "delete"],
        ),
        rest_handlers.ScanManifestHandler: (
            r"/scan/(?P<scan_id>\w+)/manifest$",
            ["get", "patch"],
        ),
        rest_handlers.ScanResultHandler: (
            r"/scan/(?P<scan_id>\w+)/result$",
            ["get", "put"],
        ),
    }

    # search for all known handlers
    for handler, (route, methods) in known_handlers.items():
        assert all(x in dir(handler) for x in methods)
        assert not any(x in dir(handler) for x in REST_METHODS - set(methods))
        assert handler.ROUTE == route  # type: ignore[attr-defined]  # base type does not have ROUTE

    # find
    for _, klass in inspect.getmembers(
        rest_handlers,
        predicate=lambda x: (
            inspect.isclass(x) and issubclass(x, RestHandler) and x != RestHandler
        ),
    ):
        assert klass in known_handlers or klass == rest_handlers.BaseSkyDriverHandler
