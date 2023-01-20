"""Test that everything is where we think it is."""

import inspect

from rest_tools.server import RestHandler
from skydriver import rest_handlers


def test_00__rest_handlers() -> None:
    """Dir-check all the REST handlers."""

    known_handlers = {
        rest_handlers.MainHandler: (
            r"/$",
            ["post"],
        ),
        rest_handlers.EventMappingHandler: (
            r"/event/(?P<event_id>\w+)$",
            ["get"],
        ),
        rest_handlers.ScanLauncherHandler: (
            r"/scan$",
            ["post"],
        ),
        rest_handlers.ManifestHandler: (
            r"/scan/manifest/(?P<scan_id>\w+)$",
            ["get", "delete", "patch"],
        ),
        rest_handlers.ResultsHandler: (
            r"/scan/result/(?P<scan_id>\w+)$",
            ["get", "delete", "put"],
        ),
    }

    # search for all known handlers
    for handler, (route, methods) in known_handlers.items():
        assert all(x in dir(handler) for x in methods)
        assert handler.ROUTE == route  # type: ignore[attr-defined]  # base type does not have ROUTE

    # find
    for _, klass in inspect.getmembers(
        rest_handlers,
        predicate=lambda x: (
            inspect.isclass(x) and issubclass(x, RestHandler) and x != RestHandler
        ),
    ):
        assert klass in known_handlers or klass == rest_handlers.BaseSkyDriverHandler
