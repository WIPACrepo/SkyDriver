"""Test that everything is where we think it is."""

import inspect

from rest_server import handlers
from rest_tools.server import RestHandler


def test_00__rest_handlers() -> None:
    """Dir-check all the REST handlers."""

    known_handlers = {
        handlers.MainHandler: (
            r"/$",
            ["post"],
        ),
        handlers.EventMappingHandler: (
            r"/event/(?P<event_id>\w+)$",
            ["get"],
        ),
        handlers.ScanLauncherHandler: (
            r"/scan$",
            ["post"],
        ),
        handlers.ManifestHandler: (
            r"/scan/manifest/(?P<scan_id>\w+)$",
            ["get", "delete", "patch"],
        ),
        handlers.ResultsHandler: (
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
        handlers,
        predicate=lambda x: (
            inspect.isclass(x) and issubclass(x, RestHandler) and x != RestHandler
        ),
    ):
        assert klass in known_handlers or klass == handlers.BaseSkyDriverHandler
