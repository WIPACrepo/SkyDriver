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
        assert handler.ROUTE == route

    # find
    for _, klass in inspect.getmembers(handlers):
        if not inspect.isclass(klass):  # is a class?
            continue
        if klass == handlers.BaseSkyDriverHandler:
            continue
        # if it's in the list, it must be a RestHandler
        if klass in list(known_handlers.keys()):
            assert issubclass(klass, RestHandler)
        # if it's a RestHandler, it must be in the list
        if issubclass(klass, RestHandler):
            assert klass in known_handlers
