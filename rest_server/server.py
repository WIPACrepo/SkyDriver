"""Root python script for SkyDriver REST API server interface."""


import dataclasses as dc
import inspect
import logging
from typing import Any
from urllib.parse import quote_plus

from motor.motor_tornado import MotorClient  # type: ignore
from rest_tools.server import RestHandler, RestHandlerSetup, RestServer

from . import database, handlers
from .config import ENV, is_testing


def mongodb_url() -> str:
    """Construct the MongoDB URL."""
    auth_user = quote_plus(ENV.MONGODB_AUTH_USER)
    auth_pass = quote_plus(ENV.MONGODB_AUTH_PASS)

    url = f"mongodb://{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"
    if auth_user and auth_pass:
        url = f"mongodb://{auth_user}:{auth_pass}@{ENV.MONGODB_HOST}:{ENV.MONGODB_PORT}"

    return url


async def make(debug: bool = False) -> RestServer:
    """Make a SkyDriver REST service (does not start up automatically)."""
    for field in dc.fields(ENV):
        logging.info(
            f"{field.name}\t{getattr(ENV, field.name)}\t({type(getattr(ENV, field.name)).__name__})"
        )

    rhs_config: dict[str, Any] = {"debug": debug or is_testing()}
    if ENV.AUTH_OPENID_URL:
        rhs_config["auth"] = {
            "audience": ENV.AUTH_AUDIENCE,
            "openid_url": ENV.AUTH_OPENID_URL,
        }
    args = RestHandlerSetup(rhs_config)

    # Setup DB URL
    args["mongodb_url"] = mongodb_url()

    # Configure REST Routes
    rs = RestServer(debug=debug)
    for name, klass in inspect.getmembers(
        handlers,
        predicate=lambda x: (
            inspect.isclass(x) and issubclass(x, RestHandler) and x != RestHandler
        ),
    ):
        try:
            rs.add_route(getattr(klass, "ROUTE"), klass, args)
            logging.info(f"Added handler: {name}")
        except AttributeError:
            continue

    await database.ensure_indexes(MotorClient(args["mongodb_url"]))
    return rs
