"""Root python script for SkyDriver REST API server interface."""


import argparse
import asyncio
import dataclasses as dc
import inspect
import logging
from typing import Any, Dict
from urllib.parse import quote_plus

import coloredlogs
from motor.motor_tornado import MotorClient  # type: ignore
from rest_tools.server import RestHandler, RestHandlerSetup, RestServer

from . import database, handlers
from .config import ENV


async def start(debug: bool = False) -> RestServer:
    """Start a Mad Dash REST service."""
    for field in dc.fields(ENV):
        logging.info(
            f"{field.name}\t{getattr(ENV, field.name)}\t({type(getattr(ENV, field.name)).__name__})"
        )

    mongodb_auth_user = quote_plus(ENV.MONGODB_AUTH_USER)
    mongodb_auth_pass = quote_plus(ENV.MONGODB_AUTH_PASS)
    mongodb_host = ENV.MONGODB_HOST
    mongodb_port = ENV.MONGODB_PORT

    rhs_config: Dict[str, Any] = {"debug": debug}
    if ENV.AUTH_OPENID_URL:
        rhs_config["auth"] = {
            "audience": ENV.AUTH_AUDIENCE,
            "openid_url": ENV.AUTH_OPENID_URL,
        }
    args = RestHandlerSetup(rhs_config)

    # Setup DB URL
    mongodb_url = f"mongodb://{mongodb_host}:{mongodb_port}"
    if mongodb_auth_user and mongodb_auth_pass:
        mongodb_url = f"mongodb://{mongodb_auth_user}:{mongodb_auth_pass}@{mongodb_host}:{mongodb_port}"
    args["mongodb_url"] = mongodb_url

    # Configure REST Routes
    server = RestServer(debug=debug)
    for name, klass in inspect.getmembers(
        handlers,
        predicate=lambda x: (
            inspect.isclass(x) and issubclass(x, RestHandler) and x != RestHandler
        ),
    ):
        try:
            server.add_route(getattr(klass, "ROUTE"), klass, args)
            logging.info(f"Added handler: {name}")
        except AttributeError:
            continue

    await database.ensure_indexes(MotorClient(mongodb_url))
    server.startup(address=ENV.REST_HOST, port=ENV.REST_PORT)
    return server


def main() -> None:
    """Configure logging and start."""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start(debug=True))
    loop.run_forever()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
    _args = parser.parse_args()

    coloredlogs.install(
        fmt="%(asctime)s %(hostname)s %(name)s[%(process)d] [%(filename)s:%(lineno)s/%(funcName)s()] %(levelname)s %(message)s",
        level=_args.log,
    )

    main()
