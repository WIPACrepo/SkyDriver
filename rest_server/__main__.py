"""Root python script for SkyDriver REST API server interface."""


import argparse
import asyncio
import dataclasses as dc
import inspect
import logging
from typing import Any, Dict
from urllib.parse import quote_plus

import coloredlogs  # type: ignore[import]
from rest_tools.server import RestHandler, RestHandlerSetup, RestServer
from wipac_dev_tools import from_environment_as_dataclass

from . import config, handlers


async def start(debug: bool = False) -> RestServer:
    """Start a Mad Dash REST service."""
    env = from_environment_as_dataclass(config.EnvConfig)
    for field in dc.fields(env):
        logging.info(
            f"{field.name}\t{getattr(env, field.name)}\t({type(getattr(env, field.name)).__name__})"
        )

    mongodb_auth_user = quote_plus(env.MONGODB_AUTH_USER)
    mongodb_auth_pass = quote_plus(env.MONGODB_AUTH_PASS)
    mongodb_host = env.MONGODB_HOST
    mongodb_port = env.MONGODB_PORT

    rhs_config: Dict[str, Any] = {"debug": debug}
    if env.AUTH_OPENID_URL:
        rhs_config["auth"] = {
            "audience": env.AUTH_AUDIENCE,
            "openid_url": env.AUTH_OPENID_URL,
        }
    args = RestHandlerSetup(rhs_config)

    # Setup DB URL
    mongodb_url = f"mongodb://{mongodb_host}:{mongodb_port}"
    if mongodb_auth_user and mongodb_auth_pass:
        mongodb_url = f"mongodb://{mongodb_auth_user}:{mongodb_auth_pass}@{mongodb_host}:{mongodb_port}"
    args["mongodb_url"] = mongodb_url

    # Configure REST Routes
    server = RestServer(debug=debug)
    for name, klass in inspect.getmembers(handlers):
        if not issubclass(klass, RestHandler):
            continue
        try:
            server.add_route(getattr(klass, "ROUTE"), klass, args)  # get
            logging.info(f"Added handler: {name}")
        except AttributeError:
            continue

    server.startup(address=env.REST_HOST, port=env.REST_PORT)
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

    coloredlogs.install(level=getattr(logging, _args.log.upper()))

    main()
