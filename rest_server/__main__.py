"""Start server as application."""

import argparse
import asyncio

from . import server
from .config import ENV, config_logging


async def main() -> None:
    """Start server."""
    rs = await server.make()
    rs.startup(address=ENV.REST_HOST, port=ENV.REST_PORT)  # type: ignore[no-untyped-call]
    try:
        await asyncio.Event().wait()
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
    _args = parser.parse_args()

    config_logging(_args.log)

    asyncio.run(main())
