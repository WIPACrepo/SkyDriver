"""Start server as application."""

import argparse
import asyncio

import coloredlogs  # type: ignore[import]

from . import server
from .config import ENV


async def main() -> None:
    """Start server."""
    rs = await server.make(debug=True)
    rs.startup(address=ENV.REST_HOST, port=ENV.REST_PORT)  # type: ignore[no-untyped-call]
    try:
        await asyncio.Event().wait()
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
    _args = parser.parse_args()

    coloredlogs.install(
        fmt="%(asctime)s %(hostname)s %(name)s[%(process)d] [%(filename)s:%(lineno)s/%(funcName)s()] %(levelname)s %(message)s",
        level=_args.log,
    )

    asyncio.run(main())
