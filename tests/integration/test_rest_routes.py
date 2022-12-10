"""Integration tests for the REST server."""

# pylint: disable=redefined-outer-name

import socket
from typing import Any, AsyncIterator, Callable

import pytest
import pytest_asyncio
from motor.motor_tornado import MotorClient  # type: ignore
from rest_server.config import config_logging
from rest_server.database import drop_collections
from rest_server.server import make, mongodb_url
from rest_tools.client import RestClient

########################################################################################

config_logging("debug")


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port


@pytest_asyncio.fixture
async def mongo_clear() -> Any:
    """Clear the MongoDB after test completes."""
    motor_client = MotorClient(mongodb_url())
    try:
        await drop_collections(motor_client)
        yield
    finally:
        await drop_collections(motor_client)


@pytest_asyncio.fixture
async def server(
    monkeypatch: Any,
    port: int,
    mongo_clear: Any,  # pylint:disable=unused-argument
) -> AsyncIterator[Callable[[], RestClient]]:
    """Startup server in this process, yield RestClient func, then clean up."""
    monkeypatch.setenv("PORT", str(port))

    rs = await make(debug=True)
    rs.startup(address="localhost", port=port)  # type: ignore[no-untyped-call]

    def client() -> RestClient:
        return RestClient(f"http://localhost:{port}", timeout=1, retries=0)

    try:
        yield client
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]


########################################################################################


async def test_00(server: Callable[[], RestClient]) -> None:
    """Test normal scan creation and retrieval."""
    rc = server()

    event_id = "abc123"

    # launch scan
    resp = await rc.request("POST", "/scan", {"event_id": event_id})
    scan_id = resp["scan_id"]

    # query by event id
    resp = await rc.request("GET", f"/event/{event_id}")
    assert [scan_id] == resp["scan_ids"]

    # LOOP:
    for i in range(10):
        # update progress
        await rc.request("PATCH", f"/scan/manifest/{scan_id}")
        # query progress
        await rc.request("GET", f"/scan/manifest/{scan_id}")

    # send finished result
    await rc.request("PUT", f"/scan/result/{scan_id}")

    # query progress
    await rc.request("GET", f"/scan/manifest/{scan_id}")

    # query result
    await rc.request("GET", f"/scan/result/{scan_id}")

    # delete manifest
    await rc.request("DELETE", f"/scan/manifest/{scan_id}")

    # query w/ scan id (fails)
    await rc.request("GET", f"/scan/manifest/{scan_id}")

    # query by event id (none)
    await rc.request("GET", f"/event/{event_id}")

    # query result (still exists)
    await rc.request("GET", f"/scan/result/{scan_id}")

    # delete result
    await rc.request("DELETE", f"/scan/result/{scan_id}")

    # query result (fails)
    await rc.request("GET", f"/scan/result/{scan_id}")
