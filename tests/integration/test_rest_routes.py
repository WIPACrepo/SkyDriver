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
    scan_id = resp["scan_id"]  # keep around

    # query by event id
    resp = await rc.request("GET", f"/event/{event_id}")
    assert [scan_id] == resp["scan_ids"]

    # LOOP:
    for i in range(10):
        progress = {"count": i, "double_count": i * 2, "count_pow": i**i}
        # update progress
        resp = await rc.request(
            "PATCH", f"/scan/manifest/{scan_id}", {"progress": progress}
        )
        # TODO: assert
        progress_resp = resp  # keep around

        # query progress
        resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
        assert progress_resp == resp

    # send finished result
    result = {"alpha": (i + 1) ** i, "beta": -i}
    resp = await rc.request("PUT", f"/scan/result/{scan_id}", {"result": result})
    # TODO: assert
    result_resp = resp  # keep around

    # query progress
    resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
    assert progress_resp == resp

    # query result
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert result_resp == resp

    # delete manifest
    resp = await rc.request("DELETE", f"/scan/manifest/{scan_id}")
    # TODO: assert

    # query w/ scan id (fails)
    resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
    # TODO: assert

    # query by event id (none)
    resp = await rc.request("GET", f"/event/{event_id}")
    assert not resp["scan_ids"]

    # query result (still exists)
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert result_resp == resp

    # delete result
    resp = await rc.request("DELETE", f"/scan/result/{scan_id}")
    # TODO: assert

    # query result (fails)
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    # TODO: assert
