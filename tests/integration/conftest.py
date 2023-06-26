"""Fixtures."""


import asyncio
import socket
from typing import Any, AsyncIterator, Callable
from unittest import mock
from unittest.mock import Mock

import pytest
import pytest_asyncio
import skydriver
import skydriver.images  # noqa: F401  # export
from rest_tools.client import RestClient
from skydriver.database import create_mongodb_client
from skydriver.database.interface import drop_collections
from skydriver.server import make


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port


@pytest_asyncio.fixture
async def mongo_clear() -> Any:
    """Clear the MongoDB after test completes."""
    motor_client = await create_mongodb_client()
    try:
        await drop_collections(motor_client)
        yield
    finally:
        await drop_collections(motor_client)


########################################################################################


KNOWN_CLUSTERS = {
    "foobar": {
        "orchestrator": "condor",
        "location": {
            "collector": "for-sure.a-collector.edu",
            "schedd": "foobar.schedd.edu",
        },
    },
    "a-schedd": {
        "orchestrator": "condor",
        "location": {
            "collector": "the-collector.edu",
            "schedd": "a-schedd.edu",
        },
    },
    "cloud": {
        "orchestrator": "k8s",
        "location": {
            "host": "cumulus.nimbus.com",
            "namespace": "stratus",
        },
    },
}


@pytest.fixture(scope="session")
def known_clusters() -> dict:
    return KNOWN_CLUSTERS


TEST_WAIT_BEFORE_TEARDOWN = 2.0


@pytest.fixture(scope="session")
def test_wait_before_teardown() -> float:
    return TEST_WAIT_BEFORE_TEARDOWN


@pytest_asyncio.fixture
async def server(
    monkeypatch: Any,
    port: int,
    mongo_clear: Any,  # pylint:disable=unused-argument
) -> AsyncIterator[Callable[[], RestClient]]:
    """Startup server in this process, yield RestClient func, then clean up."""

    # patch at directly named import that happens before running the test
    monkeypatch.setattr(skydriver.rest_handlers, "KNOWN_CLUSTERS", KNOWN_CLUSTERS)
    monkeypatch.setattr(
        skydriver.k8s.scanner_instance, "KNOWN_CLUSTERS", KNOWN_CLUSTERS
    )
    monkeypatch.setattr(
        skydriver.rest_handlers, "WAIT_BEFORE_TEARDOWN", TEST_WAIT_BEFORE_TEARDOWN
    )

    mongo_client = await create_mongodb_client()
    k8s_api = Mock()
    backlog_task = asyncio.create_task(
        skydriver.k8s.scan_backlog.startup(mongo_client, k8s_api)
    )
    await asyncio.sleep(0)  # start up previous task
    rs = await make(mongo_client, k8s_api)
    rs.startup(address="localhost", port=port)  # type: ignore[no-untyped-call]

    def client() -> RestClient:
        return RestClient(f"http://localhost:{port}", retries=0)

    try:
        yield client
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]
        backlog_task.cancel()
