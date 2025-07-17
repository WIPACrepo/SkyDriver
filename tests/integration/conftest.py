"""Fixtures."""

import asyncio
import socket
from typing import Any, AsyncIterator, Callable
from unittest import mock
from unittest.mock import Mock

import kubernetes.client  # type: ignore[import-untyped]
import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from rest_tools.client import RestClient

import skydriver
import skydriver.images  # noqa: F401  # export
from skydriver.__main__ import main
from skydriver.database import create_mongodb_client
from skydriver.database.utils import drop_database


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
async def mongo_clear() -> AsyncIterator[None]:
    """Clear the MongoDB after test completes."""
    motor_client = await create_mongodb_client()
    try:
        await drop_database(motor_client)
        yield
    finally:
        await drop_database(motor_client)


########################################################################################


KNOWN_CLUSTERS = {
    "foobar": {
        "orchestrator": "condor",
        "location": {
            "collector": "for-sure.a-collector.edu",
            "schedd": "foobar.schedd.edu",
        },
        "v1envvars": [
            kubernetes.client.V1EnvVar(
                name="CONDOR_TOKEN",
                value_from=kubernetes.client.V1EnvVarSource(
                    secret_key_ref=kubernetes.client.V1SecretKeySelector(
                        name="",
                        key="condor_token_foobar",
                    )
                ),
            )
        ],
    },
    "a-schedd": {
        "orchestrator": "condor",
        "location": {
            "collector": "the-collector.edu",
            "schedd": "a-schedd.edu",
        },
        "v1envvars": [
            kubernetes.client.V1EnvVar(
                name="CONDOR_TOKEN",
                value_from=kubernetes.client.V1EnvVarSource(
                    secret_key_ref=kubernetes.client.V1SecretKeySelector(
                        name="",
                        key="a_condor_token",
                    )
                ),
            )
        ],
    },
    "cloud": {
        "orchestrator": "k8s",
        "location": {
            "host": "cumulus.nimbus.com",
            "namespace": "stratus",
        },
        "v1envvars": [
            kubernetes.client.V1EnvVar(
                name="WORKER_K8S_CONFIG_FILE_BASE64",
                value_from=kubernetes.client.V1EnvVarSource(
                    secret_key_ref=kubernetes.client.V1SecretKeySelector(
                        name="",
                        key="worker_k8s_config_cloud_file_base64",
                    )
                ),
            )
        ],
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
async def mongo_client() -> AsyncIOMotorClient:  # type: ignore[valid-type]
    """A fixture to keep number of mongo connections to a minimum (aka 1)."""
    return await create_mongodb_client()


@pytest_asyncio.fixture
async def server(
    monkeypatch: Any,
    port: int,
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    mongo_clear: Any,  # pylint:disable=unused-argument
) -> AsyncIterator[Callable[[], RestClient]]:
    """Startup server in this process, yield RestClient func, then clean up."""

    # NOTE: cannot use @mock.patch with @pytest_asyncio.fixture
    # NOTE: cannot use `yield from` on async iterator

    with mock.patch("skydriver.k8s.utils.KubeAPITools.start_job", return_value=None):
        async for y in _server(monkeypatch, port, mongo_client):
            yield y


async def _server(
    monkeypatch: Any,
    port: int,
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
) -> AsyncIterator[Callable[[], RestClient]]:
    # patch at directly named import that happens before running the test
    monkeypatch.setattr(skydriver.rest_handlers, "KNOWN_CLUSTERS", KNOWN_CLUSTERS)
    monkeypatch.setattr(skydriver.config, "KNOWN_CLUSTERS", KNOWN_CLUSTERS)
    monkeypatch.setattr(
        skydriver.rest_handlers, "WAIT_BEFORE_TEARDOWN", TEST_WAIT_BEFORE_TEARDOWN
    )

    # k8s_batch_api = Mock()
    # ewms_rc = setup_ewms_client()
    # backlog_task = asyncio.create_task(
    #     skydriver.background_runners.scan_launcher.run(
    #         mongo_client,
    #         k8s_batch_api,
    #     )
    # )
    # await asyncio.sleep(0)  # start up previous task
    # rs = await make(mongo_client, k8s_batch_api, ewms_rc)
    # rs.startup(address="localhost", port=port)  # type: ignore[no-untyped-call]

    def client() -> RestClient:
        return RestClient(f"http://localhost:{port}", retries=0)

    with mock.patch("skydriver.k8s.setup_k8s_batch_api", return_value=Mock()):
        main_task = asyncio.create_task(main(address="localhost", port=port))
    await asyncio.sleep(0)  # start up previous task

    try:
        await asyncio.sleep(0.5)  # wait for server startup
        yield client
    finally:
        main_task.cancel()
        try:
            await main_task
        except asyncio.CancelledError:
            pass
