"""Integration tests for backlog runner."""

# pylint: disable=redefined-outer-name

import asyncio
import json
from typing import Any, AsyncIterator, Callable
from unittest import mock
from unittest.mock import Mock

import pytest_asyncio
import skydriver
import skydriver.images  # noqa: F401  # export
from rest_tools.client import RestClient
from skydriver.database import create_mongodb_client
from skydriver.server import make

skydriver.config.config_logging("debug")


def print_it(obj: Any) -> None:
    print(json.dumps(obj, indent=4))


########################################################################################

SCHEDD_LOOKUP = {
    "foobar": {
        "collector": "for-sure.a-collector.edu",
        "schedd": "foobar.schedd.edu",
    },
    "a-schedd": {
        "collector": "the-collector.edu",
        "schedd": "a-schedd.edu",
    },
}


TEST_WAIT_BEFORE_TEARDOWN = 2


@pytest_asyncio.fixture
async def server(
    monkeypatch: Any,
    port: int,
    mongo_clear: Any,  # pylint:disable=unused-argument
) -> AsyncIterator[Callable[[], RestClient]]:
    """Startup server in this process, yield RestClient func, then clean up."""

    # patch at directly named import that happens before running the test
    monkeypatch.setattr(skydriver.rest_handlers, "KNOWN_CONDORS", SCHEDD_LOOKUP)
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


########################################################################################

POST_SCAN_BODY = {
    "cluster": {
        "foobar": 1,
    },
    "reco_algo": "anything",
    "event_i3live_json": {"a": 22},
    "nsides": {1: 2, 3: 4},
    "real_or_simulated_event": "real",
    "docker_tag": "latest",
}

N_JOBS = 5


@mock.patch("skydriver.k8s.scanner_instance.SkymapScannerStopperJob.do_job", new=Mock())
@mock.patch("skydriver.k8s.utils.KubeAPITools.start_job")
async def test_00(kapitsj_mock: Mock, server: Callable[[], RestClient]) -> None:
    """Test backlog job starting."""
    rc = server()
    await rc.request("POST", "/scan", POST_SCAN_BODY)

    print_it(await rc.request("GET", "/scans/backlog"))

    await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.01)
    kapitsj_mock.assert_called_once()

    print_it(await rc.request("GET", "/scans/backlog"))


@mock.patch("skydriver.k8s.scanner_instance.SkymapScannerStopperJob.do_job", new=Mock())
@mock.patch("skydriver.k8s.utils.KubeAPITools.start_job")
async def test_01(kapitsj_mock: Mock, server: Callable[[], RestClient]) -> None:
    """Test backlog job starting with multiple."""
    rc = server()

    # request jobs
    for _ in range(N_JOBS):
        await asyncio.sleep(0)  # allow backlog runner to do its thing
        await rc.request("POST", "/scan", POST_SCAN_BODY)

    # inspect
    print_it(await rc.request("GET", "/scans/backlog"))
    for i in range(N_JOBS):
        await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.01)
        print_it(await rc.request("GET", "/scans/backlog"))
        assert kapitsj_mock.call_count == i + 1

    # any extra calls?
    await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 2)
    assert kapitsj_mock.call_count == N_JOBS
    print_it(await rc.request("GET", "/scans/backlog"))


@mock.patch("skydriver.k8s.scanner_instance.SkymapScannerStopperJob.do_job", new=Mock())
@mock.patch("skydriver.k8s.utils.KubeAPITools.start_job")
async def test_10(
    kapitsj_mock: Mock,
    server: Callable[[], RestClient],
) -> None:
    """Test backlog job starting with multiple cancels."""
    rc = server()

    # request jobs
    for i in range(N_JOBS):
        await asyncio.sleep(0)  # allow backlog runner to do its thing
        resp = await rc.request("POST", "/scan", POST_SCAN_BODY)
        # not asserting len in case runner is faster
        print_it(await rc.request("GET", "/scans/backlog"))
        # delete
        if i in [1, 3]:
            print_it(await rc.request("DELETE", f"/scan/{resp['scan_id']}"))

    # inspect
    print_it(await rc.request("GET", "/scans/backlog"))
    for i in range(N_JOBS - 2):
        await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.01)
        print_it(await rc.request("GET", "/scans/backlog"))
        assert kapitsj_mock.call_count >= i + 1  # in case runner is faster
    assert kapitsj_mock.call_count == N_JOBS - 2

    # any extra calls?
    await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 2)
    assert kapitsj_mock.call_count == N_JOBS - 2
    print_it(await rc.request("GET", "/scans/backlog"))
    assert not (await rc.request("GET", "/scans/backlog"))["entries"]
