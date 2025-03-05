"""Integration tests for backlog runner."""

import asyncio
import json
from typing import Any, Callable
from unittest import mock
from unittest.mock import Mock

from rest_tools.client import RestClient

import skydriver
import skydriver.images  # noqa: F401  # export

skydriver.config.config_logging()


def print_it(obj: Any) -> None:
    print(json.dumps(obj, indent=4))


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
    "max_pixel_reco_time": 60,
}

N_JOBS = 5


@mock.patch("skydriver.k8s.utils.KubeAPITools.start_job")
async def test_00(
    kapitsj_mock: Mock,
    server: Callable[[], RestClient],
) -> None:
    """Test backlog job starting."""
    rc = server()
    await rc.request("POST", "/scan", POST_SCAN_BODY)

    print_it(await rc.request("GET", "/scans/backlog"))

    await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.01)

    # call counts
    kapitsj_mock.assert_called_once()

    print_it(await rc.request("GET", "/scans/backlog"))


@mock.patch("skydriver.k8s.utils.KubeAPITools.start_job")
async def test_01(
    kapitsj_mock: Mock,
    server: Callable[[], RestClient],
) -> None:
    """Test backlog job starting with multiple."""
    rc = server()

    # request workers
    for _ in range(N_JOBS):
        await asyncio.sleep(0)  # allow backlog runner to do its thing
        await rc.request("POST", "/scan", POST_SCAN_BODY)

    # inspect
    print_it(await rc.request("GET", "/scans/backlog"))
    for i in range(N_JOBS):
        await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.01)
        print_it(await rc.request("GET", "/scans/backlog"))
        # call counts
        assert kapitsj_mock.call_count >= i + 1  # in case runner is faster
    # call counts
    assert kapitsj_mock.call_count == N_JOBS

    await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 2)

    # any extra calls?
    assert kapitsj_mock.call_count == N_JOBS

    print_it(await rc.request("GET", "/scans/backlog"))


@mock.patch("skydriver.k8s.utils.KubeAPITools.start_job")
async def test_10(
    kapitsj_mock: Mock,
    server: Callable[[], RestClient],
) -> None:
    """Test backlog job starting with multiple cancels."""
    rc = server()

    # request workers
    for i in range(N_JOBS):
        await asyncio.sleep(0)  # allow backlog runner to do its thing
        resp = await rc.request("POST", "/scan", POST_SCAN_BODY)
        # not asserting len in case runner is faster
        print_it(await rc.request("GET", "/scans/backlog"))
        # delete
        if i in [1, 3]:
            print_it(await rc.request("DELETE", f"/scan/{resp['scan_id']}"))

    # NOTE: KubeAPITools.start_job() should be called:
    #   1x for each scan POST and 1x for each DELETE,
    #   *unless* the scan is deleted before the backlog starts it (then, just 1x)

    # inspect
    print_it(await rc.request("GET", "/scans/backlog"))
    for i in range(N_JOBS - 2):
        await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.01)
        print_it(await rc.request("GET", "/scans/backlog"))
        # call counts
        assert kapitsj_mock.call_count >= i + 1  # in case runner is faster
    # call counts
    assert kapitsj_mock.call_count == N_JOBS - 2

    await asyncio.sleep(skydriver.config.ENV.SCAN_BACKLOG_RUNNER_DELAY * 2)

    # any extra calls?
    assert kapitsj_mock.call_count == N_JOBS - 2

    print_it(await rc.request("GET", "/scans/backlog"))
    assert not (await rc.request("GET", "/scans/backlog"))["entries"]
