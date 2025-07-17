"""Test the scan pod watchdog runner."""

import asyncio
from typing import Callable
from unittest import mock
from unittest.mock import AsyncMock, call

from rest_tools.client import RestClient

import skydriver
from skydriver.config import ENV

skydriver.config.config_logging()


@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._request_replacement_rescan",
    new_callable=AsyncMock,
)
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._has_scan_been_rescanned",
    side_effect=[False, True, False],
    #            A      D x   E
)
@mock.patch(
    "skydriver.k8s.utils.KubeAPITools.has_transiently_killed_pod",
    side_effect=[True, False, True, True],
)  #             A     C x    D     E               # noqa
@mock.patch(
    "skydriver.utils.get_scan_state_if_final_result_received",
    side_effect=[False, True, False, False, False],
)  #             A      B x   C      D      E       # noqa
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._get_recent_scans",
    return_value=["scan_A", "scan_B", "scan_C", "scan_D", "scan_E"],
)
async def test_watchdog_filtering_per_stage(
    recent_mock: AsyncMock,
    final_result_mock: AsyncMock,
    pod_killed_mock: AsyncMock,
    rescanned_mock: AsyncMock,
    rescan_request_mock: AsyncMock,
    server: Callable[[], RestClient],
) -> None:
    """Test that watchdog filters scans at each filtering stage individually."""

    # Wait for one watchdog loop
    await asyncio.sleep(ENV.SCAN_BACKLOG_RUNNER_DELAY * 1.1)

    # Assert calls per filtering stage
    assert recent_mock.call_count == 1
    assert final_result_mock.await_count == 5  # called for each scan
    assert pod_killed_mock.call_count == 4  # only for those that passed final result
    assert rescanned_mock.await_count == 3  # only for those that passed pod check

    # Rescans
    expected_rescans = ["scan_A", "scan_E"]
    rescan_request_mock.assert_has_awaits(
        [call(mock.ANY, scan_id) for scan_id in expected_rescans],
    )
    assert rescan_request_mock.await_count == len(expected_rescans)
