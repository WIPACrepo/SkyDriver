"""Test the scan pod watchdog runner."""

import asyncio
from unittest import mock
from unittest.mock import AsyncMock, call

import skydriver
from skydriver.config import ENV

skydriver.config.config_logging()


@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._request_replacement_rescan",
    new_callable=AsyncMock,
)
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog.KubeAPITools.has_transiently_killed_pod",
    side_effect=[True, False],
    # A = pass, E = fail
)
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._has_scan_been_rescanned_too_many_times_too_recently",
    side_effect=[False, True, False],
    # A = pass, D = fail, E = pass
)
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._has_scan_been_rescanned",
    side_effect=[False, True, False, False],
    # A = pass, C = fail, D = pass, E = pass
)
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog.get_scan_state_if_final_result_received",
    side_effect=[False, True, False, False, False],
    # B = fail
)
@mock.patch(
    "skydriver.background_runners.scan_pod_watchdog._get_recent_scans",
    return_value=["scan_A", "scan_B", "scan_C", "scan_D", "scan_E"],
)
async def test_watchdog_filtering_per_stage(
    recent_mock: AsyncMock,
    final_result_mock: AsyncMock,
    rescanned_mock: AsyncMock,
    too_many_mock: AsyncMock,
    pod_killed_mock: AsyncMock,
    rescan_request_mock: AsyncMock,
    server,
) -> None:
    """Test that each scan is filtered out by a unique stage."""

    await asyncio.sleep(ENV.SCAN_POD_WATCHDOG_DELAY * 1.1)

    # Assert filter stage counts
    assert recent_mock.call_count == 1
    assert final_result_mock.await_count == 5
    assert rescanned_mock.await_count == 4
    assert too_many_mock.await_count == 3
    assert pod_killed_mock.call_count == 2

    rescan_request_mock.assert_has_awaits([call(mock.ANY, "scan_A")])
    assert rescan_request_mock.await_count == 1
