"""Test dynamically generating the scan state."""

import time
from unittest.mock import MagicMock, patch

import pytest

from skydriver.database import schema
from skydriver.database.schema import get_scan_state


async def test_00__scan_finished_successfully() -> None:
    """Test with SCAN_FINISHED_SUCCESSFULLY."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id=MagicMock(),
        timestamp=MagicMock(),
        is_deleted=MagicMock(),
        event_i3live_json_dict=MagicMock(),
        scanner_server_args=MagicMock(),
        #
        # now, args that actually matter:
        ewms_workflow_id="ewms123",
        progress=MagicMock(processing_stats=MagicMock(finished=True)),
    )
    assert (
        await get_scan_state(manifest, ewms_rc)
        == schema.ScanState.SCAN_FINISHED_SUCCESSFULLY.name
    )


@pytest.mark.parametrize(
    "ewms_dtype,state",
    [
        ("ABORTED", "ABORTED__PARTIAL_RESULT_GENERATED"),
        ("FINISHED", "FINISHED__PARTIAL_RESULT_GENERATED"),
        (None, schema.ScanState.IN_PROGRESS__PARTIAL_RESULT_GENERATED.name),
    ],
)
async def test_10__partial_result_generated(ewms_dtype: str, state: str) -> None:
    """Test normal and stopped variants."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id=MagicMock(),
        timestamp=MagicMock(),
        is_deleted=MagicMock(),
        event_i3live_json_dict=MagicMock(),
        scanner_server_args=MagicMock(),
        #
        # now, args that actually matter:
        ewms_workflow_id="ewms123",
        progress=MagicMock(processing_stats=MagicMock(rate={"abc": 123})),
    )

    with patch("skydriver.ewms.get_deactivated_type", return_value=ewms_dtype):
        assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__WAITING_ON_FIRST_PIXEL_RECO),
        (False, schema.ScanState.IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO),
    ],
)
async def test_20__waiting_on_first_pixel_reco(
    stopped: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.InHouseStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            clusters=[
                schema.InHouseClusterInfo(
                    orchestrator="condor",
                    location=schema.HTCondorLocation(
                        collector="foo",
                        schedd="bar",
                    ),
                    n_workers=111,
                    cluster_id="abc123",  # "" is a non-started cluster
                    starter_info={"abc": 123},
                )
            ],
        ),
        #
        progress=schema.Progress(
            "summary",
            "epilogue",
            {},
            schema.ProgressProcessingStats(
                start={},
                runtime={},
                # rate={"abc": 123},
                # end,
                # finished=True,
                # predictions,
            ),
            1.0,
            str(time.time()),
        ),
    )
    assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__WAITING_ON_CLUSTER_STARTUP),
        (False, schema.ScanState.PENDING__WAITING_ON_CLUSTER_STARTUP),
    ],
)
async def test_30__waiting_on_cluster_startup(
    stopped: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.InHouseStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            # clusters=[
            #     schema.ManualCluster(
            #         orchestrator="condor",
            #         location=schema.HTCondorLocation(
            #             collector="foo",
            #             schedd="bar",
            #         ),
            #         n_workers=111,
            #         cluster_id="abc123",  # "" is a non-started cluster
            #         starter_info={"abc": 123},
            #     )
            # ],
        ),
        #
        progress=schema.Progress(
            "summary",
            "epilogue",
            {},
            schema.ProgressProcessingStats(
                start={},
                runtime={},
                # rate={"abc": 123},
                # end,
                # finished=True,
                # predictions,
            ),
            1.0,
            str(time.time()),
        ),
    )
    assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__WAITING_ON_SCANNER_SERVER_STARTUP),
        (False, schema.ScanState.PENDING__WAITING_ON_SCANNER_SERVER_STARTUP),
    ],
)
async def test_40__waiting_on_scanner_server_startup(
    stopped: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.InHouseStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            clusters=[
                schema.InHouseClusterInfo(
                    orchestrator="condor",
                    location=schema.HTCondorLocation(
                        collector="foo",
                        schedd="bar",
                    ),
                    n_workers=111,
                    cluster_id="abc123",  # "" is a non-started cluster
                    starter_info={"abc": 123},
                )
            ],
        ),
        #
        # progress=schema.Progress(
        #     "summary",
        #     "epilogue",
        #     {},
        #     schema.ProgressProcessingStats(
        #         start={},
        #         runtime={},
        #         # rate={"abc": 123},
        #         # end,
        #         # finished=True,
        #         # predictions,
        #     ),
        #     1.0,
        #     str(time.time()),
        # ),
    )
    assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__PRESTARTUP),
        (False, schema.ScanState.PENDING__PRESTARTUP),
    ],
)
async def test_50__prestartup(stopped: bool, state: schema.ScanState) -> None:
    """Test normal and stopped varriants."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.InHouseStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            # clusters=[
            #     schema.ManualCluster(
            #         orchestrator="condor",
            #         location=schema.HTCondorLocation(
            #             collector="foo",
            #             schedd="bar",
            #         ),
            #         n_workers=111,
            #         cluster_id="abc123",  # "" is a non-started cluster
            #         starter_info={"abc": 123},
            #     )
            # ],
        ),
        #
        # progress=schema.Progress(
        #     "summary",
        #     "epilogue",
        #     {},
        #     schema.ProgressProcessingStats(
        #         start={},
        #         runtime={},
        #         # rate={"abc": 123},
        #         # end,
        #         # finished=True,
        #         # predictions,
        #     ),
        #     1.0,
        #     str(time.time()),
        # ),
    )
    assert await get_scan_state(manifest, ewms_rc) == state
