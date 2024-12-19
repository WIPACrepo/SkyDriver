"""Test dynamically generating the scan state."""

import time

import pytest

from skydriver.database import schema


def test_00__scan_finished_successfully() -> None:
    """Test with SCAN_FINISHED_SUCCESSFULLY."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.ManualStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=True,
        ),
        #
        progress=schema.Progress(
            "summary",
            "epilogue",
            {},
            schema.ProgressProcessingStats(
                start={},
                runtime={},
                # rate,
                # end,
                finished=True,
                # predictions,
            ),
            1.0,
            str(time.time()),
        ),
    )
    assert manifest.get_state() == schema.ScanState.SCAN_FINISHED_SUCCESSFULLY


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__PARTIAL_RESULT_GENERATED),
        (False, schema.ScanState.IN_PROGRESS__PARTIAL_RESULT_GENERATED),
    ],
)
def test_10__partial_result_generated(
    is_complete: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.ManualStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            clusters=[
                schema.ManualCluster(
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
                rate={"abc": 123},
                # end,
                # finished=True,
                # predictions,
            ),
            1.0,
            str(time.time()),
        ),
    )
    assert manifest.get_state() == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__WAITING_ON_FIRST_PIXEL_RECO),
        (False, schema.ScanState.IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO),
    ],
)
def test_20__waiting_on_first_pixel_reco(
    is_complete: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.ManualStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            clusters=[
                schema.ManualCluster(
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
    assert manifest.get_state() == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__WAITING_ON_CLUSTER_STARTUP),
        (False, schema.ScanState.PENDING__WAITING_ON_CLUSTER_STARTUP),
    ],
)
def test_30__waiting_on_cluster_startup(
    is_complete: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.ManualStarterInfo(
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
    assert manifest.get_state() == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__WAITING_ON_SCANNER_SERVER_STARTUP),
        (False, schema.ScanState.PENDING__WAITING_ON_SCANNER_SERVER_STARTUP),
    ],
)
def test_40__waiting_on_scanner_server_startup(
    is_complete: bool, state: schema.ScanState
) -> None:
    """Test normal and stopped variants."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.ManualStarterInfo(
            tms_args=[],
            env_vars=schema.EnvVars(scanner_server=[], tms_starters=[]),
            complete=is_complete,
            clusters=[
                schema.ManualCluster(
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
    assert manifest.get_state() == state


@pytest.mark.parametrize(
    "is_complete,state",
    [
        (True, schema.ScanState.STOPPED__PRESTARTUP),
        (False, schema.ScanState.PENDING__PRESTARTUP),
    ],
)
def test_50__prestartup(is_complete: bool, state: schema.ScanState) -> None:
    """Test normal and stopped varriants."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        ewms_task=schema.ManualStarterInfo(
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
    assert manifest.get_state() == state
