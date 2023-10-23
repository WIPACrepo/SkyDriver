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
        tms_args=[],
        env_vars={},
        #
        complete=True,
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
    """Test with STOPPED__PARTIAL_RESULT_GENERATED and
    IN_PROGRESS__PARTIAL_RESULT_GENERATED."""
    manifest = schema.Manifest(
        scan_id="abc123",
        timestamp=time.time(),
        is_deleted=False,
        event_i3live_json_dict={"abc": 123},
        scanner_server_args="",
        tms_args=[],
        env_vars={},
        #
        complete=is_complete,
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
        clusters=[
            schema.Cluster(
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
    )
    assert manifest.get_state() == state
