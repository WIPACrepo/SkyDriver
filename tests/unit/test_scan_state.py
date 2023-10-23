"""Test dynamically generating the scan state."""


import time

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
