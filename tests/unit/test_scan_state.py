"""Test dynamically generating the scan state."""

from unittest.mock import MagicMock, patch

import pytest

from skydriver.database import schema
from skydriver.database.schema import PENDING_EWMS_WORKFLOW
from skydriver.utils import get_scan_state


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
        progress=MagicMock(
            spec_set=["processing_stats"],  # no magic strict attrs -- kind of like dict
            processing_stats=MagicMock(
                spec_set=["finished"],  # no magic strict attrs -- kind of like dict
                finished=True,
            ),
        ),
    )
    assert await get_scan_state(manifest, ewms_rc) == "SCAN_FINISHED_SUCCESSFULLY"


@pytest.mark.parametrize(
    "ewms_dtype,state",
    [
        ("ABORTED", "ABORTED__PARTIAL_RESULT_GENERATED"),
        ("FINISHED", "FINISHED__PARTIAL_RESULT_GENERATED"),
        (None, "IN_PROGRESS__PARTIAL_RESULT_GENERATED"),
    ],
)
async def test_10__partial_result_generated(ewms_dtype: str | None, state: str) -> None:
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
        progress=MagicMock(
            spec_set=["processing_stats"],  # no magic strict attrs -- kind of like dict
            processing_stats=MagicMock(
                spec_set=[  # no magic strict attrs -- kind of like dict
                    "finished",
                    "rate",
                ],
                finished=False,
                rate={"abc": 123},
            ),
        ),
    )

    with patch("skydriver.ewms.get_deactivated_type", return_value=ewms_dtype):
        assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "ewms_dtype,state",
    [
        ("ABORTED", "ABORTED__WAITING_ON_FIRST_PIXEL_RECO"),
        ("FINISHED", "FINISHED__WAITING_ON_FIRST_PIXEL_RECO"),
        (None, "IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO"),
    ],
)
async def test_20__waiting_on_first_pixel_reco(
    ewms_dtype: str | None, state: str
) -> None:
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
        progress=MagicMock(
            spec_set=["processing_stats"],  # no magic strict attrs -- kind of like dict
            processing_stats=MagicMock(
                spec_set=[  # no magic strict attrs -- kind of like dict
                    "finished",
                    "rate",
                ],
                finished=False,
                rate=None,
            ),
        ),
    )

    with patch("skydriver.ewms.get_deactivated_type", return_value=ewms_dtype):
        assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "ewms_dtype,state",
    [
        ("ABORTED", "ABORTED__WAITING_ON_SCANNER_SERVER_STARTUP"),
        ("FINISHED", "FINISHED__WAITING_ON_SCANNER_SERVER_STARTUP"),
        (None, "PENDING__WAITING_ON_SCANNER_SERVER_STARTUP"),
    ],
)
async def test_40__waiting_on_scanner_server_startup(
    ewms_dtype: str | None, state: str
) -> None:
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
        progress=None,
    )

    with patch("skydriver.ewms.get_deactivated_type", return_value=ewms_dtype):
        assert await get_scan_state(manifest, ewms_rc) == state


@pytest.mark.parametrize(
    "ewms_dtype,state",
    [
        ("ABORTED", "ABORTED__PRESTARTUP"),
        ("FINISHED", "FINISHED__PRESTARTUP"),
        (None, "PENDING__PRESTARTUP"),
    ],
)
async def test_50__prestartup(ewms_dtype: str | None, state: str) -> None:
    """Test normal and stopped varriants."""
    ewms_rc = MagicMock()

    manifest = schema.Manifest(
        scan_id=MagicMock(),
        timestamp=MagicMock(),
        is_deleted=MagicMock(),
        event_i3live_json_dict=MagicMock(),
        scanner_server_args=MagicMock(),
        #
        # now, args that actually matter:
        ewms_workflow_id=PENDING_EWMS_WORKFLOW,
        progress=None,
    )

    with patch("skydriver.ewms.get_deactivated_type", return_value=ewms_dtype):
        assert await get_scan_state(manifest, ewms_rc) == state
