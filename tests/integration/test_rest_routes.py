"""Integration tests for the REST server."""

import pytest
from rest_tools.client import RestClient

pytestmark = pytest.mark.asyncio  # marks all tests as async


@pytest.fixture
def rc() -> RestClient:
    """Get data source REST client."""
    return RestClient("localhost", token=None, timeout=60.0, retries=10)


########################################################################################


async def test_00(rc: RestClient) -> None:
    """Test normal scan creation and retrieval."""
    event_id = "abc123"

    # launch scan
    await rc.request("POST", "/scan")

    # query by event id
    resp = await rc.request("GET", f"/event/{event_id}")
    scan_id = resp["scan_id"]

    # LOOP:
    for i in range(10):
        # update progress
        await rc.request("PATCH", f"/scan/manifest/{scan_id}")
        # query progress
        await rc.request("GET", f"/scan/manifest/{scan_id}")

    # send finished result
    await rc.request("PUT", f"/scan/result/{scan_id}")

    # query progress
    await rc.request("GET", f"/scan/manifest/{scan_id}")

    # query result
    await rc.request("GET", f"/scan/result/{scan_id}")

    # delete manifest
    await rc.request("DELETE", f"/scan/manifest/{scan_id}")

    # query w/ scan id (fails)
    await rc.request("GET", f"/scan/manifest/{scan_id}")

    # query by event id (none)
    await rc.request("GET", f"/event/{event_id}")

    # query result (still exists)
    await rc.request("GET", f"/scan/result/{scan_id}")

    # delete result
    await rc.request("DELETE", f"/scan/result/{scan_id}")

    # query result (fails)
    await rc.request("GET", f"/scan/result/{scan_id}")
