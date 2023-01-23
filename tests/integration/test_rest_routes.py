"""Integration tests for the REST server."""

# pylint: disable=redefined-outer-name

import re
import socket
from typing import Any, AsyncIterator, Callable
from unittest.mock import ANY, AsyncMock, Mock, patch, sentinel

import pytest
import pytest_asyncio
import requests
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
from rest_tools.client import RestClient
from skydriver.config import config_logging
from skydriver.database.interface import drop_collections
from skydriver.server import make, mongodb_url

########################################################################################

config_logging("debug")


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port


@pytest_asyncio.fixture
async def mongo_clear() -> Any:
    """Clear the MongoDB after test completes."""
    motor_client = AsyncIOMotorClient(mongodb_url())
    try:
        await drop_collections(motor_client)
        yield
    finally:
        await drop_collections(motor_client)


@pytest_asyncio.fixture
async def server(
    monkeypatch: Any,
    port: int,
    mongo_clear: Any,  # pylint:disable=unused-argument
) -> AsyncIterator[Callable[[], RestClient]]:
    """Startup server in this process, yield RestClient func, then clean up."""
    # monkeypatch.setenv("PORT", str(port))

    with patch("skydriver.server.setup_k8s_client", return_value=Mock()):
        rs = await make(debug=True)
    rs.startup(address="localhost", port=port)  # type: ignore[no-untyped-call]

    def client() -> RestClient:
        return RestClient(f"http://localhost:{port}", timeout=1, retries=0)

    try:
        yield client
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]


########################################################################################


async def _launch_scan(rc: RestClient, event_id: str) -> str:
    # launch scan
    resp = await rc.request("POST", "/scan", {"event_id": event_id})
    scan_id = resp["scan_id"]  # keep around
    # query by event id
    resp = await rc.request("GET", f"/event/{event_id}")
    assert [scan_id] == resp["scan_ids"]
    return scan_id  # type: ignore[no-any-return]


async def _do_progress(
    rc: RestClient, event_id: str, scan_id: str, n: int
) -> dict[str, Any]:
    for i in range(n):
        progress = {"count": i, "double_count": i * 2, "count_pow": i**i}
        # update progress
        resp = await rc.request(
            "PATCH", f"/scan/manifest/{scan_id}", {"progress": progress}
        )
        assert resp == {
            "scan_id": scan_id,
            "is_deleted": False,
            "event_id": event_id,
            "progress": progress,
        }
        progress_resp = resp  # keep around
        # query progress
        resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
        assert progress_resp == resp
    return progress_resp  # type: ignore[no-any-return]


async def _send_result(
    rc: RestClient,
    # event_id: str,
    scan_id: str,
    last_known_manifest: dict[str, Any],
) -> dict[str, Any]:
    # send finished result
    result = {"alpha": (11 + 1) ** 11, "beta": -11}
    resp = await rc.request("PUT", f"/scan/result/{scan_id}", {"scan_result": result})
    assert resp == {
        "scan_id": scan_id,
        "is_deleted": False,
        "scan_result": result,
    }
    result = resp  # keep around

    # query progress
    resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
    assert last_known_manifest == resp

    # query result
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert result == resp

    return result


async def _delete_manifest(
    rc: RestClient,
    event_id: str,
    scan_id: str,
    last_known_manifest: dict[str, Any],
    last_known_result: dict[str, Any],
) -> None:
    # delete manifest
    resp = await rc.request("DELETE", f"/scan/manifest/{scan_id}")
    assert resp == {
        "scan_id": scan_id,
        "is_deleted": True,
        "event_id": event_id,
        "progress": last_known_manifest["progress"],
    }
    del_resp = resp  # keep around

    # query w/ scan id (fails)
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/manifest/{scan_id}"
        ),
    ):
        await rc.request("GET", f"/scan/manifest/{scan_id}")

    # query w/ incl_del
    resp = await rc.request(
        "GET", f"/scan/manifest/{scan_id}", {"include_deleted": True}
    )
    assert del_resp == resp

    # query by event id (none)
    resp = await rc.request("GET", f"/event/{event_id}")
    assert not resp["scan_ids"]  # no matches

    # query by event id w/ incl_del
    resp = await rc.request("GET", f"/event/{event_id}", {"include_deleted": True})
    assert resp["scan_ids"] == [scan_id]

    # query result (still exists)
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert last_known_result == resp


async def _delete_result(
    rc: RestClient,
    # event_id: str,
    scan_id: str,
    last_known_result: dict[str, Any],
) -> None:
    # delete result
    resp = await rc.request("DELETE", f"/scan/result/{scan_id}")
    assert resp == {
        "scan_id": scan_id,
        "is_deleted": True,
        "scan_result": last_known_result["scan_result"],
    }
    del_resp = resp  # keep around

    # query result (fails)
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/result/{scan_id}"
        ),
    ):
        await rc.request("GET", f"/scan/result/{scan_id}")

    # query w/ incl_del
    resp = await rc.request("GET", f"/scan/result/{scan_id}", {"include_deleted": True})
    assert del_resp == resp


########################################################################################


async def test_00(server: Callable[[], RestClient]) -> None:
    """Test normal scan creation and retrieval."""
    rc = server()
    event_id = "abc123"

    #
    # PRECHECK EMPTY DB
    #

    # query by event id
    resp = await rc.request("GET", f"/event/{event_id}")
    assert not resp["scan_ids"]  # no matches

    #
    # LAUNCH SCAN
    #
    scan_id = await _launch_scan(rc, event_id)

    #
    # ADD PROGRESS
    #
    manifest = await _do_progress(rc, event_id, scan_id, 10)

    #
    # SEND RESULT
    #
    result = await _send_result(rc, scan_id, manifest)

    #
    # DELETE MANIFEST
    #
    await _delete_manifest(rc, event_id, scan_id, manifest, result)

    #
    # DELETE RESULT
    #
    await _delete_result(rc, scan_id, result)


async def test_01__bad_data(server: Callable[[], RestClient]) -> None:
    """Failure-test scan creation and retrieval."""
    rc = server()
    event_id = "abc123"

    #
    # PRECHECK EMPTY DB
    #

    # query by event id
    resp = await rc.request("GET", f"/event/{event_id}")
    assert not resp["scan_ids"]  # no matches

    # bad url
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(f"404 Client Error: Not Found for url: {rc.address}/event"),
    ) as e:
        await rc.request("GET", "/event")
    print(e.value)

    #
    # LAUNCH SCAN
    #

    # ERROR
    # # empty body
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: `event_id`: (MissingArgumentError) required argument is missing for url: {rc.address}/scan"
        ),
    ) as e:
        await rc.request("POST", "/scan", {})
    print(e.value)
    # # bad-type body-arg
    for bad_arg in ["", "  ", "\t"]:
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `event_id`: (ValueError) cannot use empty string for url: {rc.address}/scan"
            ),
        ) as e:
            await rc.request("POST", "/scan", {"event_id": bad_arg})
        print(e.value)

    # OK
    scan_id = await _launch_scan(rc, event_id)

    #
    # ADD PROGRESS
    #

    # ERROR - update progress
    # # no arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/manifest"
        ),
    ) as e:
        await rc.request("PATCH", "/scan/manifest")
    print(e.value)
    # # no arg w/ body
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/manifest"
        ),
    ) as e:
        await rc.request("PATCH", "/scan/manifest", {"progress": {"a": 1}})
    print(e.value)
    # # empty body
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: `progress`: (MissingArgumentError) required argument is missing for url: {rc.address}/scan/manifest/{scan_id}"
        ),
    ) as e:
        await rc.request("PATCH", f"/scan/manifest/{scan_id}", {})
    print(e.value)
    # # empty body-arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"422 Client Error: Attempted progress update with an empty object ({{}}) for url: {rc.address}/scan/manifest/{scan_id}"
        ),
    ) as e:
        await rc.request("PATCH", f"/scan/manifest/{scan_id}", {"progress": {}})
    print(e.value)
    # # bad-type body-arg
    for bad_arg in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `progress`: (ValueError) type mismatch: 'dict' (value is '{type(bad_arg)}') for url: {rc.address}/scan/manifest/{scan_id}"
            ),
        ) as e:
            await rc.request(
                "PATCH", f"/scan/manifest/{scan_id}", {"progress": bad_arg}
            )
        print(e.value)

    # OK
    manifest = await _do_progress(rc, event_id, scan_id, 10)

    # # no arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/manifest"
        ),
    ) as e:
        await rc.request("GET", "/scan/manifest")
    print(e.value)

    #
    # SEND RESULT
    #

    # ERROR
    # # no arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/result"
        ),
    ) as e:
        await rc.request("PUT", "/scan/result")
    print(e.value)
    # # no arg w/ body
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/result"
        ),
    ) as e:
        await rc.request("PUT", "/scan/result", {"scan_result": {"bb": 22}})
    print(e.value)
    # # empty body
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: `scan_result`: (MissingArgumentError) required argument is missing for url: {rc.address}/scan/result/{scan_id}"
        ),
    ) as e:
        await rc.request("PUT", f"/scan/result/{scan_id}", {})
    print(e.value)
    # # empty body-arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"422 Client Error: Attempted to add result with an empty object ({{}}) for url: {rc.address}/scan/result/{scan_id}"
        ),
    ) as e:
        await rc.request("PUT", f"/scan/result/{scan_id}", {"scan_result": {}})
    print(e.value)
    # # bad-type body-arg
    for bad_arg in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `scan_result`: (ValueError) type mismatch: 'dict' (value is '{type(bad_arg)}') for url: {rc.address}/scan/result/{scan_id}"
            ),
        ) as e:
            await rc.request("PUT", f"/scan/result/{scan_id}", {"scan_result": bad_arg})
        print(e.value)

    # OK
    result = await _send_result(rc, scan_id, manifest)

    # # no arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/result"
        ),
    ) as e:
        await rc.request("GET", "/scan/result")
    print(e.value)

    #
    # DELETE MANIFEST
    #

    # ERROR
    # # no arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/manifest"
        ),
    ) as e:
        await rc.request("DELETE", "/scan/manifest")
    print(e.value)

    # OK
    await _delete_manifest(rc, event_id, scan_id, manifest, result)

    # also OK
    await _delete_manifest(rc, event_id, scan_id, manifest, result)

    #
    # DELETE RESULT
    #

    # # no arg
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/result"
        ),
    ) as e:
        await rc.request("DELETE", "/scan/result")
    print(e.value)

    # OK
    await _delete_result(rc, scan_id, result)

    # also OK
    await _delete_result(rc, scan_id, result)
