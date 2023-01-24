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

POST_SCAN_BODY = {
    "njobs": 1,
    "memory": "20G",
    "reco_algo": "anything",
    "event_i3live_json": {"a": 22},
    "nsides": {1: 2, 3: 4},
}


async def _launch_scan(rc: RestClient) -> str:
    # launch scan
    resp = await rc.request("POST", "/scan", POST_SCAN_BODY)
    scan_id = resp["scan_id"]  # keep around
    return scan_id  # type: ignore[no-any-return]


async def _do_progress(
    rc: RestClient,
    runevent: tuple[int, int] | None,
    scan_id: str,
    n: int,
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
            "runevent": resp["runevent"],  # check below
            "progress": progress,
        }
        if runevent:
            assert resp["runevent"]["run_id"] == runevent[0]
            assert resp["runevent"]["event_id"] == runevent[1]
        progress_resp = resp  # keep around
        # query progress
        resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
        assert progress_resp == resp
    return progress_resp  # type: ignore[no-any-return]


async def _server_reply_with_runevent(rc: RestClient, scan_id: str) -> tuple[int, int]:
    # reply as a the scanner server with the newly gathered run+event ids
    event_id = 123
    run_id = 456

    # update progress
    progress = await _do_progress(rc, None, scan_id, 1)
    run_id, event_id = progress["runevent"]["run_id"], progress["runevent"]["event_id"]

    # query by event id
    resp = await rc.request("GET", "/scans", {"run_id": run_id, "event_id": event_id})
    assert [scan_id] == resp["scan_ids"]

    return run_id, event_id


async def _send_result(
    rc: RestClient,
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
    runevent: tuple[int, int],
    scan_id: str,
    last_known_manifest: dict[str, Any],
    last_known_result: dict[str, Any],
) -> None:
    # delete manifest
    resp = await rc.request("DELETE", f"/scan/manifest/{scan_id}")
    assert resp == {
        "scan_id": scan_id,
        "is_deleted": True,
        "event_id": resp["event_id"],
        "progress": last_known_manifest["progress"],
    }
    if runevent:
        assert resp["runevent"]["run_id"] == runevent[0]
        assert resp["runevent"]["event_id"] == runevent[1]
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
    resp = await rc.request(
        "GET",
        "/scans",
        {"run_id": runevent[0], "event_id": runevent[1]},
    )
    assert not resp["scan_ids"]  # no matches

    # query by event id w/ incl_del
    resp = await rc.request(
        "GET",
        "/scans",
        {"run_id": runevent[0], "event_id": runevent[1], "include_deleted": True},
    )
    assert resp["scan_ids"] == [scan_id]

    # query result (still exists)
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert last_known_result == resp


async def _delete_result(
    rc: RestClient,
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

    #
    # LAUNCH SCAN
    #
    scan_id = await _launch_scan(rc)
    run_id, event_id = await _server_reply_with_runevent(rc, scan_id)

    #
    # ADD PROGRESS
    #
    manifest = await _do_progress(rc, (run_id, event_id), scan_id, 10)

    #
    # SEND RESULT
    #
    result = await _send_result(rc, scan_id, manifest)

    #
    # DELETE MANIFEST
    #
    await _delete_manifest(rc, (run_id, event_id), scan_id, manifest, result)

    #
    # DELETE RESULT
    #
    await _delete_result(rc, scan_id, result)


async def test_01__bad_data(server: Callable[[], RestClient]) -> None:
    """Failure-test scan creation and retrieval."""
    rc = server()

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
        match=rf"400 Client Error: `\w+`: \(MissingArgumentError\) required argument is missing for url: {rc.address}/scan",
    ) as e:
        await rc.request("POST", "/scan", {})
    print(e.value)
    # # bad-type body-arg
    for arg in POST_SCAN_BODY:
        for bad_val in [
            "",
            "  ",
            "\t",
            1 if not isinstance(POST_SCAN_BODY[arg], int) else None,
        ]:
            print(f"{arg}: [{bad_val}]")
            with pytest.raises(
                requests.exceptions.HTTPError,
                match=rf"400 Client Error: `{arg}`: \(ValueError\) .+ for url: {rc.address}/scan",
            ) as e:
                await rc.request("POST", "/scan", {**POST_SCAN_BODY, arg: bad_val})
            print(e.value)

    # OK
    scan_id = await _launch_scan(rc)
    run_id, event_id = await _server_reply_with_runevent(rc, scan_id)

    #
    # ADD PROGRESS
    #

    # ERROR - update PROGRESS
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
    for bad_val in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `progress`: (ValueError) type mismatch: 'dict' (value is '{type(bad_val)}') for url: {rc.address}/scan/manifest/{scan_id}"
            ),
        ) as e:
            await rc.request(
                "PATCH", f"/scan/manifest/{scan_id}", {"progress": bad_val}
            )
        print(e.value)

    # OK
    manifest = await _do_progress(rc, (run_id, event_id), scan_id, 10)

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
    for bad_val in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `scan_result`: (ValueError) type mismatch: 'dict' (value is '{type(bad_val)}') for url: {rc.address}/scan/result/{scan_id}"
            ),
        ) as e:
            await rc.request("PUT", f"/scan/result/{scan_id}", {"scan_result": bad_val})
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
    await _delete_manifest(rc, (run_id, event_id), scan_id, manifest, result)

    # also OK
    await _delete_manifest(rc, (run_id, event_id), scan_id, manifest, result)

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
