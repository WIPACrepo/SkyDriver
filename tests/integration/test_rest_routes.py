"""Integration tests for the REST server."""

# pylint: disable=redefined-outer-name

import random
import re
import socket
from typing import Any, AsyncIterator, Callable
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
import requests
from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
from rest_tools.client import RestClient
from skydriver.config import config_logging
from skydriver.database.interface import drop_collections
from skydriver.server import make, mongodb_url

config_logging("debug")

StrDict = dict[str, Any]

########################################################################################


IS_REAL_EVENT = True  # for simplicity, hardcode for all requests


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
    "real_or_simulated_event": "real",
}


async def _launch_scan(rc: RestClient) -> str:
    # launch scan
    resp = await rc.request("POST", "/scan", POST_SCAN_BODY)

    server_args = (
        f"python -m skymap_scanner.server "
        f"--reco-algo {POST_SCAN_BODY['reco_algo']} "
        f"--cache-dir common-space/cache "
        f"--output-dir common-space/output "
        f"--startup-json-file common-space/startup.json "
        f"--nsides {' '.join(f'{k}:{v}' for k,v in POST_SCAN_BODY['nsides'].items())} "  # type: ignore[attr-defined]
        f"--{POST_SCAN_BODY['real_or_simulated_event']}-event"
    )

    clientmanager_args = (
        f"python resources/client_starter.py "
        f" --logs-directory common-space "
        f" --jobs {POST_SCAN_BODY['njobs']} "
        f" --memory {POST_SCAN_BODY['memory']} "
        f" --singularity-image /cvmfs/icecube.opensciencegrid.org/containers/realtime/skymap_scanner:latest "
        f" --startup-json-file common-space/startup.json "
    )

    assert resp == dict(
        scan_id=resp["scan_id"],
        is_deleted=False,
        event_i3live_json_dict=POST_SCAN_BODY["event_i3live_json"],
        event_metadata=None,
        scan_metadata=None,
        condor_clusters=[],
        progress=None,
        server_args=resp["server_args"],  # see below
        clientmanager_args=resp["clientmanager_args"],  # see below
        env_vars=resp["env_vars"],  # see below
        # TODO: check more fields in future
    )

    # check args (avoid whitespace headaches...)
    assert resp["server_args"].split() == server_args.split()
    assert resp["clientmanager_args"].split() == clientmanager_args.split()

    # check env vars
    print(resp["env_vars"])
    assert set(resp["env_vars"].keys()) == {
        "SKYSCAN_BROKER_ADDRESS",
        "SKYSCAN_BROKER_AUTH",
        "SKYSCAN_SKYDRIVER_ADDRESS",
        "SKYSCAN_SKYDRIVER_AUTH",
        "SKYSCAN_SKYDRIVER_SCAN_ID",
    }
    # check env vars, more closely
    assert set(  # these have `value`s
        k for k, v in resp["env_vars"].items() if v["value"] and not v["value_from"]
    ) == {
        "SKYSCAN_BROKER_ADDRESS",
        "SKYSCAN_SKYDRIVER_ADDRESS",
        "SKYSCAN_SKYDRIVER_SCAN_ID",
    }
    assert set(  # these have `value_from`s
        k for k, v in resp["env_vars"].items() if v["value_from"] and not v["value"]
    ) == {
        "SKYSCAN_BROKER_AUTH",
        "SKYSCAN_SKYDRIVER_AUTH",
    }
    # check env vars, even MORE closely
    assert resp["env_vars"]["SKYSCAN_BROKER_ADDRESS"]["value"] == "localhost"
    assert re.match(
        r"http://localhost:[0-9]+",
        resp["env_vars"]["SKYSCAN_SKYDRIVER_ADDRESS"]["value"],
    )
    assert len(resp["env_vars"]["SKYSCAN_SKYDRIVER_SCAN_ID"]["value"]) == 32

    # get scan_id
    assert resp["scan_id"]
    return resp["scan_id"]  # type: ignore[no-any-return]


async def _do_patch(
    rc: RestClient,
    scan_id: str,
    progress: StrDict | None = None,
    event_metadata: StrDict | None = None,
    scan_metadata: StrDict | None = None,
    condor_cluster: StrDict | None = None,
    previous_clusters: list[StrDict] | None = None,
) -> StrDict:
    # do PATCH @ /scan/manifest, assert response
    body = {}
    if progress:
        body["progress"] = progress
    if event_metadata:
        body["event_metadata"] = event_metadata
    if scan_metadata:
        body["scan_metadata"] = scan_metadata
    if condor_cluster:
        body["condor_cluster"] = condor_cluster
        assert isinstance(previous_clusters, list)  # gotta include this one too
    assert body

    resp = await rc.request("PATCH", f"/scan/manifest/{scan_id}", body)
    assert resp == dict(
        scan_id=scan_id,
        is_deleted=False,
        event_i3live_json_dict=resp["event_i3live_json_dict"],  # not checking
        event_metadata=event_metadata if event_metadata else resp["event_metadata"],
        scan_metadata=scan_metadata if scan_metadata else resp["scan_metadata"],
        condor_clusters=(
            previous_clusters + [condor_cluster]  # type: ignore[operator]  # see assert ^^^^
            if condor_cluster
            else resp["condor_clusters"]  # not checking
        ),
        progress=(
            {  # inject the auto-filled args
                **progress,
                "processing_stats": {
                    **progress["processing_stats"],
                    "end": "",
                    "finished": False,
                    "predictions": {},
                },
            }
            if progress
            else resp["progress"]  # not checking
        ),
        server_args=resp["server_args"],  # not checking
        clientmanager_args=resp["clientmanager_args"],  # not checking
        env_vars=resp["env_vars"],  # not checking
        # TODO: check more fields in future
    )
    manifest = resp  # keep around
    # query progress
    resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
    assert resp == manifest
    return manifest  # type: ignore[no-any-return]


async def _patch_progress_and_scan_metadata(
    rc: RestClient,
    scan_id: str,
    n: int,
) -> StrDict:
    # send progress updates
    for i in range(n):
        progress = dict(
            summary="it's a summary",
            epilogue="and that's all folks",
            tallies={"edgar": i, "tombo": 2 * i},
            processing_stats=dict(
                start={"the_beginning": 0.01},
                runtime={"from_the_beginning": 13.7 + i},
                rate={"hanks/hour": 1.5 / (i + 1)},
                # NOTE: these args below aren't needed but they'll be auto-filled in response
                # end: str = ""
                # finished: bool = False
                # predictions: StrDict = dc.field(default_factory=dict)  # open to requestor)
            ),
        )
        # update progress (update `scan_metadata` sometimes--not as important)
        if i % 2:  # odd
            manifest = await _do_patch(rc, scan_id, progress=progress)
        else:  # even
            manifest = await _do_patch(
                rc,
                scan_id,
                progress=progress,
                scan_metadata={"scan_id": scan_id, "foo": "bar"},
            )
    return manifest


async def _server_reply_with_event_metadata(rc: RestClient, scan_id: str) -> StrDict:
    # reply as the scanner server with the newly gathered run+event ids
    event_id = 123
    run_id = 456

    event_metadata = dict(
        run_id=run_id,
        event_id=event_id,
        event_type="groovy",
        mjd=9876543.21,
        is_real_event=IS_REAL_EVENT,
    )

    await _do_patch(rc, scan_id, event_metadata=event_metadata)

    # query by event id
    resp = await rc.request(
        "GET",
        "/scans",
        {
            "run_id": run_id,
            "event_id": event_id,
            "is_real_event": IS_REAL_EVENT,
        },
    )
    assert resp["scan_ids"] == [scan_id]

    return event_metadata


async def _clientmanager_reply(
    rc: RestClient, scan_id: str, previous_clusters: list[StrDict]
) -> StrDict:
    # reply as the clientmanager with a new condor cluster
    condor_cluster = dict(
        collector="https://le-collector.edu",
        schedd="https://un-schedd.edu",
        cluster_id=random.randint(1, 10000),
        jobs=random.randint(1, 10000),
    )

    manifest = await _do_patch(
        rc,
        scan_id,
        condor_cluster=condor_cluster,
        previous_clusters=previous_clusters,
    )
    return manifest


async def _send_result(
    rc: RestClient,
    scan_id: str,
    last_known_manifest: StrDict,
    is_final: bool,
) -> StrDict:
    # send finished result
    result = {"alpha": (11 + 1) ** 11, "beta": -11}
    if is_final:
        result["gamma"] = 5
    resp = await rc.request(
        "PUT", f"/scan/result/{scan_id}", {"scan_result": result, "is_final": is_final}
    )
    assert resp == {
        "scan_id": scan_id,
        "is_deleted": False,
        "scan_result": result,
        "is_final": is_final,
    }
    result = resp  # keep around

    # query progress
    resp = await rc.request("GET", f"/scan/manifest/{scan_id}")
    assert resp == last_known_manifest

    # query result
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert resp == result

    return result


async def _delete_manifest(
    rc: RestClient,
    event_metadata: StrDict,
    scan_id: str,
    last_known_manifest: StrDict,
    last_known_result: StrDict,
) -> None:
    # delete manifest
    resp = await rc.request("DELETE", f"/scan/manifest/{scan_id}")
    assert resp == dict(
        scan_id=scan_id,
        is_deleted=True,
        event_metadata=resp["event_metadata"],
        progress=last_known_manifest["progress"],
        event_i3live_json_dict=resp["event_i3live_json_dict"],  # not checking
        scan_metadata=resp["scan_metadata"],  # not checking
        condor_clusters=resp["condor_clusters"],  # not checking
        server_args=resp["server_args"],  # not checking
        clientmanager_args=resp["clientmanager_args"],  # not checking
        env_vars=resp["env_vars"],  # not checking
        # TODO: check more fields in future
    )
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
    assert resp == del_resp

    # query by event id (none)
    resp = await rc.request(
        "GET",
        "/scans",
        {
            "run_id": event_metadata["run_id"],
            "event_id": event_metadata["event_id"],
            "is_real_event": IS_REAL_EVENT,
        },
    )
    assert not resp["scan_ids"]  # no matches

    # query by event id w/ incl_del
    resp = await rc.request(
        "GET",
        "/scans",
        {
            "run_id": event_metadata["run_id"],
            "event_id": event_metadata["event_id"],
            "include_deleted": True,
            "is_real_event": IS_REAL_EVENT,
        },
    )
    assert resp["scan_ids"] == [scan_id]

    # query result (still exists)
    resp = await rc.request("GET", f"/scan/result/{scan_id}")
    assert resp == last_known_result


async def _delete_result(
    rc: RestClient,
    scan_id: str,
    last_known_result: StrDict,
    is_final: bool,
) -> None:
    # delete result
    resp = await rc.request("DELETE", f"/scan/result/{scan_id}")
    assert resp == {
        "scan_id": scan_id,
        "is_deleted": True,
        "is_final": is_final,
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
    assert resp == del_resp


########################################################################################


async def test_00(server: Callable[[], RestClient]) -> None:
    """Test normal scan creation and retrieval."""
    rc = server()

    #
    # LAUNCH SCAN
    #
    scan_id = await _launch_scan(rc)
    event_metadata = await _server_reply_with_event_metadata(rc, scan_id)
    manifest = await _clientmanager_reply(rc, scan_id, [])

    #
    # ADD PROGRESS
    #
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    #
    # SEND INTERMEDIATES (these can happen in any order, or even async)
    #
    # FIRST, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)
    # NEXT, spun up more workers in condor
    manifest = await _clientmanager_reply(rc, scan_id, manifest["condor_clusters"])
    # THEN, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    #
    # SEND RESULT(s)
    #
    result = await _send_result(rc, scan_id, manifest, True)

    #
    # DELETE MANIFEST
    #
    await _delete_manifest(rc, event_metadata, scan_id, manifest, result)

    #
    # DELETE RESULT
    #
    await _delete_result(rc, scan_id, result, True)


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
            "string" if not isinstance(POST_SCAN_BODY[arg], str) else None,
        ]:
            print(f"{arg}: [{bad_val}]")
            with pytest.raises(
                requests.exceptions.HTTPError,
                match=rf"400 Client Error: `{arg}`: \(ValueError\) .+ for url: {rc.address}/scan",
            ) as e:
                await rc.request("POST", "/scan", {**POST_SCAN_BODY, arg: bad_val})
            print(e.value)
    # # missing arg
    for arg in POST_SCAN_BODY:
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=rf"400 Client Error: `{arg}`: \(MissingArgumentError\) required argument is missing for url: {rc.address}/scan",
        ) as e:
            # remove arg from body
            await rc.request(
                "POST", "/scan", {k: v for k, v in POST_SCAN_BODY.items() if k != arg}
            )
        print(e.value)

    # OK
    scan_id = await _launch_scan(rc)
    event_metadata = await _server_reply_with_event_metadata(rc, scan_id)
    manifest = await _clientmanager_reply(rc, scan_id, [])

    # ATTEMPT OVERWRITE
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Cannot change an existing event_metadata for url: {rc.address}/scan/manifest"
        ),
    ) as e:
        await _do_patch(
            rc,
            scan_id,
            event_metadata=dict(
                run_id=event_metadata["run_id"],
                event_id=event_metadata["event_id"],
                event_type="funky",
                mjd=23423432.3,
                is_real_event=IS_REAL_EVENT,
            ),
        )

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
    # # empty body-arg -- this is okay, it'll silently do nothing
    # with pytest.raises(
    #     requests.exceptions.HTTPError,
    #     match=re.escape(
    #         f"422 Client Error: Attempted progress update with an empty object ({{}}) for url: {rc.address}/scan/manifest/{scan_id}"
    #     ),
    # ) as e:
    #     await rc.request("PATCH", f"/scan/manifest/{scan_id}", {"progress": {}})
    print(e.value)
    # # bad-type body-arg
    for bad_val in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=rf"400 Client Error: `progress`: \(ValueError\) missing value for field .* for url: {rc.address}/scan/manifest/{scan_id}",
        ) as e:
            await rc.request(
                "PATCH", f"/scan/manifest/{scan_id}", {"progress": bad_val}
            )
        print(e.value)

    # OK
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    # ATTEMPT OVERWRITE
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Cannot change an existing scan_metadata for url: {rc.address}/scan/manifest"
        ),
    ) as e:
        await _do_patch(rc, scan_id, scan_metadata={"boo": "baz", "bot": "fox"})

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
        await rc.request(
            "PUT", f"/scan/result/{scan_id}", {"scan_result": {}, "is_final": True}
        )
    print(e.value)
    # # bad-type body-arg
    for bad_val in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `scan_result`: (ValueError) type mismatch: 'dict' (value is '{type(bad_val)}') for url: {rc.address}/scan/result/{scan_id}"
            ),
        ) as e:
            await rc.request(
                "PUT",
                f"/scan/result/{scan_id}",
                {"scan_result": bad_val, "is_final": True},
            )
        print(e.value)

    # OK
    result = await _send_result(rc, scan_id, manifest, True)

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
    await _delete_manifest(rc, event_metadata, scan_id, manifest, result)

    # also OK
    await _delete_manifest(rc, event_metadata, scan_id, manifest, result)

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
    await _delete_result(rc, scan_id, result, True)

    # also OK
    await _delete_result(rc, scan_id, result, True)
