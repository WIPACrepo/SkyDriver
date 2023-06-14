"""Integration tests for the REST server."""

# pylint: disable=redefined-outer-name

import asyncio
import os
import random
import re
from typing import Any, AsyncIterator, Callable
from unittest.mock import Mock

import pytest
import pytest_asyncio
import requests
import skydriver
import skydriver.images  # noqa: F401  # export
from rest_tools.client import RestClient
from skydriver.database import create_mongodb_client
from skydriver.server import make

skydriver.config.config_logging("debug")

StrDict = dict[str, Any]

########################################################################################

KNOWN_CONDOR_CLUSTERS = {
    "foobar": {
        "collector": "for-sure.a-collector.edu",
        "schedd": "foobar.schedd.edu",
    },
    "a-schedd": {
        "collector": "the-collector.edu",
        "schedd": "a-schedd.edu",
    },
}


IS_REAL_EVENT = True  # for simplicity, hardcode for all requests
TEST_WAIT_BEFORE_TEARDOWN = 2


@pytest_asyncio.fixture
async def server(
    monkeypatch: Any,
    port: int,
    mongo_clear: Any,  # pylint:disable=unused-argument
) -> AsyncIterator[Callable[[], RestClient]]:
    """Startup server in this process, yield RestClient func, then clean up."""

    # patch at directly named import that happens before running the test
    monkeypatch.setattr(
        skydriver.rest_handlers, "KNOWN_CONDOR_CLUSTERS", KNOWN_CONDOR_CLUSTERS
    )
    monkeypatch.setattr(
        skydriver.rest_handlers, "WAIT_BEFORE_TEARDOWN", TEST_WAIT_BEFORE_TEARDOWN
    )

    rs = await make(
        mongo_client=await create_mongodb_client(),
        k8s_api=Mock(),
    )
    rs.startup(address="localhost", port=port)  # type: ignore[no-untyped-call]

    def client() -> RestClient:
        return RestClient(f"http://localhost:{port}", retries=0)

    try:
        yield client
    finally:
        await rs.stop()  # type: ignore[no-untyped-call]


########################################################################################

POST_SCAN_BODY = {
    "cluster": {
        "foobar": 1,
    },
    "reco_algo": "anything",
    "event_i3live_json": {"a": 22},
    "nsides": {1: 2, 3: 4},
    "real_or_simulated_event": "real",
    "docker_tag": "latest",
}


async def _launch_scan(rc: RestClient, post_scan_body: dict, expected_tag: str) -> dict:
    # launch scan
    resp = await rc.request("POST", "/scan", post_scan_body)

    scanner_server_args = (
        f"python -m skymap_scanner.server "
        f"--reco-algo {post_scan_body['reco_algo']} "
        f"--cache-dir /common-space "
        f"--client-startup-json /common-space/startup.json "
        f"--nsides {' '.join(f'{k}:{v}' for k,v in post_scan_body['nsides'].items())} "
        f"--{post_scan_body['real_or_simulated_event']}-event "
        f"--predictive-scanning-threshold 1.0 "  # the default
    )

    clusters = post_scan_body["cluster"]
    if isinstance(clusters, dict):
        clusters = list(clusters.items())
    match len(clusters):
        # doing things manually here so we don't duplicate the same method used in the app
        case 1:
            tms_args = [
                f"python -m clientmanager "
                f" --collector {KNOWN_CONDOR_CLUSTERS[clusters[0][0]]['collector']} "
                f" --schedd {KNOWN_CONDOR_CLUSTERS[clusters[0][0]]['schedd']} "
                f" start "
                f" --n-workers {clusters[0][1]} "
                f" --memory 8GB "
                f" --image {skydriver.images._SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH/'skymap_scanner'}:{expected_tag} "
                f" --client-startup-json /common-space/startup.json "
                # f" --logs-directory /common-space "
            ]
        case 2:
            tms_args = [
                f"python -m clientmanager "
                f" --collector {KNOWN_CONDOR_CLUSTERS[clusters[0][0]]['collector']} "
                f" --schedd {KNOWN_CONDOR_CLUSTERS[clusters[0][0]]['schedd']} "
                f" start "
                f" --n-workers {clusters[0][1]} "
                f" --memory 8GB "
                f" --image {skydriver.images._SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH/'skymap_scanner'}:{expected_tag} "
                f" --client-startup-json /common-space/startup.json "
                # f" --logs-directory /common-space "
                ,
                f"python -m clientmanager "
                f" --collector {KNOWN_CONDOR_CLUSTERS[clusters[1][0]]['collector']} "
                f" --schedd {KNOWN_CONDOR_CLUSTERS[clusters[1][0]]['schedd']} "
                f" start "
                f" --n-workers {clusters[1][1]} "
                f" --memory 8GB "
                f" --image {skydriver.images._SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH/'skymap_scanner'}:{expected_tag} "
                f" --client-startup-json /common-space/startup.json "
                # f" --logs-directory /common-space "
            ]
        case 3:
            tms_args = [
                f"python -m clientmanager "
                f" --collector {KNOWN_CONDOR_CLUSTERS[clusters[0][0]]['collector']} "
                f" --schedd {KNOWN_CONDOR_CLUSTERS[clusters[0][0]]['schedd']} "
                f" start "
                f" --n-workers {clusters[0][1]} "
                f" --memory 8GB "
                f" --image {skydriver.images._SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH/'skymap_scanner'}:{expected_tag} "
                f" --client-startup-json /common-space/startup.json "
                # f" --logs-directory /common-space "
                ,
                f"python -m clientmanager "
                f" --collector {KNOWN_CONDOR_CLUSTERS[clusters[1][0]]['collector']} "
                f" --schedd {KNOWN_CONDOR_CLUSTERS[clusters[1][0]]['schedd']} "
                f" start "
                f" --n-workers {clusters[1][1]} "
                f" --memory 8GB "
                f" --image {skydriver.images._SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH/'skymap_scanner'}:{expected_tag} "
                f" --client-startup-json /common-space/startup.json "
                # f" --logs-directory /common-space "
                ,
                f"python -m clientmanager "
                f" --collector {KNOWN_CONDOR_CLUSTERS[clusters[2][0]]['collector']} "
                f" --schedd {KNOWN_CONDOR_CLUSTERS[clusters[2][0]]['schedd']} "
                f" start "
                f" --n-workers {clusters[2][1]} "
                f" --memory 8GB "
                f" --image {skydriver.images._SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH/'skymap_scanner'}:{expected_tag} "
                f" --client-startup-json /common-space/startup.json "
                # f" --logs-directory /common-space "
            ]
        case _:
            raise RuntimeError("need more cases")

    assert resp == dict(
        scan_id=resp["scan_id"],
        is_deleted=False,
        event_i3live_json_dict=post_scan_body["event_i3live_json"],
        event_metadata=None,
        scan_metadata=None,
        clusters=[],
        progress=None,
        scanner_server_args=resp["scanner_server_args"],  # see below
        tms_args=resp["tms_args"],  # see below
        env_vars=resp["env_vars"],  # see below
        complete=False,
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )

    # check args (avoid whitespace headaches...)
    assert resp["scanner_server_args"].split() == scanner_server_args.split()
    # fmt: off
    # order of tms args doesn't matter here
    assert sorted(a.split() for a in resp["tms_args"]) == sorted(a.split() for a in tms_args)
    # fmt: on

    # check env vars
    print(resp["env_vars"])
    assert set(resp["env_vars"].keys()) == {
        "CONDOR_TOKEN",
        "EWMS_PILOT_SUBPROC_TIMEOUT",  # set by CI runner
        "EWMS_TMS_S3_ACCESS_KEY_ID",
        "EWMS_TMS_S3_BUCKET",
        "EWMS_TMS_S3_SECRET_KEY",
        "EWMS_TMS_S3_URL",
        "SKYSCAN_BROKER_ADDRESS",
        "SKYSCAN_BROKER_AUTH",
        "SKYSCAN_SKYDRIVER_ADDRESS",
        "SKYSCAN_SKYDRIVER_AUTH",
        "SKYSCAN_SKYDRIVER_SCAN_ID",
    }
    # check env vars, more closely
    assert set(  # these have `value`s
        k
        for k, v in resp["env_vars"].items()
        if v["value"] is not None and v["value_from"] is None
    ) == {
        "EWMS_PILOT_SUBPROC_TIMEOUT",  # set by CI runner
        "EWMS_TMS_S3_BUCKET",
        "EWMS_TMS_S3_URL",
        "SKYSCAN_BROKER_ADDRESS",
        "SKYSCAN_BROKER_AUTH",
        "SKYSCAN_SKYDRIVER_ADDRESS",
        "SKYSCAN_SKYDRIVER_AUTH",
        "SKYSCAN_SKYDRIVER_SCAN_ID",
    }
    assert set(  # these have `value_from`s
        k
        for k, v in resp["env_vars"].items()
        if v["value_from"] is not None and v["value"] is None
    ) == {
        "CONDOR_TOKEN",
        "EWMS_TMS_S3_ACCESS_KEY_ID",
        "EWMS_TMS_S3_SECRET_KEY",
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
    return resp  # type: ignore[no-any-return]


async def _do_patch(
    rc: RestClient,
    scan_id: str,
    progress: StrDict | None = None,
    event_metadata: StrDict | None = None,
    scan_metadata: StrDict | None = None,
    cluster: StrDict | None = None,
    previous_clusters: list[StrDict] | None = None,
) -> StrDict:
    # do PATCH @ /scan/{scan_id}/manifest, assert response
    body = {}
    if progress:
        body["progress"] = progress
    if event_metadata:
        body["event_metadata"] = event_metadata
    if scan_metadata:
        body["scan_metadata"] = scan_metadata
    if cluster:
        body["cluster"] = cluster
        assert isinstance(previous_clusters, list)  # gotta include this one too
    assert body

    resp = await rc.request("PATCH", f"/scan/{scan_id}/manifest", body)
    assert resp == dict(
        scan_id=scan_id,
        is_deleted=False,
        event_i3live_json_dict=resp["event_i3live_json_dict"],  # not checking
        event_metadata=event_metadata if event_metadata else resp["event_metadata"],
        scan_metadata=scan_metadata if scan_metadata else resp["scan_metadata"],
        clusters=(
            previous_clusters + [cluster]  # type: ignore[operator]  # see assert ^^^^
            if cluster
            else resp["clusters"]  # not checking
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
        scanner_server_args=resp["scanner_server_args"],  # not checking
        tms_args=resp["tms_args"],  # not checking
        env_vars=resp["env_vars"],  # not checking
        complete=False,
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )
    manifest = resp  # keep around
    # query progress
    resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
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
            predictive_scanning_threshold=1.0,
            last_updated="now!",
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
    cluster = dict(
        orchestrator="condor",
        location=dict(
            collector="for-sure.a-collector.edu",
            schedd="this.schedd.edu",
        ),
        cluster_id=random.randint(1, 10000),
        n_workers=random.randint(1, 10000),
    )

    manifest = await _do_patch(
        rc,
        scan_id,
        cluster=cluster,
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
        "PUT",
        f"/scan/{scan_id}/result",
        {"skyscan_result": result, "is_final": is_final},
    )
    assert resp == {
        "scan_id": scan_id,
        "skyscan_result": result,
        "is_final": is_final,
    }
    result = resp  # keep around

    # query progress
    resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert resp == last_known_manifest

    # query result
    resp = await rc.request("GET", f"/scan/{scan_id}/result")
    assert resp == result

    # query scan
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == last_known_manifest
    assert resp["result"] == result

    return result


async def _delete_scan(
    rc: RestClient,
    event_metadata: StrDict,
    scan_id: str,
    last_known_manifest: StrDict,
    last_known_result: StrDict,
    is_final: bool,
    delete_completed_scan: bool | None,
) -> None:
    # DELETE SCAN
    body = {}
    if delete_completed_scan is not None:
        body["delete_completed_scan"] = delete_completed_scan
    resp = await rc.request("DELETE", f"/scan/{scan_id}", body)
    assert resp == {
        "manifest": {
            **resp["manifest"],
            # only checking these fields:
            "scan_id": scan_id,
            "is_deleted": True,
            "progress": last_known_manifest["progress"],
            "complete": last_known_manifest["complete"],
            # TODO: check more fields in future (hint: ctrl+F this comment)
        },
        "result": {
            "scan_id": scan_id,
            "is_final": is_final,
            "skyscan_result": last_known_result["skyscan_result"],
        },
    }
    del_resp = resp  # keep around

    #

    # SCAN: query w/ scan id (fails)
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/{scan_id}"
        ),
    ):
        await rc.request("GET", f"/scan/{scan_id}")
    # query w/ incl_del
    resp = await rc.request("GET", f"/scan/{scan_id}", {"include_deleted": True})
    assert resp == del_resp

    # MANIFEST: query w/ scan id (fails)
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/{scan_id}/manifest"
        ),
    ):
        await rc.request("GET", f"/scan/{scan_id}/manifest")
    # query w/ incl_del
    resp = await rc.request(
        "GET", f"/scan/{scan_id}/manifest", {"include_deleted": True}
    )
    assert resp == del_resp["manifest"]

    # RESULT: query w/ scan id (fails)
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"404 Client Error: Not Found for url: {rc.address}/scan/{scan_id}/result"
        ),
    ):
        await rc.request("GET", f"/scan/{scan_id}/result")
    # query w/ incl_del
    resp = await rc.request("GET", f"/scan/{scan_id}/result", {"include_deleted": True})
    assert resp == del_resp["result"]

    #

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


########################################################################################


@pytest.mark.parametrize(
    "docker_tag_input_and_expect",
    [
        ("latest", os.environ["LATEST_TAG"]),
        ("3.4.0", "3.4.0"),
        ("v3", os.environ["LATEST_TAG"]),
        ("3.1", "3.1.5"),
        ("gcd-handling-improvements-fe8ecee", "gcd-handling-improvements-fe8ecee"),
    ],
)
@pytest.mark.parametrize(
    "clusters",
    [
        {"foobar": 1},
        {"foobar": 1, "a-schedd": 999},
        [["foobar", 1], ["a-schedd", 999], ["a-schedd", 1234]],
    ],
)
async def test_00(
    clusters: list | dict,
    docker_tag_input_and_expect: tuple[str, str],
    server: Callable[[], RestClient],
) -> None:
    """Test normal scan creation and retrieval."""
    rc = server()

    #
    # LAUNCH SCAN
    #
    manifest = await _launch_scan(
        rc,
        {
            **POST_SCAN_BODY,
            "docker_tag": docker_tag_input_and_expect[0],
            "cluster": clusters,
        },
        docker_tag_input_and_expect[1],
    )
    scan_id = manifest["scan_id"]
    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

    #
    # INITIAL UPDATES
    #
    event_metadata = await _server_reply_with_event_metadata(rc, scan_id)
    manifest = await _clientmanager_reply(rc, scan_id, [])
    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

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
    manifest = await _clientmanager_reply(rc, scan_id, manifest["clusters"])
    # THEN, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    #
    # SEND RESULT(s)
    #
    assert not manifest["complete"]
    result = await _send_result(rc, scan_id, manifest, True)
    # wait as long as the server, so it'll mark as complete
    await asyncio.sleep(TEST_WAIT_BEFORE_TEARDOWN)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest["complete"]

    #
    # DELETE SCAN
    #
    await _delete_scan(rc, event_metadata, scan_id, manifest, result, True, True)


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
    for bad_val in [  # type: ignore[assignment]
        {},
        {"collector": "a"},
        {"schedd": "a"},
        {"collector": "a", "schedd": "a"},  # missing n_workers
        {"collector": "a", "schedd": "a", "n_workers": "not-a-number"},
    ]:
        print(f"[{bad_val}]")
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=rf"400 Client Error: `cluster`: \(ValueError\) .+ for url: {rc.address}/scan",
        ) as e:
            await rc.request("POST", "/scan", {**POST_SCAN_BODY, "cluster": bad_val})
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
    # # bad docker tag
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=rf"400 Client Error: `docker_tag`: \(ValueError\) .+ for url: {rc.address}/scan",
    ) as e:
        await rc.request("POST", "/scan", {**POST_SCAN_BODY, "docker_tag": "foo"})
    print(e.value)

    # OK
    manifest = await _launch_scan(rc, POST_SCAN_BODY, os.environ["LATEST_TAG"])
    scan_id = manifest["scan_id"]
    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

    #
    # INITIAL UPDATES
    #
    event_metadata = await _server_reply_with_event_metadata(rc, scan_id)
    manifest = await _clientmanager_reply(rc, scan_id, [])
    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

    # ATTEMPT OVERWRITE
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Cannot change an existing event_metadata for url: {rc.address}/scan/{scan_id}/manifest"
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
    # # empty body-arg -- this is okay, it'll silently do nothing
    # with pytest.raises(
    #     requests.exceptions.HTTPError,
    #     match=re.escape(
    #         f"422 Client Error: Attempted progress update with an empty object ({{}}) for url: {rc.address}/scan/{scan_id}/manifest"
    #     ),
    # ) as e:
    #     await rc.request("PATCH", f"/scan/{scan_id}/manifest", {"progress": {}})
    print(e.value)
    # # bad-type body-arg
    for bad_val in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=rf"400 Client Error: `progress`: \(ValueError\) missing value for field .* for url: {rc.address}/scan/{scan_id}/manifest",
        ) as e:
            await rc.request(
                "PATCH", f"/scan/{scan_id}/manifest", {"progress": bad_val}
            )
        print(e.value)

    # OK
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    # ATTEMPT OVERWRITE
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Cannot change an existing scan_metadata for url: {rc.address}/scan/{scan_id}/manifest"
        ),
    ) as e:
        await _do_patch(rc, scan_id, scan_metadata={"boo": "baz", "bot": "fox"})

    #
    # SEND RESULT
    #

    # ERROR
    # # empty body
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: `skyscan_result`: (MissingArgumentError) required argument is missing for url: {rc.address}/scan/{scan_id}/result"
        ),
    ) as e:
        await rc.request("PUT", f"/scan/{scan_id}/result", {})
    print(e.value)
    # # empty body-arg -- no error, doesn't do anything but return {}
    ret = await rc.request(
        "PUT", f"/scan/{scan_id}/result", {"skyscan_result": {}, "is_final": True}
    )
    assert ret == {}
    print(e.value)
    # # bad-type body-arg
    for bad_val in ["Done", ["a", "b", "c"]]:  # type: ignore[assignment]
        with pytest.raises(
            requests.exceptions.HTTPError,
            match=re.escape(
                f"400 Client Error: `skyscan_result`: (ValueError) type mismatch: 'dict' (value is '{type(bad_val)}') for url: {rc.address}/scan/{scan_id}/result"
            ),
        ) as e:
            await rc.request(
                "PUT",
                f"/scan/{scan_id}/result",
                {"skyscan_result": bad_val, "is_final": True},
            )
        print(e.value)

    # OK
    result = await _send_result(rc, scan_id, manifest, True)
    # wait as long as the server, so it'll mark as complete
    await asyncio.sleep(TEST_WAIT_BEFORE_TEARDOWN)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest["complete"]

    #
    # DELETE SCAN
    #

    # ERROR
    # # try to delete completed scan
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Attempted to delete a completed scan "
            f"(must use `delete_completed_scan=True`) for url: {rc.address}/scan"
        ),
    ) as e:
        await rc.request("DELETE", f"/scan/{scan_id}", {"delete_completed_scan": False})
    print(e.value)
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Attempted to delete a completed scan "
            f"(must use `delete_completed_scan=True`) for url: {rc.address}/scan"
        ),
    ) as e:
        await rc.request("DELETE", f"/scan/{scan_id}")
    print(e.value)

    # OK
    await _delete_scan(rc, event_metadata, scan_id, manifest, result, True, True)

    # also OK
    await _delete_scan(rc, event_metadata, scan_id, manifest, result, True, True)
