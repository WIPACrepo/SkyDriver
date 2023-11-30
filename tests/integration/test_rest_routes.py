"""Integration tests for the REST server."""

# pylint: disable=redefined-outer-name

import asyncio
import hashlib
import json
import os
import random
import re
import time
import uuid
from typing import Any, Callable

import pytest
import requests
import skydriver
import skydriver.images  # noqa: F401  # export
from rest_tools.client import RestClient

skydriver.config.config_logging("debug")

StrDict = dict[str, Any]

########################################################################################


RE_UUID4HEX = re.compile(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}")


IS_REAL_EVENT = True  # for simplicity, hardcode for all requests

CLUSTER_ID_PLACEHOLDER = "CLUSTER_ID_PLACEHOLDER"

CLASSIFIERS = {
    "foo": 1,
    "bar bat": True,
    "baz": "y" * skydriver.rest_handlers.MAX_CLASSIFIERS_LEN,  # type: ignore[attr-defined]
    "z" * skydriver.rest_handlers.MAX_CLASSIFIERS_LEN: 3.1415,  # type: ignore[attr-defined]
}
POST_SCAN_BODY = {
    "reco_algo": "anything",
    "event_i3live_json": {"a": 22},
    "nsides": {1: 2, 3: 4},
    "real_or_simulated_event": "real",
    "docker_tag": "latest",
    "classifiers": CLASSIFIERS,
    "max_pixel_reco_time": 60,
    "debug_mode": "client-logs",
}
REQUIRED_FIELDS = [
    "reco_algo",
    "event_i3live_json",
    "nsides",
    "real_or_simulated_event",
    "docker_tag",
    "max_pixel_reco_time",
]


########################################################################################


async def _launch_scan(
    rc: RestClient, post_scan_body: dict, tms_args: list[str]
) -> dict:
    # launch scan
    launch_time = time.time()
    resp = await rc.request(
        "POST",
        "/scan",
        {**post_scan_body, "manifest_projection": ["*"]},
    )

    scanner_server_args = (
        f"python -m skymap_scanner.server "
        f"--reco-algo {post_scan_body['reco_algo']} "
        f"--cache-dir /common-space "
        f"--client-startup-json /common-space/startup.json "
        f"--nsides {' '.join(f'{k}:{v}' for k,v in post_scan_body['nsides'].items())} "
        f"--{post_scan_body['real_or_simulated_event']}-event "
        f"--predictive-scanning-threshold 1.0 "  # the default
    )

    assert resp == dict(
        scan_id=resp["scan_id"],
        is_deleted=False,
        timestamp=resp["timestamp"],  # see below
        event_i3live_json_dict__hash=hashlib.md5(
            json.dumps(
                post_scan_body["event_i3live_json"],
                sort_keys=True,
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest(),
        event_i3live_json_dict=post_scan_body["event_i3live_json"],
        event_metadata=None,
        scan_metadata=None,
        clusters=[],
        progress=None,
        scanner_server_args=resp["scanner_server_args"],  # see below
        tms_args=resp["tms_args"],  # see below
        env_vars=resp["env_vars"],  # see below
        complete=False,
        classifiers=post_scan_body["classifiers"],
        last_updated=resp["last_updated"],  # see below
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )
    assert RE_UUID4HEX.fullmatch(resp["scan_id"])
    assert launch_time < resp["timestamp"] < resp["last_updated"] < time.time()

    # check args (avoid whitespace headaches...)
    assert resp["scanner_server_args"].split() == scanner_server_args.split()
    for got_args, exp_args in zip(resp["tms_args"], tms_args):
        print(got_args, exp_args)
        for got, exp in zip(got_args.split(), exp_args.split()):
            print(got, exp)
            if exp == CLUSTER_ID_PLACEHOLDER:
                assert RE_UUID4HEX.fullmatch(got)
            else:
                assert got == exp
        assert len(got_args.split()) == len(exp_args.split())
    assert len(resp["tms_args"]) == len(tms_args)

    # check env vars
    print(resp["env_vars"])
    assert set(resp["env_vars"].keys()) == {"scanner_server", "tms_starters"}

    # check env vars, more closely
    # "scanner_server"
    assert set(  # these have `value`s
        e["name"]
        for e in resp["env_vars"]["scanner_server"]
        if e["value"] is not None and e["value_from"] is None
    ) == {
        "SKYSCAN_BROKER_ADDRESS",
        "SKYSCAN_BROKER_AUTH",
        "SKYSCAN_SKYDRIVER_ADDRESS",
        "SKYSCAN_SKYDRIVER_AUTH",
        "SKYSCAN_SKYDRIVER_SCAN_ID",
        "SKYSCAN_EWMS_PILOT_LOG",
        "SKYSCAN_MQ_CLIENT_LOG",
    }
    assert (
        set(  # these have `value_from`s
            e
            for e in resp["env_vars"]["scanner_server"]
            if e["value_from"] is not None and e["value"] is None
        )
        == set()
    )
    # "tms_starters"
    for env_dicts in resp["env_vars"]["tms_starters"]:
        assert set(  # these have `value`s
            e["name"]
            for e in env_dicts
            if e["value"] is not None and e["value_from"] is None
        ) == {
            "EWMS_PILOT_TASK_TIMEOUT",  # set by CI runner
            "EWMS_TMS_S3_BUCKET",
            "EWMS_TMS_S3_URL",
            "SKYSCAN_BROKER_ADDRESS",
            "SKYSCAN_BROKER_AUTH",
            "SKYSCAN_SKYDRIVER_ADDRESS",
            "SKYSCAN_SKYDRIVER_AUTH",
            "SKYSCAN_SKYDRIVER_SCAN_ID",
            "SKYSCAN_EWMS_PILOT_LOG",
            "SKYSCAN_MQ_CLIENT_LOG",
            "WORKER_K8S_LOCAL_APPLICATION_NAME",
            "EWMS_PILOT_DUMP_TASK_OUTPUT",
        }
        assert (
            next(
                x["value"]
                for x in env_dicts
                if x["name"] == "EWMS_PILOT_DUMP_TASK_OUTPUT"
            )
            == "True"
        )
        assert set(  # these have `value_from`s
            e["name"]
            for e in env_dicts
            if e["value_from"] is not None and e["value"] is None
        ) == {
            "CONDOR_TOKEN",
            "EWMS_TMS_S3_ACCESS_KEY_ID",
            "EWMS_TMS_S3_SECRET_KEY",
        } or set(  # these have `value_from`s
            e["name"]
            for e in env_dicts
            if e["value_from"] is not None and e["value"] is None
        ) == {
            "WORKER_K8S_CONFIG_FILE_BASE64",
            "EWMS_TMS_S3_ACCESS_KEY_ID",
            "EWMS_TMS_S3_SECRET_KEY",
        }

    # check env vars, even MORE closely
    for env_dicts in [resp["env_vars"]["scanner_server"]] + resp["env_vars"][
        "tms_starters"
    ]:
        assert (
            next(x["value"] for x in env_dicts if x["name"] == "SKYSCAN_BROKER_ADDRESS")
            == "localhost"
        )
        assert re.match(
            r"http://localhost:[0-9]+",
            next(
                x["value"]
                for x in env_dicts
                if x["name"] == "SKYSCAN_SKYDRIVER_ADDRESS"
            ),
        )
        assert (
            len(
                next(
                    x["value"]
                    for x in env_dicts
                    if x["name"] == "SKYSCAN_SKYDRIVER_SCAN_ID"
                )
            )
            == 32
        )

    # get scan_id
    assert resp["scan_id"]

    # remove fields usually not returned
    assert resp.pop("event_i3live_json_dict")  # remove to match with other requests
    assert resp.pop("env_vars")  # remove to match with other requests
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

    now = time.time()

    resp = await rc.request("PATCH", f"/scan/{scan_id}/manifest", body)
    assert resp.pop("event_i3live_json_dict")  # remove to match with other requests
    assert resp.pop("env_vars")  # remove to match with other requests
    assert resp == dict(
        scan_id=scan_id,
        is_deleted=False,
        timestamp=resp["timestamp"],  # see below
        event_i3live_json_dict__hash=resp[
            "event_i3live_json_dict__hash"
        ],  # not checking
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
        complete=False,
        classifiers=CLASSIFIERS,
        last_updated=resp["last_updated"],  # see below
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )
    assert 0.0 < resp["timestamp"] < now < resp["last_updated"] < time.time()

    manifest = resp  # keep around
    # query progress
    resp = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert resp.pop("event_i3live_json_dict")  # remove to match with other requests
    assert resp.pop("env_vars")  # remove to match with other requests
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

    # query by run+event id
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": run_id,
                "event_metadata.event_id": event_id,
                "event_metadata.is_real_event": IS_REAL_EVENT,
            }
        },
    )
    assert [m["scan_id"] for m in resp["manifests"]] == [scan_id]
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": run_id,
                "event_metadata.event_id": event_id,
                "event_metadata.is_real_event": IS_REAL_EVENT,
            },
            "include_deleted": False,
        },
    )
    assert [m["scan_id"] for m in resp["manifests"]] == [scan_id]
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": run_id,
                "event_metadata.event_id": event_id,
                "event_metadata.is_real_event": IS_REAL_EVENT,
                "is_deleted": False,
            },
        },
    )
    assert [m["scan_id"] for m in resp["manifests"]] == [scan_id]

    return event_metadata


async def _clientmanager_reply(
    rc: RestClient,
    scan_id: str,
    cluster_name__n_workers: tuple[str, int],
    previous_clusters: list[StrDict],
    known_clusters: dict,
) -> StrDict:
    # reply as the clientmanager with a new cluster
    cluster = dict(
        orchestrator=known_clusters[cluster_name__n_workers[0]]["orchestrator"],
        location=known_clusters[cluster_name__n_workers[0]]["location"],
        cluster_id=f"cluster-{random.randint(1, 10000)}",
        n_workers=cluster_name__n_workers[1],
        starter_info={},
        statuses={},
        top_task_errors={},
        uuid=str(uuid.uuid4().hex),
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
    assert resp.pop("event_i3live_json_dict")  # remove to match with other requests
    assert resp.pop("env_vars")  # remove to match with other requests
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

    now = time.time()

    resp = await rc.request("DELETE", f"/scan/{scan_id}", body)
    assert resp == {
        "manifest": {
            **resp["manifest"],
            # only checking these fields:
            "scan_id": scan_id,
            "is_deleted": True,
            "progress": last_known_manifest["progress"],
            "complete": last_known_manifest["complete"],  # whether workforce is done
            "last_updated": resp["manifest"]["last_updated"],  # see below
            # TODO: check more fields in future (hint: ctrl+F this comment)
        },
        "result": {
            "scan_id": scan_id,
            "is_final": is_final,
            "skyscan_result": last_known_result["skyscan_result"],
        },
    }
    assert (
        0.0
        < resp["manifest"]["timestamp"]
        < now
        < resp["manifest"]["last_updated"]
        < time.time()
    )
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
    assert resp.pop("event_i3live_json_dict")  # remove to match with other requests
    assert resp.pop("env_vars")  # remove to match with other requests
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

    # query by run+event id (none)
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": event_metadata["run_id"],
                "event_metadata.event_id": event_metadata["event_id"],
                "event_metadata.is_real_event": IS_REAL_EVENT,
            },
        },
    )
    assert not resp["manifests"]  # no matches
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": event_metadata["run_id"],
                "event_metadata.event_id": event_metadata["event_id"],
                "event_metadata.is_real_event": IS_REAL_EVENT,
            },
            "include_deleted": False,
        },
    )
    assert not resp["manifests"]  # no matches
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": event_metadata["run_id"],
                "event_metadata.event_id": event_metadata["event_id"],
                "event_metadata.is_real_event": IS_REAL_EVENT,
                "is_deleted": False,
            },
        },
    )
    assert not resp["manifests"]  # no matches

    # query by run+event id w/ incl_del
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": event_metadata["run_id"],
                "event_metadata.event_id": event_metadata["event_id"],
                "event_metadata.is_real_event": IS_REAL_EVENT,
            },
            "include_deleted": True,
        },
    )
    assert [m["scan_id"] for m in resp["manifests"]] == [scan_id]
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {
                "event_metadata.run_id": event_metadata["run_id"],
                "event_metadata.event_id": event_metadata["event_id"],
                "event_metadata.is_real_event": IS_REAL_EVENT,
                "is_deleted": True,
            },
        },
    )
    assert [m["scan_id"] for m in resp["manifests"]] == [scan_id]


def get_tms_args(
    clusters: list | dict,
    docker_tag_expected: str,
    known_clusters: dict,
) -> list[str]:
    tms_args = []
    for cluster in clusters if isinstance(clusters, list) else list(clusters.items()):
        orchestrator = known_clusters[cluster[0]]["orchestrator"]
        location = known_clusters[cluster[0]]["location"]
        image = (
            f"/cvmfs/icecube.opensciencegrid.org/containers/realtime/skymap_scanner:{docker_tag_expected}"
            if orchestrator == "condor"
            else f"icecube/skymap_scanner:{docker_tag_expected}"
        )
        tms_args += [
            f"python -m clientmanager "
            f" --uuid {CLUSTER_ID_PLACEHOLDER} "
            f" {orchestrator} "
            f" {' '.join(f'--{k} {v}' for k,v in location.items())} "
            f" start "
            f" --n-workers {cluster[1]} "
            f" --memory 8GB "
            f" --image {image} "
            f" --client-startup-json /common-space/startup.json "
            f" --max-worker-runtime {4 * 60 * 60} "
            f" --spool "
        ]

    return tms_args


########################################################################################


@pytest.mark.parametrize(
    "docker_tag_input,docker_tag_expected",
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
        {"foobar": 1, "a-schedd": 999, "cloud": 4568},
        [
            ["foobar", 1],
            ["a-schedd", 999],
            ["cloud", 5845],
            ["a-schedd", 1234],
            ["cloud", 6548],
        ],
    ],
)
async def test_00(
    clusters: list | dict,
    docker_tag_input: str,
    docker_tag_expected: str,
    server: Callable[[], RestClient],
    known_clusters: dict,
    test_wait_before_teardown: float,
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
            "docker_tag": docker_tag_input,
            "cluster": clusters,
        },
        get_tms_args(clusters, docker_tag_expected, known_clusters),
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
    manifest = await _clientmanager_reply(
        rc,
        scan_id,
        clusters[0] if isinstance(clusters, list) else list(clusters.items())[0],
        [],
        known_clusters,
    )
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
    # NEXT, spin up more workers in clusters
    for cluster_name__n_workers in (
        clusters[1:] if isinstance(clusters, list) else list(clusters.items())[1:]
    ):
        manifest = await _clientmanager_reply(
            rc,
            scan_id,
            cluster_name__n_workers,
            manifest["clusters"],
            known_clusters,
        )
    # THEN, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    #
    # SEND RESULT(s)
    #
    assert not manifest["complete"]  # workforce is not done
    result = await _send_result(rc, scan_id, manifest, True)
    # wait as long as the server, so it'll mark as complete
    await asyncio.sleep(test_wait_before_teardown + 1)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest.pop("event_i3live_json_dict")  # remove to match with other requests
    assert manifest.pop("env_vars")  # remove to match with other requests
    assert manifest["complete"]  # workforce is done

    #
    # DELETE SCAN
    #
    await _delete_scan(rc, event_metadata, scan_id, manifest, result, True, True)


POST_SCAN_BODY_FOR_TEST_01 = dict(**POST_SCAN_BODY, cluster={"foobar": 1})


async def test_01__bad_data(
    server: Callable[[], RestClient],
    known_clusters: dict,
    test_wait_before_teardown: float,
) -> None:
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
    for arg in POST_SCAN_BODY_FOR_TEST_01:
        for bad_val in [
            "",
            "  ",
            "\t",
            "string" if not isinstance(POST_SCAN_BODY_FOR_TEST_01[arg], str) else None,
        ]:
            print(f"{arg}: [{bad_val}]")
            with pytest.raises(
                requests.exceptions.HTTPError,
                match=rf"400 Client Error: `{arg}`: \(ValueError\) .+ for url: {rc.address}/scan",
            ) as e:
                await rc.request(
                    "POST", "/scan", {**POST_SCAN_BODY_FOR_TEST_01, arg: bad_val}
                )
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
            await rc.request(
                "POST", "/scan", {**POST_SCAN_BODY_FOR_TEST_01, "cluster": bad_val}
            )
    print(e.value)
    # # missing arg
    for arg in POST_SCAN_BODY_FOR_TEST_01:
        if arg in REQUIRED_FIELDS:
            print(arg)
            with pytest.raises(
                requests.exceptions.HTTPError,
                match=rf"400 Client Error: `{arg}`: \(MissingArgumentError\) required argument is missing for url: {rc.address}/scan",
            ) as e:
                # remove arg from body
                await rc.request(
                    "POST",
                    "/scan",
                    {k: v for k, v in POST_SCAN_BODY_FOR_TEST_01.items() if k != arg},
                )
        print(e.value)
    # # bad docker tag
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=rf"400 Client Error: `docker_tag`: \(ValueError\) .+ for url: {rc.address}/scan",
    ) as e:
        await rc.request(
            "POST", "/scan", {**POST_SCAN_BODY_FOR_TEST_01, "docker_tag": "foo"}
        )
    print(e.value)

    # OK
    manifest = await _launch_scan(
        rc,
        POST_SCAN_BODY_FOR_TEST_01,
        get_tms_args(
            POST_SCAN_BODY_FOR_TEST_01["cluster"],  # type: ignore[arg-type]
            os.environ["LATEST_TAG"],
            known_clusters,
        ),
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
    manifest = await _clientmanager_reply(
        rc,
        scan_id,
        ("foobar", random.randint(1, 10000)),
        [],
        known_clusters,
    )
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
    await asyncio.sleep(test_wait_before_teardown)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest.pop("event_i3live_json_dict")  # remove to match with other requests
    assert manifest.pop("env_vars")  # remove to match with other requests
    assert manifest["complete"]  # workforce is done

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
