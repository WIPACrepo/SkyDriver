"""Integration tests for the REST server."""

import asyncio
import copy
import logging
import os
import pprint
import random
import re
import time
import uuid
from typing import Any, Callable

import humanfriendly
import pytest
import requests
from motor.motor_asyncio import AsyncIOMotorClient
from rest_tools.client import RestClient

import skydriver.images  # noqa: F401  # export

LOGGER = logging.getLogger(__name__)


# pylint: disable=redefined-outer-name


skydriver.config.config_logging()

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
    "scanner_server_env": {"MY_ENV_VAR": True},
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
    rc: RestClient,
    mongo_client: AsyncIOMotorClient,
    post_scan_body: dict,
    tms_args: list[str],
) -> dict:
    # launch scan
    launch_time = time.time()
    print(f"now: {launch_time}")
    post_resp = await rc.request(
        "POST",
        "/scan",
        {**post_scan_body, "manifest_projection": ["*"]},
    )
    pprint.pprint(post_resp)

    scanner_server_args = (
        f"python -m skymap_scanner.server "
        f"--reco-algo {post_scan_body['reco_algo']} "
        f"--cache-dir /common-space "
        f"--client-startup-json /common-space/startup.json "
        f"--nsides {' '.join(f'{k}:{v}' for k,v in post_scan_body['nsides'].items())} "
        f"--{post_scan_body['real_or_simulated_event']}-event "
        f"--predictive-scanning-threshold 1.0 "  # the default
    )

    assert post_resp == dict(
        scan_id=post_resp["scan_id"],  # see below
        is_deleted=False,
        timestamp=post_resp["timestamp"],  # see below
        event_i3live_json_dict__hash=None,  # field has been deprecated, always 'None'
        event_i3live_json_dict="use 'i3_event_id'",  # field has been deprecated
        i3_event_id=post_resp["i3_event_id"],  # see below
        event_metadata=None,
        scan_metadata=None,
        progress=None,
        scanner_server_args=post_resp["scanner_server_args"],  # see below
        ewms_task="use 'ewms_workflow_id'",
        ewms_workflow_id="pending-ewms",
        classifiers=post_scan_body["classifiers"],
        last_updated=post_resp["last_updated"],  # see below
        priority=0,
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )
    assert RE_UUID4HEX.fullmatch(post_resp["scan_id"])
    assert RE_UUID4HEX.fullmatch(post_resp["i3_event_id"])
    # check timestamps
    post_launch_ts = time.time()
    print(f"now: {post_launch_ts}")
    assert (
        launch_time
        < post_resp["timestamp"]
        < post_resp["last_updated"]
        < post_launch_ts
    )

    # query the SkyScanK8sJobs coll
    # -> since the scanner-server metadata is no longer stored in the manifest
    doc = await mongo_client["SkyDriver_DB"]["SkyScanK8sJobs"].find_one(
        {"scan_id": post_resp["scan_id"]}
    )
    pprint.pprint(doc)
    assert doc == dict(
        scan_id=post_resp["scan_id"],
        rescan_ids=[],
        #
        docker_tag=post_scan_body["docker_tag"],
        #
        # skyscan server config
        scanner_server_memory_bytes=humanfriendly.parse_size("1024M"),
        reco_algo=post_scan_body["reco_algo"],
        nsides=post_scan_body["nsides"],
        real_or_simulated_event=post_scan_body["real_or_simulated_event"],
        predictive_scanning_threshold=1.0,
        #
        classifiers=post_scan_body["classifiers"],
        #
        # cluster (condor) config
        request_clusters=post_scan_body["cluster"],
        worker_memory_bytes=humanfriendly.parse_size("8GB"),
        worker_disk_bytes=humanfriendly.parse_size("1GB"),
        max_pixel_reco_time=post_scan_body["max_pixel_reco_time"],
        max_worker_runtime=4 * 60 * 60,
        priority=0,
        debug_mode=[post_scan_body["debug_mode"]],
        #
        # misc
        skyscan_mq_client_timeout_wait_for_first_message=None,
        i3_event_id=post_resp["i3_event_id"],
        rest_address="",
        scanner_server_env_from_user=post_scan_body["scanner_server_env"],
    )

    # query the ScanRequests coll
    # TODO
    assert 0


def foo():
    # check args (avoid whitespace headaches...)
    assert resp["scanner_server_args"].split() == scanner_server_args.split()
    for got_args, exp_args in zip(resp["ewms_task"]["tms_args"], tms_args):
        print(got_args, exp_args)
        for got, exp in zip(got_args.split(), exp_args.split()):
            print(got, exp)
            if exp == CLUSTER_ID_PLACEHOLDER:
                assert RE_UUID4HEX.fullmatch(got)
            else:
                assert got == exp
        assert len(got_args.split()) == len(exp_args.split())
    assert len(resp["ewms_task"]["tms_args"]) == len(tms_args)

    # check env vars
    print(resp["ewms_task"]["env_vars"])
    assert set(resp["ewms_task"]["env_vars"].keys()) == {
        "scanner_server",
        "tms_starters",
    }

    # check env vars, more closely
    # "scanner_server"
    assert set(  # these have `value`s
        e["name"]
        for e in resp["ewms_task"]["env_vars"]["scanner_server"]
        if e["value"] is not None and e["value_from"] is None
    ) == {
        "SKYSCAN_BROKER_ADDRESS",
        "SKYSCAN_BROKER_AUTH",
        "SKYSCAN_SKYDRIVER_ADDRESS",
        "SKYSCAN_SKYDRIVER_AUTH",
        "SKYSCAN_SKYDRIVER_SCAN_ID",
        "SKYSCAN_EWMS_PILOT_LOG",
        "SKYSCAN_MQ_CLIENT_LOG",
        *post_scan_body["scanner_server_env"].keys(),  # type: ignore[attr-defined]
    }
    assert (
        set(  # these have `value_from`s
            e
            for e in resp["ewms_task"]["env_vars"]["scanner_server"]
            if e["value_from"] is not None and e["value"] is None
        )
        == set()
    )
    # "tms_starters"
    for env_dicts in resp["ewms_task"]["env_vars"]["tms_starters"]:
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
    for env_dicts in [resp["ewms_task"]["env_vars"]["scanner_server"]] + resp[
        "ewms_task"
    ]["env_vars"]["tms_starters"]:
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
    assert resp == dict(
        scan_id=scan_id,
        is_deleted=False,
        timestamp=resp["timestamp"],  # see below
        i3_event_id=resp["i3_event_id"],  # not checking
        event_i3live_json_dict=resp["event_i3live_json_dict"],  # not checking
        event_i3live_json_dict__hash=resp[
            "event_i3live_json_dict__hash"
        ],  # not checking
        event_metadata=event_metadata if event_metadata else resp["event_metadata"],
        scan_metadata=scan_metadata if scan_metadata else resp["scan_metadata"],
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
        ewms_task=dict(
            tms_args=resp["ewms_task"]["tms_args"],  # not checking
            env_vars=resp["ewms_task"]["env_vars"],  # not checking
            complete=False,
            clusters=(
                previous_clusters + [cluster]  # type: ignore[operator]  # see assert ^^^^
                if cluster
                else resp["ewms_task"]["clusters"]  # not checking
            ),
        ),
        classifiers=resp["classifiers"],  # not checking
        last_updated=resp["last_updated"],  # see below
        priority=0,
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )
    assert 0.0 < resp["timestamp"] < now < resp["last_updated"] < time.time()

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
            "ewms_task": {
                **resp["manifest"]["ewms_task"],
                # whether workforce is done
                "complete": last_known_manifest["ewms_task"]["complete"],
            },
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
    assert scan_id in [m["scan_id"] for m in resp["manifests"]]
    # ^^^ not testing that this is unique b/c the event could've been re-ran (rescan)
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
    assert scan_id in [m["scan_id"] for m in resp["manifests"]]
    # ^^^ not testing that this is unique b/c the event could've been re-ran (rescan)


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
            f" --worker-memory-bytes {humanfriendly.parse_size('8GB')} "
            f" --worker-disk-bytes {humanfriendly.parse_size('1GB')} "
            f" --image {image} "
            f" --client-startup-json /common-space/startup.json "
            f" --max-worker-runtime {4 * 60 * 60} "
            f" --priority 0 "
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
async def test_000(
    clusters: list | dict,
    docker_tag_input: str,
    docker_tag_expected: str,
    server: Callable[[], RestClient],
    known_clusters: dict,
    test_wait_before_teardown: float,
    mongo_client: AsyncIOMotorClient,
) -> None:
    """Test normal scan creation and retrieval."""
    rc = server()

    manifest = await _launch_scan(
        rc,
        mongo_client,
        {
            **POST_SCAN_BODY,
            "docker_tag": docker_tag_input,
            "cluster": clusters,
        },
        get_tms_args(clusters, docker_tag_expected, known_clusters),
    )

    await _after_scan_start_logic(
        rc,
        manifest,
        clusters,
        known_clusters,
        test_wait_before_teardown,
    )


async def _after_scan_start_logic(
    rc: RestClient,
    manifest: dict,
    clusters: list | dict,
    known_clusters: dict,
    test_wait_before_teardown: float,
):
    scan_id = manifest["scan_id"]

    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

    # TODO: at what point do we expect the backlogger to request to ewms?
    # TODO: we need to assert what ewms is sent (store in dummy ewms, and query here; or assert the call?)

    #
    # INITIAL UPDATES
    #
    event_metadata = await _server_reply_with_event_metadata(rc, scan_id)
    manifest = await _clientmanager_reply(  # TODO: remove/replace (and anywhere else)
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
            manifest["ewms_task"]["clusters"],
            known_clusters,
        )
    # THEN, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, 10)

    #
    # SEND RESULT(s)
    #
    assert not manifest["ewms_task"]["complete"]  # workforce is not done
    result = await _send_result(rc, scan_id, manifest, True)
    # wait as long as the server, so it'll mark as complete
    await asyncio.sleep(test_wait_before_teardown + 1)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest["ewms_task"]["complete"]  # workforce is done

    #
    # DELETE SCAN
    #
    await _delete_scan(rc, event_metadata, scan_id, manifest, result, True, True)


POST_SCAN_BODY_FOR_TEST_01 = dict(**POST_SCAN_BODY, cluster={"foobar": 1})


def _assert_manifests_equal_with_normalization(
    manifest_beta: dict, manifest_alpha: dict
):
    """
    Asserts that specific keys in two manifests are equal after normalization.
    Handles dynamically generated fields such as UUIDs and scan IDs.

    Args:
        manifest_beta (dict): The first manifest to compare.
        manifest_alpha (dict): The second manifest to compare.

    Raises:
        AssertionError: If any of the specified keys are not equal after normalization.
    """
    keys_to_compare = [
        "i3_event_id",
        "ewms_task",
        "priority",
        "scanner_server_args",
    ]

    def normalize_ewms_task(ewms_task: dict) -> dict:
        """
        Normalizes the `ewms_task` dictionary by redacting specific dynamic sub-keys.
        """
        normalized = copy.deepcopy(ewms_task)

        # Normalize `env_vars.scanner_server`
        for dicto in normalized["env_vars"]["scanner_server"]:
            if dicto["name"] == "SKYSCAN_SKYDRIVER_SCAN_ID":
                dicto["value"] = "<redacted>"
        # Normalize `env_vars.scanner_server`
        for listo in normalized["env_vars"]["tms_starters"]:
            for dicto in listo:
                if dicto["name"] == "SKYSCAN_SKYDRIVER_SCAN_ID":
                    dicto["value"] = "<redacted>"

        # Normalize `tms_args`
        normalized["tms_args"] = [
            re.sub(r"--uuid [a-f0-9\-]+", "--uuid <redacted>", arg)
            for arg in normalized["tms_args"]
        ]

        return normalized

    for key in keys_to_compare:
        if key == "ewms_task":
            normalized_beta = normalize_ewms_task(manifest_beta[key])
            normalized_alpha = normalize_ewms_task(manifest_alpha[key])
            assert normalized_beta == normalized_alpha, (
                f"Mismatch in key '{key}':\n"
                f"Beta: {normalized_beta}\n"
                f"Alpha: {normalized_alpha}"
            )
        else:
            assert manifest_beta[key] == manifest_alpha[key], (
                f"Mismatch in key '{key}':\n"
                f"Beta: {manifest_beta.get(key)}\n"
                f"Alpha: {manifest_alpha.get(key)}"
            )

    assert manifest_beta["timestamp"] > manifest_alpha["timestamp"]


async def test_010__rescan(
    server: Callable[[], RestClient],
    known_clusters: dict,
    test_wait_before_teardown: float,
) -> None:
    rc = server()

    clusters = {"foobar": 1, "a-schedd": 999, "cloud": 4568}

    # OG SCAN
    manifest_alpha = await _launch_scan(
        rc,
        {
            **POST_SCAN_BODY,
            "docker_tag": "3.4.0",
            "cluster": clusters,
        },
        get_tms_args(clusters, "3.4.0", known_clusters),
    )
    await _after_scan_start_logic(
        rc,
        manifest_alpha,
        clusters,
        known_clusters,
        test_wait_before_teardown,
    )

    # RESCAN
    manifest_beta = await rc.request(
        "POST",
        f"/scan/{manifest_alpha['scan_id']}/actions/rescan",
    )
    # compare manifests
    assert manifest_beta["classifiers"] == {
        **manifest_alpha["classifiers"],
        **{"rescan": True, "origin_scan_id": manifest_alpha["scan_id"]},
    }
    _assert_manifests_equal_with_normalization(manifest_beta, manifest_alpha)
    # continue on...
    await _after_scan_start_logic(
        rc,
        manifest_beta,
        clusters,
        known_clusters,
        test_wait_before_teardown,
    )


########################################################################################


async def test_100__bad_data(
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
        match=re.escape(
            f"400 Client Error: the following arguments are required: "
            f"docker_tag, cluster, reco_algo, event_i3live_json, nsides, "
            f"real_or_simulated_event, max_pixel_reco_time "
            f"for url: {rc.address}/scan"
        ),
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
                match=rf"400 Client Error: argument {arg}: .+ for url: {rc.address}/scan",
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
            match=rf"400 Client Error: argument cluster: .+ for url: {rc.address}/scan",
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
                match=re.escape(
                    f"400 Client Error: the following arguments are required: {arg} "
                    f"for url: {rc.address}/scan"
                ),
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
        match=rf"400 Client Error: argument docker_tag: invalid type for url: {rc.address}/scan",
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
            match=rf"400 Client Error: argument progress: missing value for field .* for url: {rc.address}/scan/{scan_id}/manifest",
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
            f"400 Client Error: the following arguments are required: "
            f"skyscan_result, is_final "
            f"for url: {rc.address}/scan/{scan_id}/result"
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
                f"400 Client Error: argument skyscan_result: arg must be a dict "
                f"for url: {rc.address}/scan/{scan_id}/result"
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
    assert manifest["ewms_task"]["complete"]  # workforce is done

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
