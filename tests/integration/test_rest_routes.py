"""Integration tests for the REST server."""

import asyncio
import logging
import os
import pprint
import re
import time
from typing import Any, Callable

import humanfriendly
import pytest
import requests
from motor.motor_asyncio import AsyncIOMotorClient
from rest_tools.client import RestClient

import skydriver.images  # noqa: F401  # export
from skydriver.__main__ import setup_ewms_client

NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS = "not-yet-requested"

LOGGER = logging.getLogger(__name__)

skydriver.config.config_logging()

sdict = dict[str, Any]

########################################################################################
# CONSTANTS
########################################################################################

RE_SCANID = re.compile(r"[0-9a-f]{11}x[0-9a-f]{20}")  # see make_scan_id()
RE_UUID4HEX = re.compile(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}")  # normal uuid4

_EWMS_URL_V_PREFIX = "v1"

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
# UTILS
########################################################################################


async def _launch_scan(
    rc: RestClient,
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    post_scan_body: dict,
    docker_tag_expected: str,
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
        ewms_workflow_id=NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
        ewms_address=None,
        classifiers=post_scan_body["classifiers"],
        last_updated=post_resp["last_updated"],  # see below
        priority=0,
        # TODO: check more fields in future (hint: ctrl+F this comment)
    )
    assert RE_SCANID.fullmatch(post_resp["scan_id"])
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

    # check database
    rest_address = await _assert_db_scanrequests_coll(
        mongo_client,
        post_scan_body,
        post_resp,
        docker_tag_expected,
    )
    await _assert_db_skyscank8sjobs_coll(
        mongo_client,
        post_scan_body,
        post_resp,
        scanner_server_args,
        rest_address,
        docker_tag_expected,
    )

    return post_resp  # type: ignore[no-any-return]


async def _assert_db_scanrequests_coll(
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    post_scan_body: dict,
    post_resp: dict,
    docker_tag_expected: str,
) -> str:
    """Query the ScanRequests coll.

    Return the REST address
    """
    doc_sr = await mongo_client["SkyDriver_DB"]["ScanRequests"].find_one(  # type: ignore[index]
        {"scan_id": post_resp["scan_id"]}, {"_id": 0}
    )
    pprint.pprint(doc_sr)
    assert doc_sr == dict(
        scan_id=post_resp["scan_id"],
        rescan_ids=[],
        #
        docker_tag=docker_tag_expected,
        #
        # skyscan server config
        scanner_server_memory_bytes=humanfriendly.parse_size("1024M"),
        reco_algo=post_scan_body["reco_algo"],
        nsides={str(k): v for k, v in post_scan_body["nsides"].items()},
        real_or_simulated_event=post_scan_body["real_or_simulated_event"],
        predictive_scanning_threshold=1.0,
        #
        classifiers=post_scan_body["classifiers"],
        #
        # cluster (condor) config
        request_clusters=(
            list([k, v] for k, v in post_scan_body["cluster"].items())
            if isinstance(post_scan_body["cluster"], dict)
            else post_scan_body["cluster"]
        ),
        worker_memory_bytes=humanfriendly.parse_size("8GB"),
        worker_disk_bytes=humanfriendly.parse_size("1GB"),
        max_pixel_reco_time=post_scan_body["max_pixel_reco_time"],
        priority=0,
        debug_mode=[post_scan_body["debug_mode"]],
        #
        # misc
        i3_event_id=post_resp["i3_event_id"],
        rest_address=doc_sr["rest_address"],  # see below
        scanner_server_env_from_user=post_scan_body["scanner_server_env"],
    )
    assert re.fullmatch(rf"{re.escape('http://localhost:')}\d+", doc_sr["rest_address"])

    return doc_sr["rest_address"]


async def _assert_db_skyscank8sjobs_coll(  # noqa: MFL000
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
    post_scan_body: dict,
    post_resp: dict,
    scanner_server_args: str,
    rest_address: str,
    docker_tag_expected: str,
):
    # query the SkyScanK8sJobs coll
    # -> since the scanner-server metadata is no longer stored in the manifest
    doc_k8s = await mongo_client["SkyDriver_DB"]["SkyScanK8sJobs"].find_one(  # type: ignore[index]
        {"scan_id": post_resp["scan_id"]}, {"_id": 0}
    )
    pprint.pprint(doc_k8s)
    assert doc_k8s == {
        "scan_id": post_resp["scan_id"],
        "skyscan_k8s_job_dict": {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "annotations": {"argocd.argoproj.io/sync-options": "Prune=false"},
                "labels": {"app.kubernetes.io/instance": None},
                "name": f"skyscan-{post_resp['scan_id']}",
                "namespace": None,
            },
            "spec": {
                "activeDeadlineSeconds": 172800,
                "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": {"app": "scanner-instance"}},
                    "spec": {
                        "containers": [
                            {
                                "args": scanner_server_args.split(),
                                "command": [],
                                "env": [
                                    {
                                        "name": "SKYSCAN_EWMS_JSON",
                                        "value": "/common-space/ewms.json",
                                    },
                                    {
                                        "name": "SKYSCAN_SKYDRIVER_ADDRESS",
                                        "value": rest_address,
                                    },
                                    {
                                        "name": "SKYSCAN_SKYDRIVER_SCAN_ID",
                                        "value": post_resp["scan_id"],
                                    },
                                    {
                                        "name": "SKYSCAN_EWMS_PILOT_LOG",
                                        "value": "WARNING",
                                    },
                                    {
                                        "name": "SKYSCAN_MQ_CLIENT_LOG",
                                        "value": "WARNING",
                                    },
                                    {"name": "SKYSCAN_SKYDRIVER_AUTH", "value": ""},
                                ]
                                + [
                                    {"name": k, "value": str(v)}
                                    for k, v in post_scan_body[
                                        "scanner_server_env"
                                    ].items()
                                ],
                                "image": f"icecube/skymap_scanner:{docker_tag_expected}",
                                "name": f'skyscan-server-{post_resp["scan_id"]}',
                                "resources": {
                                    "limits": {"cpu": "1.0", "memory": "1024000000"},
                                    "requests": {
                                        "cpu": "0.1",
                                        "ephemeral-storage": "8G",
                                        "memory": "1024000000",
                                    },
                                },
                                "volumeMounts": [
                                    {
                                        "mountPath": "/common-space",
                                        "name": "common-space-volume",
                                    }
                                ],
                            },
                            {
                                "args": [
                                    "/common-space/startup.json",
                                    "--wait-indefinitely",
                                ],
                                "command": ["python", "-m", "s3_sidecar"],
                                "env": [
                                    {
                                        "name": "K8S_SCANNER_SIDECAR_S3_LIFETIME_SECONDS",
                                        "value": str(900),
                                    },
                                    {"name": "S3_URL", "value": os.environ["S3_URL"]},
                                    {
                                        "name": "S3_ACCESS_KEY_ID",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "key": os.environ[
                                                    "S3_ACCESS_KEY_ID__K8S_SECRET_KEY"
                                                ],
                                                "name": os.environ["K8S_SECRET_NAME"],
                                            }
                                        },
                                    },
                                    {
                                        "name": "S3_SECRET_KEY",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "key": os.environ[
                                                    "S3_SECRET_KEY__K8S_SECRET_KEY"
                                                ],
                                                "name": os.environ["K8S_SECRET_NAME"],
                                            }
                                        },
                                    },
                                    {"name": "S3_EXPIRES_IN", "value": str(604800)},
                                    {
                                        "name": "S3_BUCKET",
                                        "value": os.environ["S3_BUCKET"],
                                    },
                                    {
                                        "name": "S3_OBJECT_KEY",
                                        "value": f"{post_resp['scan_id']}-s3-object",
                                    },
                                ],
                                "image": os.environ["THIS_IMAGE_WITH_TAG"],
                                "name": f"sidecar-s3-{post_resp['scan_id']}",
                                "resources": {
                                    "limits": {"cpu": "0.1", "memory": "100M"},
                                    "requests": {
                                        "cpu": "0.05",
                                        "ephemeral-storage": "1M",
                                        "memory": "10M",
                                    },
                                },
                                "restartPolicy": "OnFailure",
                                "volumeMounts": [
                                    {
                                        "mountPath": "/common-space",
                                        "name": "common-space-volume",
                                    }
                                ],
                            },
                        ],
                        "initContainers": [
                            {
                                "args": [
                                    post_resp["scan_id"],
                                    "--json-out",
                                    "/common-space/ewms.json",
                                ],
                                "command": ["python", "-m", "ewms_init_container"],
                                "env": [
                                    {
                                        "name": "SKYSCAN_SKYDRIVER_ADDRESS",
                                        "value": rest_address,
                                    },
                                    {"name": "SKYSCAN_SKYDRIVER_AUTH", "value": ""},
                                    {
                                        "name": "EWMS_ADDRESS",
                                        "value": os.environ["EWMS_ADDRESS"],
                                    },
                                    {
                                        "name": "EWMS_TOKEN_URL",
                                        "value": os.environ["EWMS_TOKEN_URL"],
                                    },
                                    {
                                        "name": "EWMS_CLIENT_ID",
                                        "value": os.environ["EWMS_CLIENT_ID"],
                                    },
                                    {
                                        "name": "EWMS_CLIENT_SECRET",
                                        "value": os.environ["EWMS_CLIENT_SECRET"],
                                    },
                                    {
                                        "name": "EWMS_CLUSTERS",
                                        "value": " ".join(
                                            list(post_scan_body["cluster"].keys())
                                            if isinstance(
                                                post_scan_body["cluster"], dict
                                            )
                                            else [
                                                c[0] for c in post_scan_body["cluster"]
                                            ]
                                        ),
                                    },
                                    {"name": "EWMS_N_WORKERS", "value": "1"},
                                    {
                                        "name": "EWMS_TASK_IMAGE",
                                        "value": f"/cvmfs/icecube.opensciencegrid.org/containers/realtime/skymap_scanner:{docker_tag_expected}",
                                    },
                                    {
                                        "name": "EWMS_PILOT_TASK_TIMEOUT",
                                        "value": str(
                                            post_scan_body["max_pixel_reco_time"]
                                        ),
                                    },
                                    {
                                        "name": "EWMS_PILOT_QUARANTINE_TIME",
                                        "value": str(
                                            post_scan_body["max_pixel_reco_time"]
                                        ),
                                    },
                                    {
                                        "name": "EWMS_PILOT_DUMP_TASK_OUTPUT",
                                        "value": str(
                                            bool(
                                                "client-logs"
                                                in post_scan_body["debug_mode"]
                                            )
                                        ),
                                    },
                                    {
                                        "name": "EWMS_WORKER_MAX_WORKER_RUNTIME",
                                        "value": str(24 * 60 * 60),
                                    },
                                    {"name": "EWMS_WORKER_PRIORITY", "value": "0"},
                                    {
                                        "name": "EWMS_WORKER_DISK_BYTES",
                                        "value": "1000000000",
                                    },
                                    {
                                        "name": "EWMS_WORKER_MEMORY_BYTES",
                                        "value": "8000000000",
                                    },
                                    {"name": "S3_URL", "value": os.environ["S3_URL"]},
                                    {
                                        "name": "S3_ACCESS_KEY_ID",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "key": os.environ[
                                                    "S3_ACCESS_KEY_ID__K8S_SECRET_KEY"
                                                ],
                                                "name": os.environ["K8S_SECRET_NAME"],
                                            }
                                        },
                                    },
                                    {
                                        "name": "S3_SECRET_KEY",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "key": os.environ[
                                                    "S3_SECRET_KEY__K8S_SECRET_KEY"
                                                ],
                                                "name": os.environ["K8S_SECRET_NAME"],
                                            }
                                        },
                                    },
                                    {"name": "S3_EXPIRES_IN", "value": str(604800)},
                                    {
                                        "name": "S3_BUCKET",
                                        "value": os.environ["S3_BUCKET"],
                                    },
                                    {
                                        "name": "S3_OBJECT_KEY",
                                        "value": f"{post_resp['scan_id']}-s3-object",
                                    },
                                ],
                                "image": os.environ["THIS_IMAGE_WITH_TAG"],
                                "name": f"init-ewms-{post_resp['scan_id']}",
                                "resources": {
                                    "limits": {"cpu": "0.1", "memory": "100M"},
                                    "requests": {
                                        "cpu": "0.05",
                                        "ephemeral-storage": "1M",
                                        "memory": "10M",
                                    },
                                },
                                "volumeMounts": [
                                    {
                                        "mountPath": "/common-space",
                                        "name": "common-space-volume",
                                    }
                                ],
                            }
                        ],
                        "restartPolicy": "Never",
                        "serviceAccountName": None,
                        "volumes": [{"emptyDir": {}, "name": "common-space-volume"}],
                    },
                },
                "ttlSecondsAfterFinished": 600,
            },
        },
    }


async def _do_patch(
    rc: RestClient,
    scan_id: str,
    manifest: sdict,
    progress: sdict | None = None,
    event_metadata: sdict | None = None,
    scan_metadata: sdict | None = None,
) -> sdict:
    # do PATCH @ /scan/{scan_id}/manifest, assert response
    body = {}
    if progress:
        body["progress"] = progress
    if event_metadata:
        body["event_metadata"] = event_metadata
    if scan_metadata:
        body["scan_metadata"] = scan_metadata
    assert body

    now = time.time()

    resp = await rc.request("PATCH", f"/scan/{scan_id}/manifest", body)
    assert resp == dict(
        scan_id=scan_id,
        is_deleted=False,
        timestamp=resp["timestamp"],  # see below
        i3_event_id=manifest["i3_event_id"],  # should not change
        event_i3live_json_dict=manifest["event_i3live_json_dict"],  # should not change
        event_i3live_json_dict__hash=manifest[
            "event_i3live_json_dict__hash"
        ],  # should not change
        event_metadata=(
            event_metadata if event_metadata else manifest["event_metadata"]
        ),
        scan_metadata=(scan_metadata if scan_metadata else manifest["scan_metadata"]),
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
            else manifest["progress"]  # should not change
        ),
        scanner_server_args=manifest["scanner_server_args"],  # should not change
        ewms_task="use 'ewms_workflow_id'",
        ewms_workflow_id=manifest["ewms_workflow_id"],  # should not change
        ewms_address=manifest["ewms_address"],  # should not change
        classifiers=manifest["classifiers"],  # should not change
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
    manifest: sdict,
    n: int,
) -> sdict:
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
            start=123,
            end=456,
        )
        # update progress (update `scan_metadata` sometimes--not as important)
        if i % 2:  # odd
            manifest = await _do_patch(
                rc,
                scan_id,
                manifest,
                progress=progress,
            )
        else:  # even
            manifest = await _do_patch(
                rc,
                scan_id,
                manifest,
                progress=progress,
                scan_metadata={"scan_id": scan_id, "foo": "bar"},
            )
    return manifest


async def _server_reply_with_event_metadata(
    rc: RestClient,
    scan_id: str,
    manifest: sdict,
) -> sdict:
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

    manifest = await _do_patch(rc, scan_id, manifest, event_metadata=event_metadata)

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

    return manifest


async def _send_result(
    rc: RestClient,
    scan_id: str,
    manifest: sdict,
    is_final: bool,
) -> sdict:
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
    assert resp == manifest

    # query result
    resp = await rc.request("GET", f"/scan/{scan_id}/result")
    assert resp == result

    # query scan
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == result

    return result


async def _delete_scan(
    rc: RestClient,
    event_metadata: sdict,
    scan_id: str,
    manifest: sdict,
    last_known_result: sdict,
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
            "progress": manifest["progress"],
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


async def _is_scan_complete(rc: RestClient, scan_id: str) -> bool:
    resp = await rc.request("GET", f"/scan/{scan_id}/status")
    pprint.pprint(resp)
    return resp["scan_complete"]


########################################################################################
# TESTS
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
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
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
        docker_tag_expected,
    )

    await _after_scan_start_logic(
        rc,
        manifest,
        test_wait_before_teardown,
    )


async def _after_scan_start_logic(
    rc: RestClient,
    manifest: sdict,
    test_wait_before_teardown: float,
):
    scan_id = manifest["scan_id"]

    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

    # wait backlogger to request to ewms
    assert int(os.environ["SCAN_BACKLOG_RUNNER_DELAY"])
    await asyncio.sleep(int(os.environ["SCAN_BACKLOG_RUNNER_DELAY"]) * 5)  # extra

    # mimic the ewms-init container...
    # -> before
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest["ewms_workflow_id"] == NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS
    assert manifest["ewms_address"] is None
    assert (await rc.request("GET", f"/scan/{scan_id}/ewms/workflow-id")) == {
        "workflow_id": NOT_YET_SENT_WORKFLOW_REQUEST_TO_EWMS,
        "requested_ewms_workflow": False,
        "eligible_for_ewms": True,
        "ewms_address": None,
    }
    # -> update workflow_id
    resp = await setup_ewms_client().request(
        "POST", f"/{_EWMS_URL_V_PREFIX}/workflows", {"foo": "bar"}
    )
    workflow_id = resp["workflow"]["workflow_id"]
    await rc.request(
        "POST",
        f"/scan/{scan_id}/ewms/workflow-id",
        {"workflow_id": workflow_id, "ewms_address": "ewms.foo.aq"},
    )
    # -> after
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert manifest["ewms_workflow_id"] == workflow_id
    assert manifest["ewms_address"] == "ewms.foo.aq"
    assert (await rc.request("GET", f"/scan/{scan_id}/ewms/workflow-id")) == {
        "workflow_id": workflow_id,
        "requested_ewms_workflow": True,
        "eligible_for_ewms": True,
        "ewms_address": "ewms.foo.aq",
    }

    #
    # INITIAL UPDATES
    #
    manifest = await _server_reply_with_event_metadata(rc, scan_id, manifest)
    # follow-up query
    assert await rc.request("GET", f"/scan/{scan_id}/result") == {}
    resp = await rc.request("GET", f"/scan/{scan_id}")
    assert resp["manifest"] == manifest
    assert resp["result"] == {}

    #
    # ADD PROGRESS
    #
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, manifest, 10)

    #
    # SEND INTERMEDIATES (these can happen in any order, or even async)
    #
    # FIRST, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, manifest, 10)
    # THEN, clients send updates
    result = await _send_result(rc, scan_id, manifest, False)
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, manifest, 10)

    #
    # SEND RESULT(s)
    #
    assert not await _is_scan_complete(rc, manifest["scan_id"])  # workforce is not done
    result = await _send_result(rc, scan_id, manifest, True)
    # wait as long as the server, so it'll mark as complete
    await asyncio.sleep(test_wait_before_teardown + 1)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert await _is_scan_complete(rc, manifest["scan_id"])  # workforce is done

    #
    # DELETE SCAN
    #
    await _delete_scan(
        rc,
        manifest["event_metadata"],
        scan_id,
        manifest,
        result,
        True,
        True,
    )


POST_SCAN_BODY_FOR_TEST_01 = dict(**POST_SCAN_BODY, cluster={"foobar": 1})


async def test_010__rescan(
    server: Callable[[], RestClient],
    known_clusters: dict,
    test_wait_before_teardown: float,
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
) -> None:
    rc = server()

    clusters = {"foobar": 1, "a-schedd": 999, "cloud": 4568}

    # OG SCAN
    manifest_alpha = await _launch_scan(
        rc,
        mongo_client,
        {
            **POST_SCAN_BODY,
            "docker_tag": "3.4.0",
            "cluster": clusters,
        },
        "3.4.0",
    )
    await _after_scan_start_logic(
        rc,
        manifest_alpha,
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
    skip_keys = ["classifiers", "scan_id", "last_updated", "timestamp"]
    assert {k: v for k, v in manifest_beta.items() if k not in skip_keys} == {
        k: v for k, v in manifest_alpha.items() if k not in skip_keys
    }
    for sk in skip_keys:
        assert manifest_beta[sk] != manifest_alpha[sk]
    # continue on...
    await _after_scan_start_logic(
        rc,
        manifest_beta,
        test_wait_before_teardown,
    )


########################################################################################


async def test_100__bad_data(
    server: Callable[[], RestClient],
    known_clusters: dict,
    test_wait_before_teardown: float,
    mongo_client: AsyncIOMotorClient,  # type: ignore[valid-type]
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
        match=rf"400 Client Error: argument docker_tag: Image tag not on Docker Hub for url: {rc.address}/scan",
    ) as e:
        await rc.request(
            "POST", "/scan", {**POST_SCAN_BODY_FOR_TEST_01, "docker_tag": "foo"}
        )
    print(e.value)

    # OK
    manifest = await _launch_scan(
        rc,
        mongo_client,
        POST_SCAN_BODY_FOR_TEST_01,
        os.environ["LATEST_TAG"],
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
    manifest = await _server_reply_with_event_metadata(rc, scan_id, manifest)
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
            manifest,
            event_metadata=dict(
                run_id=manifest["event_metadata"]["run_id"],
                event_id=manifest["event_metadata"]["event_id"],
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
    manifest = await _patch_progress_and_scan_metadata(rc, scan_id, manifest, 10)

    # ATTEMPT OVERWRITE
    with pytest.raises(
        requests.exceptions.HTTPError,
        match=re.escape(
            f"400 Client Error: Cannot change an existing scan_metadata for url: {rc.address}/scan/{scan_id}/manifest"
        ),
    ) as e:
        await _do_patch(
            rc, scan_id, manifest, scan_metadata={"boo": "baz", "bot": "fox"}
        )

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
    await asyncio.sleep(test_wait_before_teardown + 1)
    manifest = await rc.request("GET", f"/scan/{scan_id}/manifest")
    assert await _is_scan_complete(rc, manifest["scan_id"])  # workforce is done

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
    await _delete_scan(
        rc,
        manifest["event_metadata"],
        scan_id,
        manifest,
        result,
        True,
        True,
    )

    # also OK
    await _delete_scan(
        rc,
        manifest["event_metadata"],
        scan_id,
        manifest,
        result,
        True,
        True,
    )
