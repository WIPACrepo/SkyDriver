"""Tools for interfacing with EMWS."""

from rest_tools.client import RestClient

from . import database


async def request_workflow_on_ewms(
    ewms_rc: RestClient,
    manifest: database.schema.Manifest,
) -> str:
    """Request a workflow in EWMS."""
    if not isinstance(manifest.ewms_task, database.schema.EWMSRequestInfo):
        raise TypeError("Manifest is not designated for EWMS")

    body = {
        "public_queue_aliases": ["to-client-queue", "from-client-queue"],
        "tasks": [
            {
                "cluster_locations": manifest.ewms_task.cluster_locations,
                "input_queue_aliases": ["to-client-queue"],
                "output_queue_aliases": ["from-client-queue"],
                "task_image": "/cvmfs/icecube.opensciencegrid.org/containers/realtime/skymap_scanner:$SKYSCAN_TAG",
                "task_args": "python -m skymap_scanner.client --infile {{INFILE}} --outfile {{OUTFILE}} --client-startup-json {{DATA_HUB}}/startup.json",
                "init_image": "/cvmfs/icecube.opensciencegrid.org/containers/realtime/skymap_scanner:$SKYSCAN_TAG",
                "init_args": "bash -c \"curl --fail-with-body --max-time 60 -o {{DATA_HUB}}/startup.json '$S3_OBJECT_URL'\" ",
                "n_workers": manifest.ewms_task.n_workers,
                "pilot_config": {
                    "tag": "latest",
                    "environment": {
                        "EWMS_PILOT_INIT_TIMEOUT": 1 * 60,
                        "EWMS_PILOT_TASK_TIMEOUT": 1 * 60 * 60,
                        "EWMS_PILOT_TIMEOUT_QUEUE_WAIT_FOR_FIRST_MESSAGE": 10 * 60,
                        "EWMS_PILOT_TIMEOUT_QUEUE_INCOMING": 5 * 60,
                        "EWMS_PILOT_CONTAINER_DEBUG": "True",
                        "EWMS_PILOT_INFILE_EXT": ".json",
                        "EWMS_PILOT_OUTFILE_EXT": ".json",
                    },
                    "input_files": [],
                },
                "worker_config": {
                    "do_transfer_worker_stdouterr": True,
                    "max_worker_runtime": 2 * 60 * 60,
                    "n_cores": 1,
                    "priority": manifest.priority,
                    "worker_disk": "512M",
                    "worker_memory": "8G",
                    "condor_requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx && has_avx2",
                },
            }
        ],
    }

    resp = await ewms_rc.request("POST", "/v0/workflows", body)
    return resp["workflow"]["workflow_id"]
