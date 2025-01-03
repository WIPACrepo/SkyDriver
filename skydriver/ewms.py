"""Tools for interfacing with EMWS."""

from rest_tools.client import RestClient

from . import database, images


async def request_workflow_on_ewms(
    ewms_rc: RestClient,
    manifest: database.schema.Manifest,
    s3_obj_url: str,
    scan_request_obj: dict,
) -> str:
    """Request a workflow in EWMS."""
    if isinstance(manifest.ewms_task, database.schema.InHouseStarterInfo):
        raise TypeError("Manifest is not designated for EWMS")

    image = images.get_skyscan_docker_image(scan_request_obj["docker_tag"])

    body = {
        "public_queue_aliases": ["to-client-queue", "from-client-queue"],
        "tasks": [
            {
                "cluster_locations": [
                    cname for cname, _ in scan_request_obj["request_clusters"]
                ],
                "input_queue_aliases": ["to-client-queue"],
                "output_queue_aliases": ["from-client-queue"],
                "task_image": image,
                "task_args": (
                    "python -m skymap_scanner.client "
                    "--infile {{INFILE}} --outfile {{OUTFILE}} "
                    "--client-startup-json {{DATA_HUB}}/startup.json"
                ),
                "init_image": image,  # piggyback this image since it's already present
                "init_args": (
                    "bash -c "
                    '"'  # quote for bash -c "..."
                    "curl --fail-with-body --max-time 60 -o {{DATA_HUB}}/startup.json "
                    f"'{s3_obj_url}'"  # single-quote the url
                    '"'  # unquote for bash -c "..."
                ),
                "n_workers": scan_request_obj["request_clusters"][0][1],
                # TODO: ^^^ pass on varying # of workers per cluster
                "pilot_config": {
                    "tag": "latest",
                    "environment": {
                        "EWMS_PILOT_INIT_TIMEOUT": 61,  # 1 sec more than 'curl' timeout
                        "EWMS_PILOT_TASK_TIMEOUT": scan_request_obj[
                            "max_pixel_reco_time"
                        ],
                        "EWMS_PILOT_TIMEOUT_QUEUE_WAIT_FOR_FIRST_MESSAGE": scan_request_obj[
                            "skyscan_mq_client_timeout_wait_for_first_message"
                        ],
                        "EWMS_PILOT_TIMEOUT_QUEUE_INCOMING": 5 * 60,
                        "EWMS_PILOT_CONTAINER_DEBUG": "True",  # toggle?
                        "EWMS_PILOT_INFILE_EXT": ".json",
                        "EWMS_PILOT_OUTFILE_EXT": ".json",
                    },
                    "input_files": [],
                },
                "worker_config": {
                    "do_transfer_worker_stdouterr": True,  # toggle?
                    "max_worker_runtime": 6 * 60 * 60,  # 6 hours
                    "n_cores": 1,
                    "priority": scan_request_obj["priority"],
                    "worker_disk": scan_request_obj["worker_disk_bytes"],
                    "worker_memory": scan_request_obj["worker_memory_bytes"],
                    "condor_requirements": "HAS_CVMFS_icecube_opensciencegrid_org && has_avx && has_avx2",
                },
            }
        ],
    }

    resp = await ewms_rc.request("POST", "/v0/workflows", body)
    return resp["workflow"]["workflow_id"]
