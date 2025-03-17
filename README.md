<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/SkyDriver?include_prereleases)](https://github.com/WIPACrepo/SkyDriver/) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen)
<!--- End of README Badges (automated) --->

# SkyDriver v1

A SaaS Solution for Neutrino Event Reconstruction using the Skymap Scanner

## Overview

SkyDriver automates the entire scanning of an event: starting all servers and workers, transferring all needed data, and finally, all tear-down. SkyDriver also includes a database for storing scan requests, progress reports, and results. The computational engine for a scan is the [Skymap Scanner](https://github.com/icecube/skymap_scanner). The main interface is a REST server with several [routes and methods](#rest-api).

One of many workflows may be:

1. Request a scan ([POST @ `/scan`](#scan---post))
1. Monitor the scanning status ([GET @ `/scan/SCAN_ID/status`](#scanscan_idstatus---get))
2. Check for progress updates ([GET @ `/scan/SCAN_ID/manifest`](#scanscan_idmanifest---get))
3. Check for partial results ([GET @ `/scan/SCAN_ID/result`](#scanscan_idresult---get))
4. Get a final result ([GET @ `/scan/SCAN_ID/result`](#scanscan_idresult---get))
5. [Make plots](#making-plots-with-a-scans-result-using-the-scan_id)

Another workflow:

1. Find a scan id for a particular run and event ([GET @ `/scans/find`](#scansfind---post))
2. Get the scan's manifest and result ([GET @ `/scan/SCAN_ID`](#scanscan_id---get))

&nbsp;

## Getting Started

Users interface with SkyDriver via REST calls, so first, you will need to get a connection. This example uses [wipac-rest-tools](https://pypi.org/project/wipac-rest-tools/):

```python
from rest_tools.client import RestClient, SavedDeviceGrantAuth


def get_rest_client() -> RestClient:
    """Get REST client for talking to SkyDriver.

    This will present a QR code in the terminal for initial validation.
    """

    # NOTE: If your script will not be interactive (like a cron job),
    # then you need to first run your script manually to validate using
    # the QR code in the terminal.

    return SavedDeviceGrantAuth(
        "https://skydriver.icecube.aq",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        filename="device-refresh-token",
        client_id="skydriver-external",
        retries=0,
    )


rc = get_rest_client()
```

Now, you can make all the REST calls needed:

```python
rc.request_seq(method, path, args_dict)
```

### Two Quick Examples

To request a new scan (see [POST @ `/scan`](#scan---post)):

```python
manifest = rc.request_seq("POST", "/scan", {"docker_tag": ...})
print(json.dumps(manifest))
```

To see your scan's status (see [GET @ `/scan/SCAN_ID/status`](#scanscan_idstatus---get)):

```python
status = rc.request_seq("GET", f"/scan/{scan_id}/status")
print(json.dumps(status))
```

Refer to the [REST API](#rest-api) section for comprehensive documentation detailing the available interactions with SkyDriver.

Also, see [Using a Scan Result Outside of SkyDriver](#using-a-scan-result-outside-of-skydriver).

&nbsp;

## REST API

Documentation for the public-facing routes and method

&nbsp;
### `/scan` - POST
-------------------------------------------------------------------------------
_Launch a new scan of an event_

#### Arguments

| Argument                                             | Type                                                            | Required/Default               | Description                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------------|-----------------------------------------------------------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `"docker_tag"`                                       | str                                                             | *[REQUIRED]*                   | the docker tag of the Skymap Scanner image (must be in CVMFS). Ex: `v3.1.4`, `v3.5`, `v3`, `latest`, `eqscan-6207146` (branch-based tag)                                                                                                                                                                             
| `"cluster"`                                          | dict or list                                                    | *[REQUIRED]*                   | the worker cluster(s) to use *along with the number of workers for each:* Example: `{"sub-2": 1234}`. NOTE: To request a schedd more than once, provide a list of 2-lists instead (Ex: `[ ["sub-2", 56], ["sub-2", 1234] ]`)                                                                                         
| `"reco_algo"`                                        | bool                                                            | *[REQUIRED]*                   | which reco algorithm to use (see [Skymap Scanner](https://github.com/icecube/skymap_scanner/tree/main/skymap_scanner/recos))                                                                                                                                                                                         
| `"event_i3live_json"`                                | dict or str                                                     | *[REQUIRED]*                   | Realtime's JSON event format                                                                                                                                                                                                                                                                                         
| `"nsides"`                                           | dict                                                            | *[REQUIRED]*                   | the nside progression to use (see [Skymap Scanner](https://github.com/icecube/skymap_scanner))                                                                                                                                                                                                                       
| `"real_or_simulated_event"`                          | str                                                             | *[REQUIRED]*                   | whether this event is real or simulated. Ex: `real`, `simulated`                                                                                                                                                                                                                                                     
| `"max_pixel_reco_time"`                              | int                                                             | *[REQUIRED]*                   | the max amount of time (seconds) each pixel's reco should take (accurate values will evict pixels from slow workers thereby re-delivering to faster workers -- slow workers are unavoidable due to non-deterministic errors)                                                                                         
| `"max_worker_runtime"`                               | int                                                             | default: `24*60*60`            | the max amount of time (second) each client worker can work for (larger values are needed as the event size increases AND the workforce size decreases)                                                                                                                                                              
| `"skyscan_mq_client_timeout_wait_for_first_message"` | int                                                             | default: image's default value | how long a client can wait for its first message (pixel) before giving up and exiting                                                                                                                                                                                                                                
| `"scanner_server_memory"`                            | str                                                             | default: `1024M`               | how much memory for the scanner server to request                                                                                                                                                                                                                                                                    
| `"worker_memory"`                                    | str                                                             | default: `8G`                  | how much memory per client worker to request                                                                                                                                                                                                                                                                         
| `"worker_disk"`                                      | str                                                             | default: `1G`                  | how much disk per client worker to request                                                                                                                                                                                                                                                                           
| `"debug_mode"`                                       | str or list                                                     | default: None                  | what debug mode(s) to use: `"client-logs"` collects the scanner clients' stderr/stdout including icetray logs (scans are limited in # of workers)                                                                                                                                                                    
| `"predictive_scanning_threshold"`                    | float                                                           | default: `1.0`                 | the predictive scanning threshold `[0.1, 1.0]` (see [Skymap Scanner](https://github.com/icecube/skymap_scanner))                                                                                                                                                                                                     
| `"priority"`                                         | int                                                             | default: `0`                   | the relative priority of this scan -- higher values indicate higher priority. **NOTE: Values `>= 100` are reserved for Realtime alert scans (these scan requests are not throttled).** Also, see [HTCondor jobs](https://htcondor.readthedocs.io/en/latest/users-manual/priorities-and-preemption.html#job-priority) 
| `"classifiers"`                                      | <code>dict[str, str &#124; bool &#124; float &#124; int]</code> | default: `{}`                  | a user-defined collection of labels, attributes, etc. -- this is constrained in size and is intended for user-defined metadata only                                                                                                                                                                                  
| `"manifest_projection"`                              | list                                                            | default: all fields            | which `Manifest` fields to include in the response (include `*` to include all fields)                                                                                                                                                                                                                               

#### SkyDriver Effects

- Creates and starts a new Skymap Scanner instance spread across many client workers
- The new scanner will send updates routinely and when the scan completes (see [GET (manifest)](#scanscan_idmanifest-get) and [GET (result)](#scanscan_idresult-get))

#### Returns

dict - [Manifest](#manifest)

&nbsp;
### `/scan/SCAN_ID/manifest` - GET
-------------------------------------------------------------------------------
_Retrieve the manifest of a scan_

#### Arguments

| Argument            | Type                    | Required/Default | Description                                                                            |
|---------------------|-------------------------|------------------|----------------------------------------------------------------------------------------|
| `"include_deleted"` | bool                    | default: `False` | *Not normally needed* -- `True` prevents a 404 error if the scan was deleted (aborted) 
 <!--                | `"manifest_projection"` | list             | default: all fields                                                                    | which `Manifest` fields to include in the response (include `*` to include all fields) -->

#### SkyDriver Effects

None

#### Returns

dict - [Manifest](#manifest)

&nbsp;
### `/scan/SCAN_ID/result` - GET
-------------------------------------------------------------------------------
_Retrieve the result of a scan_

#### Arguments

| Argument            | Type | Required/Default | Description                                                                            |
|---------------------|------|------------------|----------------------------------------------------------------------------------------|
| `"include_deleted"` | bool | default: `False` | *Not normally needed* -- `True` prevents a 404 error if the scan was deleted (aborted) 

#### SkyDriver Effects

None

#### Returns

dict - [Result](#result)

&nbsp;
### `/scan/SCAN_ID` - GET
-------------------------------------------------------------------------------
_Retrieve the manifest and result of a scan_

#### Arguments

| Argument            | Type                    | Required/Default | Description                                                                            |
|---------------------|-------------------------|------------------|----------------------------------------------------------------------------------------|
| `"include_deleted"` | bool                    | default: `False` | *Not normally needed* -- `True` prevents a 404 error if the scan was deleted (aborted) 
 <!--                | `"manifest_projection"` | list             | default: all fields                                                                    | which `Manifest` fields to include in the response (include `*` to include all fields) -->

#### SkyDriver Effects

None

#### Returns

```
{
    "manifest": Manifest dict,
    "result": Result dict,
}
```

- See [Manifest](#manifest)
- See [Result](#result)

&nbsp;
### `/scan/SCAN_ID` - DELETE
-------------------------------------------------------------------------------
_Abort a scan and/or mark scan (manifest and result) as "deleted"_

#### Arguments

| Argument                  | Type | Required/Default    | Description                                                                                        |
|---------------------------|------|---------------------|----------------------------------------------------------------------------------------------------|
| `"delete_completed_scan"` | bool | default: `False`    | whether to mark a completed scan as "deleted" -- *this is not needed for aborting an ongoing scan* 
| `"manifest_projection"`   | list | default: all fields | which `Manifest` fields to include in the response (include `*` to include all fields)             

#### SkyDriver Effects

- The Skymap Scanner instance is stopped and removed
- The scan's manifest and result are marked as "deleted" in the database

#### Returns

```
{
    "manifest": Manifest dict,
    "result": Result dict,
}
```

- See [Manifest](#manifest)
- See [Result](#result)

&nbsp;
### `/scans/find` - POST
-------------------------------------------------------------------------------
_Retrieve scan manifests corresponding to a specific search query_

#### Arguments

| Argument                | Type | Required/Default                                                                                    | Description                                                                                                                                         |
|-------------------------|------|-----------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `"filter"`              | dict | *[REQUIRED]*                                                                                        | a MongoDB-syntax filter for `Manifest`                                                                                                              
| `"include_deleted"`     | bool | default: `False`                                                                                    | whether to include deleted scans (overwritten by `filter`'s `is_deleted`)                                                                           
| `"manifest_projection"` | list | default: all fields except: `"ewms_task", "event_i3live_json_dict", "event_i3live_json_dict__hash"` | which `Manifest` fields to include in the response (`"ewms_task", "event_i3live_json_dict", "event_i3live_json_dict__hash"` will never be included) 

##### Example

One simple `"filter"` may be:

```
{
    "filter": {
       "event_metadata.run_id": 123456789,
       "event_metadata.event_id": 987654321,
       "event_metadata.is_real_event": True,
    }
}
```

See https://www.mongodb.com/docs/manual/tutorial/query-documents/ for more complex queries.

#### SkyDriver Effects

None

#### Returns

```
{
    "manifests": list[Manifest dict],
}
```

- See [Manifest](#manifest)

&nbsp;
### `/scans/backlog` - GET
-------------------------------------------------------------------------------
_Retrieve entire backlog list_

#### Arguments

None

#### SkyDriver Effects

None

#### Returns

```
{
    "entries": [
        {
            "scan_id": str,
            "timestamp": float,
            "pending_timestamp": float
        },
        ...
    ]
}
```

&nbsp;
### `/scan/SCAN_ID/status` - GET
-------------------------------------------------------------------------------
_Retrieve the status of a scan_

#### Arguments

| Argument                 | Type | Required/Default | Description                                                                                                    |
|--------------------------|------|------------------|----------------------------------------------------------------------------------------------------------------|
| `"include_pod_statuses"` | bool | `False`          | whether to include the k8s pod statuses for the clientmanager & central server -- expends additional resources 

#### SkyDriver Effects

None

#### Returns

```
{
    "scan_state": str,  # a short human-readable code
    "is_deleted": bool,
    "scan_complete": bool,  # workforce is done
    "pods": {  # field is included only if `include_pod_statuses == True`
        "pod_status": dict,  # a large k8s status object
        "pod_status_message": str,  # a human-readable message explaining the pod status retrieval
    }
    "clusters": list,  # same as Manifest's clusters field
}
```

##### Scan State Codes

There are several codes for `scan_state`:

- Successful state
    * `SCAN_FINISHED_SUCCESSFULLY`
- Non-finished scan states (in reverse order of occurrence)
    * `IN_PROGRESS__PARTIAL_RESULT_GENERATED`
    * `IN_PROGRESS__WAITING_ON_FIRST_PIXEL_RECO`
    * `PENDING__WAITING_ON_CLUSTER_STARTUP` or `PENDING__WAITING_ON_SCANNER_SERVER_STARTUP`
    * `PENDING__PRESTARTUP`
- The above non-finished states have equivalents in the case that the scan failed and/or aborted
    * `STOPPED__PARTIAL_RESULT_GENERATED`
    * `STOPPED__WAITING_ON_FIRST_PIXEL_RECO`
    * `STOPPED__WAITING_ON_CLUSTER_STARTUP` or `STOPPED__WAITING_ON_SCANNER_SERVER_STARTUP`
    * `STOPPED__PRESTARTUP`
    * *NOTE: a failed scan my not have an above code automatically, and may need a `DELETE` request to get the code. Until then, it will retain an non-finished state code.*

&nbsp;
### `/scan/SCAN_ID/logs` - GET
-------------------------------------------------------------------------------
_Retrieve the logs of a scan's pod: central server & client starter(s)_

#### Arguments

None

#### SkyDriver Effects

None

#### Returns

```
{
    "scanner_server": {
        "url": str,  # a url to a web dashboard for viewing dashboards
    }
}
```

&nbsp;
### Return Types
-------------------------------------------------------------------------------

#### Manifest

_A dictionary containing non-physics metadata on a scan_

Pseudo-code:

```
{
    scan_id: str,

    timestamp: float,
    is_deleted: bool,

    event_i3live_json_dict: dict,  # the i3 event or an internal id 
    scanner_server_args: str,

    priority: int,

    classifiers: dict[str, str | bool | float | int]

    event_i3live_json_dict__hash: str | None,  # deprecated

    ewms_task: {
        tms_args: list[str],
        env_vars: dict[str, dict[str, Any]],
        clusters: [  # 2 types: condor & k8s -- different 'location' sub-fields
            {
                orchestrator: 'condor',
                location: {
                    collector: str,
                    schedd: str,
                },
                cluster_id: int,
                n_workers: int,
                starter_info: dict,
                statuses: {
                    'Completed': {  # condor job status
                        'FatalError': int,  # pilot status value -> # of jobs
                        'Done': int,  # pilot status value -> # of jobs
                        ...
                    },
                    'Running': {
                        'Tasking': int,
                        ...
                    }
                    ...
                },
                top_task_errors: dict[str, int],  # error message -> # of jobs
            },
            ...
            {
                orchestrator: 'k8s',
                location: {
                    host: str,
                    namespace: str,
                },
                cluster_id: int,
                n_workers: int,
                starter_info: dict,
            },
            ...
        ],
        # signifies scanner is done (server and worker cluster(s))
        complete: bool,
    },

    # found/created during first few seconds of scanning
    event_metadata: {
        run_id: int,
        event_id: int,
        event_type: str,
        mjd: float,
        is_real_event: bool,  # as opposed to simulation
    },
    scan_metadata: dict | None,

    # updated during scanning, multiple times (initially will be 'None')
    progress: {
        summary: str,
        epilogue: str,
        tallies: dict,
        processing_stats: {
            start: dict,
            runtime: dict,
            rate: dict,
            end: str,
            finished: bool,
            predictions: dict,
        },
        predictive_scanning_threshold: float,
        last_updated: str,
    },

    # timestamp of any update to manifest -- also see `progress.last_updated`
    last_updated: float,
}
```

- See [skydriver/database/schema.py](https://github.com/WIPACrepo/SkyDriver/blob/main/skydriver/database/schema.py)

##### Manifest Fields Excluded by Default in Response

Some routes/methods respond with the scan's manifest. This is a large dictionary, so by default, all but [GET @ `/scan/SCAN_ID/manifest`](#scanscan_idmanifest---get) exclude these fields:

- `event_i3live_json_dict`

See https://github.com/search?q=repo%3AWIPACrepo%2FSkyDriver+DEFAULT_EXCLUDED_MANIFEST_FIELDS&type=code

#### Result

_A dictionary containing the scan result_

Pseudo-code:

```
{
    scan_id: str,

    skyscan_result: dict,  # serialized version of 'skyreader.SkyScanResult'
    is_final: bool,  # is this result the final result?
}
```

- See [skydriver/database/schema.py](https://github.com/WIPACrepo/SkyDriver/blob/main/skydriver/database/schema.py)
- See [skyreader's SkyScanResult](https://github.com/icecube/skyreader/)

&nbsp;

## Using a Scan Result Outside of SkyDriver

### Making Plots with a Scan's Result (using the `scan_id`)

See skyreader's [plot_skydriver_scan_result.py](https://github.com/icecube/skyreader/blob/main/examples/plot_skydriver_scan_result.py)

### Creating a `SkyScanResult` Instance from a Scan's Result (using the `scan_id`)

Also, see skyreader's [plot_skydriver_scan_result.py](https://github.com/icecube/skyreader/blob/main/examples/plot_skydriver_scan_result.py)
