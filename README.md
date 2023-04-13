<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/SkyDriver?include_prereleases)](https://github.com/WIPACrepo/SkyDriver/) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen) 
<!--- End of README Badges (automated) --->
# SkyDriver
A SaaS Solution for Neutrino Event Reconstruction using the Skymap Scanner

## Overview
SkyDriver automates the entire scanning of an event: starting all servers and workers, transferring all needed data, and finally, all tear-down. SkyDriver also includes a database for storing scan requests, progress reports, and results. The computational engine for a scan is the [Skymap Scanner](https://github.com/icecube/skymap_scanner). The main interface is a REST server with several [routes and methods](#rest-api).

A standard workflow may be:
1. Request a scan ([POST @ `/scan`](#scan-post))
2. Check for progress updates ([GET @ `/scan/manifest/SCAN_ID`](#scanmanifestscan_id-get))
3. Check for partial results ([GET @ `/scan/result/SCAN_ID`](#scanresultscan_id-get))
4. Get a final result ([GET @ `/scan/result/SCAN_ID`](#scanresultscan_id-get))
5. [Make plots](#making-plots-with-a-scans-result-using-the-scan_id)


&nbsp;
## REST API
Documentation for the public-facing routes and methods

&nbsp;
### `/scans` - GET
-------------------------------------------------------------------------------
_Retrieve scan ids corresponding to a specific run & event_

#### Arguments
| Argument            | Type        | Required/Default | Description          |
| ------------------- | ----------- | ---------------- | -------------------- |
| `"run_id"`          | int         | *[REQUIRED]*     | id of the run
| `"event_id"`        | int         | *[REQUIRED]*     | id of the event
| `"is_real_event"`   | bool        | *[REQUIRED]*     | whether this event is real or simulated
| `"include_deleted"` | bool        | default: `False` | whether to include deleted scans

#### SkyDriver Effects
None

#### Returns
```
{
    "event_id": event_id,
    "scan_ids": scan_ids,  # list of strings
}
```


&nbsp;
### `/scan` - POST
-------------------------------------------------------------------------------
_Launch a new scan of an event_

#### Arguments
| Argument                          | Type         | Required/Default | Description          |
| --------------------------------- | ------------ | ---------------- | -------------------- |
| `"docker_tag"`                    | str          | *[REQUIRED]*     | the docker tag of the Skymap Scanner image (must be in CVMFS). Ex: `v3.1.4`, `v3.5`, `v3`, `latest`, `eqscan-6207146` (branch-based tag)
| `"cluster"`                       | dict or list | *[REQUIRED]*     | the HTCondor cluster(s) to use along with the number of jobs for each: Example: `{"sub-2": 1234}`. NOTE: To request a schedd more than once, provide a list of 2-lists instead (Ex: `[ ["sub-2", 56], ["sub-2", 1234] ]`)
| `"reco_algo"`                     | bool         | *[REQUIRED]*     | which reco algorithm to use (see [Skymap Scanner](https://github.com/icecube/skymap_scanner/tree/main/skymap_scanner/recos))
| `"event_i3live_json"`             | dict or str  | *[REQUIRED]*     | Realtime's JSON event format
| `"nsides"`                        | dict         | *[REQUIRED]*     | the nside progression to use (see [Skymap Scanner](https://github.com/icecube/skymap_scanner))
| `"real_or_simulated_event"`       | str          | *[REQUIRED]*     | whether this event is real or simulated. Ex: `real`, `simulated`
| `"memory"`                        | str          | default: `8GB`   | how much memory per condor worker to request
| `"predictive_scanning_threshold"` | float        | default: `1.0`   | the predictive scanning threshold [0.1, 1.0] (see [Skymap Scanner](https://github.com/icecube/skymap_scanner))
| `"max_reco_time"`                 | int          | default: `None`  | the max amount of time each reco should take

#### SkyDriver Effects
- Creates and starts a new Skymap Scanner instance spread across many HTCondor workers
- The new scanner will send updates routinely and when the scan completes (see [GET (manifest)](#scanmanifestscan_id-get) and [GET (result)](#scanresultscan_id-get))

#### Returns
dict - [Manifest](#manifest)


&nbsp;
### `/scan/manifest/SCAN_ID` - GET
-------------------------------------------------------------------------------
_Retrieve the manifest of a scan_

#### Arguments
| Argument            | Type        | Required/Default | Description          |
| ------------------- | ----------- | ---------------- | -------------------- |
| `"include_deleted"` | bool        | default: `False` | whether to include deleted scans

#### SkyDriver Effects
None

#### Returns
dict - [Manifest](#manifest)


&nbsp;
### `/scan/manifest/SCAN_ID` - DELETE
-------------------------------------------------------------------------------
_Abort ongoing scan_

#### Arguments
None

#### SkyDriver Effects
- The Skymap Scanner instance is stopped and removed
- The scan's manifest is marked as "deleted" in the database

#### Returns
dict - [Manifest](#manifest)


&nbsp;
### `/scan/result/SCAN_ID` - GET
-------------------------------------------------------------------------------
_Retrieve the result of a scan_

#### Arguments
| Argument            | Type        | Required/Default | Description          |
| ------------------- | ----------- | ---------------- | -------------------- |
| `"include_deleted"` | bool        | default: `False` | whether to include deleted scans

#### SkyDriver Effects
Stops the Skymap Scanner instance if it is still running

#### Returns
dict - [Result](#result)


&nbsp;
### `/scan/result/SCAN_ID` - DELETE
-------------------------------------------------------------------------------
_Delete the result of a scan_

#### Arguments
None

#### SkyDriver Effects
The result is marked as "deleted" in the database

#### Returns
dict - [Result](#result)


&nbsp;
### Return Types
-------------------------------------------------------------------------------
#### Manifest
_A dictionary containing non-physics metadata on a scan_

Pseudo-code:
```
{
    event_i3live_json_dict: dict,
    scanner_server_args: str,
    tms_args: list[str],
    env_vars: dict[str, dict],

    condor_clusters: [
        {
            collector: str,
            schedd: str,
            cluster_id: int,
            jobs: int,
        },
        ...
    ],

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

    # signifies k8s jobs and condor cluster(s) are done
    complete: bool,
}
```
- See [skydriver/database/schema.py](https://github.com/WIPACrepo/SkyDriver/blob/main/skydriver/database/schema.py)

#### Result
_A dictionary containing the scan result_

Pseudo-code:
```
{
    skyscan_result: dict,  # serialized version of 'skyreader.SkyScanResult'
    is_final: bool,  # is this result the final result?
}
```
- See [skydriver/database/schema.py](https://github.com/WIPACrepo/SkyDriver/blob/main/skydriver/database/schema.py)
- See [skyreader's SkyScanResult](https://github.com/icecube/skyreader/)


&nbsp;
## Examples
### Scanning an Event
See [examples/scan_one.py](https://github.com/WIPACrepo/SkyDriver/blob/main/examples/scan_one.py)
### Monitoring: Progress, Partial Results, and Final Result
Also, see [examples/scan_one.py](https://github.com/WIPACrepo/SkyDriver/blob/main/examples/scan_one.py)
### Scanning Multiple Events
See [examples/scan_many.py](https://github.com/WIPACrepo/SkyDriver/blob/main/examples/scan_many.py)
### Making Plots with a Scan's Result (using the `scan_id`)
See skyreader's [plot_skydriver_scan_result.py](https://github.com/icecube/skyreader/blob/main/examples/plot_skydriver_scan_result.py)
### Creating a `SkyScanResult` Instance from a Scan's Result (using the `scan_id`)
Also, see skyreader's [plot_skydriver_scan_result.py](https://github.com/icecube/skyreader/blob/main/examples/plot_skydriver_scan_result.py)