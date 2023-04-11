<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/SkyDriver?include_prereleases)](https://github.com/WIPACrepo/SkyDriver/) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen) 
<!--- End of README Badges (automated) --->
# SkyDriver
A SaaS Solution for Neutrino Event Reconstruction using the Skymap Scanner

## REST API
This is documentation for the public-facing routes/methods


<br/>
-------------------------------------------------------------------------------
<br/>


### `/scans`: GET
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


<br/>
-------------------------------------------------------------------------------
<br/>


### `/scan`: POST
_Launch a new scan of an event_

#### Arguments
| Argument                          | Type         | Required/Default | Description          |
| --------------------------------- | ------------ | ---------------- | -------------------- |
| `"docker_tag"`                    | str          | *[REQUIRED]*     | the docker tag of the Skymap Scanner image (must be in CVMFS). Ex: `v3.1.4`, `v3.5`, `v3`, `latest`
| `"memory"`                        | str          | default: `8GB`   | how much memory per condor worker to request
| `"cluster"`                       | dict or list | *[REQUIRED]*     | the HTCondor cluster(s) to use: list of `{collector: foo, schedd: baz, njobs: 1234}`
| `"reco_algo"`                     | bool         | *[REQUIRED]*     | which reco algorithm to use (see [Skymap Scanner](https://github.com/icecube/skymap_scanner/tree/main/skymap_scanner/recos))
| `"event_i3live_json"`             | dict or str  | *[REQUIRED]*     | Realtime's JSON event format
| `"nsides"`                        | dict         | *[REQUIRED]*     | the nside progression to use (see [Skymap Scanner](https://github.com/icecube/skymap_scanner))
| `"real_or_simulated_event"`       | str          | *[REQUIRED]*     | whether this event is real or simulated. Ex: `real`, `simulated`
| `"predictive_scanning_threshold"` | float        | default: `1.0`   | the predictive scanning threshold [0.1, 1.0] (see [Skymap Scanner](https://github.com/icecube/skymap_scanner))
| `"max_reco_time"`                 | int          | default: `None`  | the max amount of time each reco should take

#### SkyDriver Effects
- Creates and starts a new Skymap Scanner instance spread across many HTCondor workers
- The new scanner will send updates routinely and when the scan completes (see [GET (manifest)](#scanmanifestscan_id-get) and [GET (result)](#scanresultscan_id-get))

#### Returns
`skydriver.database.schema.Manifest` as a dict


<br/>
-------------------------------------------------------------------------------
<br/>


### `/scan/manifest/SCAN_ID`: GET
_Retrieve the manifest of a scan_

#### Arguments
| Argument            | Type        | Required/Default | Description          |
| ------------------- | ----------- | ---------------- | -------------------- |
| `"include_deleted"` | bool        | default: `False` | whether to include deleted scans

#### SkyDriver Effects
None

#### Returns
`skydriver.database.schema.Manifest` as a dict


<br/>
-------------------------------------------------------------------------------
<br/>


### `/scan/manifest/SCAN_ID`: DELETE
_Abort ongoing scan_

#### Arguments
None

#### SkyDriver Effects
- The Skymap Scanner instance is stopped and removed
- The scan's manifest is marked as "deleted" in the database

#### Returns
`skydriver.database.schema.Manifest` as a dict


<br/>
-------------------------------------------------------------------------------
<br/>


### `/scan/result/SCAN_ID`: GET
_Retrieve the result of a scan_

#### Arguments
| Argument            | Type        | Required/Default | Description          |
| ------------------- | ----------- | ---------------- | -------------------- |
| `"include_deleted"` | bool        | default: `False` | whether to include deleted scans

#### SkyDriver Effects
Stops the Skymap Scanner instance if it is still running

#### Returns
`skydriver.database.schema.Result` as a dict


<br/>
-------------------------------------------------------------------------------
<br/>


### `/scan/result/SCAN_ID`: DELETE
_Delete the result of a scan_

#### Arguments
None

#### SkyDriver Effects
The result is marked as "deleted" in the database

#### Returns
`skydriver.database.schema.Result` as a dict


<br/>
-------------------------------------------------------------------------------
<br/>






