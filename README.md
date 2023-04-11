<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/SkyDriver?include_prereleases)](https://github.com/WIPACrepo/SkyDriver/) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen) 
<!--- End of README Badges (automated) --->
# SkyDriver
A SaaS Solution for Neutrino Event Reconstruction using the Skymap Scanner

## REST API

### `/scans`

#### GET

##### Arguments
###### `run_id` (int)
###### `event_id` (int)
###### `is_real_event` (bool)
###### `include_deleted` (bool): default=`False`

##### Returns
```
{
    "event_id": event_id,
    "scan_ids": scan_ids,  # list of strings
}
```


### `/scan`



### `/scan/manifest/SCAN_ID`



### `/scan/result/SCAN_ID`




