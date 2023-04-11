<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/SkyDriver?include_prereleases)](https://github.com/WIPACrepo/SkyDriver/) [![Lines of code](https://img.shields.io/tokei/lines/github/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen) 
<!--- End of README Badges (automated) --->
# SkyDriver
A SaaS Solution for Neutrino Event Reconstruction using the Skymap Scanner

## REST API

### `/scans`

#### GET
_Retrieve scan ids corresponding to a specific run & event_

##### Arguments
| Arg             | Type        | Required/Default |
| --------------- | ----------- | ---------------- |
| run_id          | int         | **[REQUIRED]**   |
| event_id        | int         | **[REQUIRED]**   |
| is_real_event   | bool        | **[REQUIRED]**   |
| include_deleted | bool        | default: `False` |

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




