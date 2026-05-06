<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/SkyDriver?include_prereleases)](https://github.com/WIPACrepo/SkyDriver) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/SkyDriver)](https://github.com/WIPACrepo/SkyDriver/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen)
<!--- End of README Badges (automated) --->

# SkyDriver v2

A SaaS Solution for Neutrino Event Reconstruction using the Skymap Scanner
<!--- Top of README Metadata Section (automated) --->

<!--- note: this information is pulled from the pyproject.toml --->

<dl>
    <dt><sub>Authors</sub></dt>
    <dd><sub>WIPAC Developers / <a href='mailto:developers@icecube.wisc.edu'>developers@icecube.wisc.edu</a></sub></dd>
    <dt><sub>Keywords</sub></dt>
    <dd><sub>WIPAC&nbsp;&nbsp;·&nbsp;&nbsp;IceCube&nbsp;&nbsp;·&nbsp;&nbsp;Skymap Scanner&nbsp;&nbsp;·&nbsp;&nbsp;Reconstruction&nbsp;&nbsp;·&nbsp;&nbsp;IceTray&nbsp;&nbsp;·&nbsp;&nbsp;EWMS</sub></dd>
    <dt><sub>URLs</sub></dt>
    <dd><sub><a href='https://github.com/WIPACrepo/SkyDriver'>Homepage</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href='https://github.com/WIPACrepo/SkyDriver/issues'>Tracker</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href='https://github.com/WIPACrepo/SkyDriver'>Source</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href='https://wipacrepo.github.io/SkyDriver'>Documentation</a></sub></dd>
</dl>

<br>
<!--- End of README Metadata Section (automated) --->

## Overview

SkyDriver automates the entire scanning of an event: starting all servers and workers, transferring all needed data, and finally, all tear-down. SkyDriver also includes a database for storing scan requests, progress reports, and results. The computational engine for a scan is the [Skymap Scanner](https://github.com/icecube/skymap_scanner). The main interface is a REST server with several [routes and methods](#rest-api).

One of many workflows may be:

1. Request a scan ([POST @ `/scan`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#post--scan))
1. Monitor the scanning status ([GET @ `/scan/SCAN_ID/status`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#get--scan-scan_id-status))
2. Check for progress updates ([GET @ `/scan/SCAN_ID/manifest`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#get--scan-scan_id-manifest))
3. Check for partial results ([GET @ `/scan/SCAN_ID/result`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#get--scan-scan_id-result))
4. Get a final result ([GET @ `/scan/SCAN_ID/result`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#get--scan-scan_id-result))
5. [Make plots](#making-plots-with-a-scans-result-using-the-scan_id)

Another workflow:

1. Find a scan id for a particular run and event ([POST @ `/scans/find`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#post--scans-find))
2. Get the scan's manifest and result ([GET @ `/scan/SCAN_ID`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#get--scan-scan_id))

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

To request a new scan (see [POST @ `/scan`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#post--scan)):

```python
manifest = rc.request_seq("POST", "/scan", {"docker_tag": ...})
print(json.dumps(manifest))
```

To see your scan's status (see [GET @ `/scan/SCAN_ID/status`](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html#get--scan-scan_id-status)):

```python
status = rc.request_seq("GET", f"/scan/{scan_id}/status")
print(json.dumps(status))
```

&nbsp;

## REST API

See [SkyDriver Docs](https://wipacrepo.github.io/SkyDriver/) for the public-facing routes and methods:

- [API Endpoints](https://wipacrepo.github.io/SkyDriver/apis/skydriver.html)
- [Object Glossary](https://wipacrepo.github.io/SkyDriver/apis/_generated/skydriver-objects.html)

&nbsp;

## Using a Scan Result Outside of SkyDriver

### Making Plots with a Scan's Result (using the `scan_id`)

See skyreader's [plot_skydriver_scan_result.py](https://github.com/icecube/skyreader/blob/main/examples/plot_skydriver_scan_result.py)

### Creating a `SkyScanResult` Instance from a Scan's Result (using the `scan_id`)

Also, see skyreader's [plot_skydriver_scan_result.py](https://github.com/icecube/skyreader/blob/main/examples/plot_skydriver_scan_result.py)
