"""A script to randomly query SkyDriver.

This will makes sure everything that should be accessible is accessible.
"""

import argparse
import asyncio
import pprint
import random

import test_runner


# Function to split list into chunks
def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def main():
    parser = argparse.ArgumentParser(
        description="Launch and monitor a scan for an event",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skydriver-url",
        required=True,
        help="the url to connect to a SkyDriver server",
    )
    args = parser.parse_args()

    rc = test_runner.get_rest_client(args.skydriver_url)

    # 1: get all the scan_ids (not too large)
    print("POST @ /scans/find ...")
    resp = await rc.request(
        "POST",
        "/scans/find",
        {
            "filter": {},
            "include_deleted": True,
            "manifest_projection": ["scan_id"],
        },
    )
    scan_ids = [m["scan_id"] for m in resp["manifests"]]
    print(f"{len(scan_ids)} scans")
    scan_ids = random.sample(scan_ids, max(1, len(scan_ids) // 20))  # 5% sample
    print(f"will run queries for {len(scan_ids)} scans (5% sample)")

    print("\n---\n")

    # 2: re-find
    total = 0
    versions = {"v1.0": [], "v1.1": []}
    for chunk_scan_ids in chunk_list(scan_ids, 10):
        print("POST @ /scans/find ...")
        resp = await rc.request(
            "POST",
            "/scans/find",
            {
                "filter": {"scan_id": {"$in": chunk_scan_ids}},
                "include_deleted": True,
            },
        )
        pprint.pprint(resp)
        print(f"found {len(resp['manifests'])}/{len(chunk_scan_ids)} scans (subset)")
        total += len(resp["manifests"])
        for m in resp["manifests"]:
            if m["i3_event_id"]:
                versions["v1.2"].append(m["scan_id"])
            elif isinstance(m["event_i3live_json_dict"], dict):
                versions["<=v1.1"].append(m["scan_id"])
            else:
                versions["other"].append(m["scan_id"])
    pprint.pprint(versions)
    print(f"confirmed {total} scans")
    assert total == len(scan_ids)
    assert all(v for v in versions.values())  # check that all versions are represented

    print("\n---\n")

    # 3. quickly query the backlog
    print("GET @ /scans/backlog ...")
    resp = await rc.request("GET", "/scans/backlog")
    pprint.pprint(resp)

    print("\n---\n")

    # 4. query each scan
    for i, scan_id in enumerate(scan_ids):
        print(f"various queries for {scan_id} ({i+1}/{len(scan_ids)}) ...")
        #
        print(f"GET @ /scan/{scan_id} ...")
        resp = await rc.request("GET", f"/scan/{scan_id}", {"include_deleted": True})
        pprint.pprint(resp)
        #
        print(f"GET @ /scan/{scan_id}/manifest ...")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/manifest", {"include_deleted": True}
        )
        pprint.pprint(resp)
        #
        print(f"GET @ /scan/{scan_id}/i3-event ...")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/i3-event", {"include_deleted": True}
        )
        pprint.pprint(resp)
        #
        print(f"GET @ /scan/{scan_id}/result ...")
        resp = await rc.request(
            "GET", f"/scan/{scan_id}/result", {"include_deleted": True}
        )
        pprint.pprint(resp)
        #
        print("\n---\n")

    print("\n---\n")
    pprint.pprint(versions)


# Run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main())
    print("Done.")
