"""Helper script to lookup all associated EWMS ids for a scan id."""

import argparse
import asyncio
import json
import logging

logging.getLogger().setLevel(logging.INFO)


async def main():
    parser = argparse.ArgumentParser(
        description="lookup all associated ids for a scan id",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--skydriver",
        dest="skydriver_type",
        required=True,
        choices=["dev", "prod"],
        help=(
            "the type of the SkyDriver instance for REST API URL "
            "(ex: prod -> https://skydriver.icecube.aq; dev -> https://skydriver-dev.icecube.aq)"
        ),
    )
    args = parser.parse_args()
    if not (query := {k: v for k, v in vars(args).items() if k in IDS and v}):
        raise ValueError("cannot find objects without at least one CL arg id")

    rc = get_rest_client(args.ewms_suffix)

    ####

    # taskforce has the most ids in it (most specific object)
    logging.info(f"{query=}")
    all_ids_on_tf = []
    after = None
    while True:
        resp = await rc.request(
            "POST",
            "/v1/query/taskforces",
            {
                "query": query,
                "projection": IDS,
                **({"after": after} if after else {}),  # only add if truthy
            },
        )
        all_ids_on_tf.extend(resp["taskforces"])
        if not (after := resp["next_after"]):
            break

    print(json.dumps(all_ids_on_tf, indent=4), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
    logging.info("Done.")
