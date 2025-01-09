"""Utilities for posting to an S3 bucket."""

import argparse
import logging
import os
import time
from pathlib import Path

import boto3
import requests

LOGGER = logging.getLogger(__package__)


def post(fpath: Path) -> None:
    """Post the file to the S3 bucket."""
    if not fpath.exists():
        raise FileNotFoundError(str(fpath))

    LOGGER.info("file exists, waiting a bit longer just in case")
    time.sleep(5)  # in case the file is currently being written (good enough logic?)

    LOGGER.info("connecting to s3...")
    s3_client = boto3.client(
        "s3",
        "us-east-1",
        endpoint_url=os.environ["S3_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    )

    # POST
    LOGGER.info("generating presigned post-url...")
    upload_details = s3_client.generate_presigned_post(
        os.environ["S3_BUCKET"],
        os.environ["S3_OBJECT_KEY"],
    )
    LOGGER.info("posting file to s3...")
    with open(fpath, "rb") as f:
        response = requests.post(
            upload_details["url"],
            data=upload_details["fields"],
            files={"file": (fpath.name, f)},  # maps filename to obj
        )

    LOGGER.info(f"Upload response: {response.status_code}")
    LOGGER.info(str(response.content))


def main() -> None:
    """Main."""
    parser = argparse.ArgumentParser(
        description="Post the file to the S3 bucket.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "fpath",
        type=Path,
        help="the file to post",
    )
    parser.add_argument(
        "--wait-indefinitely",
        action="store_true",
        default=False,
        help="whether to wait indefinitely for the file to exist",
    )

    args = parser.parse_args()

    if args.wait_indefinitely:
        LOGGER.info("Waiting for file to exist...")
        while not args.fpath.exists():
            time.sleep(1)

    post(args.fpath)


if __name__ == "__main__":
    main()
    LOGGER.info("Done.")
