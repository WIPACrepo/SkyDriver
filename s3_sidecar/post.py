"""Utilities for posting to an S3 bucket."""

import argparse
import dataclasses as dc
import logging
import time
from pathlib import Path

import boto3  # type: ignore[import-untyped]
import requests
from wipac_dev_tools import from_environment_as_dataclass, logging_tools
from wipac_dev_tools.timing_tools import IntervalTimer

LOGGER = logging.getLogger(__package__)


@dc.dataclass(frozen=True)
class EnvConfig:
    """Environment variables."""

    S3_URL: str
    S3_ACCESS_KEY_ID: str
    S3_SECRET_KEY: str
    S3_BUCKET: str
    S3_OBJECT_KEY: str


ENV = from_environment_as_dataclass(EnvConfig)


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
        endpoint_url=ENV.S3_URL,
        aws_access_key_id=ENV.S3_ACCESS_KEY_ID,
        aws_secret_access_key=ENV.S3_SECRET_KEY,
    )

    # POST
    LOGGER.info("generating presigned post-url...")
    upload_details = s3_client.generate_presigned_post(ENV.S3_BUCKET, ENV.S3_OBJECT_KEY)
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
    logging_tools.log_argparse_args(args)

    logger_timer = IntervalTimer(5, LOGGER)

    if args.wait_indefinitely:
        LOGGER.info("Waiting for file to exist...")
        while not args.fpath.exists():
            if logger_timer.has_interval_elapsed():
                LOGGER.info("still waiting...")
            time.sleep(1)

    post(args.fpath)


if __name__ == "__main__":
    logging_tools.set_level(
        "INFO",
        first_party_loggers=LOGGER,
        third_party_level="INFO",
        future_third_parties=[],
        specialty_loggers={"rest_tools": "INFO"},
    )
    main()
    LOGGER.info("Done.")
