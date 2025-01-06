"""Utilities for interacting with S3 buckets."""

import logging
import pathlib

import boto3
import requests

from .config import ENV

LOGGER = logging.getLogger(__name__)


def _get_client():
    LOGGER.info("Connecting to S3...")
    return boto3.client(
        "s3",
        "us-east-1",
        endpoint_url=ENV.S3_URL,
        aws_access_key_id=ENV.S3_ACCESS_KEY_ID,
        aws_secret_access_key=ENV.S3_SECRET_KEY,
    )


def generate_s3_url(scan_id: str) -> str:
    """Generate a pre-signed S3 url for putting shared files."""
    s3_client = _get_client()

    # get GET url
    get_url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": ENV.S3_BUCKET,
            "Key": f"{scan_id}-s3-object",
        },
        ExpiresIn=24 * 60 * 60,  # seconds
    )
    return get_url


def upload_to_s3(fpath: pathlib.Path) -> str:
    """Upload a file to S3."""
    s3_client = _get_client()

    # POST
    upload_details = s3_client.generate_presigned_post(
        ENV.S3_BUCKET, ENV.S3_OBJECT_DEST_FILE
    )

    LOGGER.info("uploading S3...")
    with open(fpath, "rb") as f:
        response = requests.post(
            upload_details["url"],
            data=upload_details["fields"],
            files={"file": (fpath.name, f)},  # maps filename to obj
        )

    print(f"Upload response: {response.status_code}")
    print(str(response.content))
