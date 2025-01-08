"""Utilities for interacting with S3 buckets."""

import logging

import boto3

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


def make_object_key(scan_id: str) -> str:
    """Construct the object key from the scan_id (deterministic)."""
    return f"{scan_id}-s3-object"


def generate_s3_get_url(object_key: str) -> str:
    """Generate a pre-signed S3 url for retrieving shared files."""
    s3_client = _get_client()

    # get GET url
    get_url = s3_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": ENV.S3_BUCKET,
            "Key": object_key,
        },
        ExpiresIn=24 * 60 * 60,  # seconds
    )
    return get_url
