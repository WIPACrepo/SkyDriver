"""Utilities for interacting with S3 buckets."""

import logging

import boto3  # type: ignore[import-untyped]
import botocore.client

from .config import ENV

LOGGER = logging.getLogger(__name__)


def make_object_key(scan_id: str) -> str:
    """Construct the object key from the scan_id (deterministic)."""
    return f"{scan_id}-s3-object"


def generate_s3_get_url(s3_client: botocore.client.BaseClient, object_key: str) -> str:
    """Generate a pre-signed S3 url for retrieving shared files."""
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
