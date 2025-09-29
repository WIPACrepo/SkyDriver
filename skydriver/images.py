"""Utilities for dealing with docker/cvmfs/singularity images."""

import logging
from pathlib import Path

import aiocache  # type: ignore[import-untyped]
import requests
from async_lru import alru_cache
from dateutil import parser as dateutil_parser
from rest_tools.client import RestClient
from wipac_dev_tools import container_registry_tools, semver_parser_tools

from .config import ENV

LOGGER = logging.getLogger(__name__)


class ImageTooOldException(Exception):
    """Raised when an image (tag) is too old to be used in a scan."""

    def __init__(self):
        # NOTE - this message is sent to user, so don't supply tag name (security)
        super().__init__(
            f"Image tag is older than the minimum supported tag "
            f"'{ENV.MIN_SKYMAP_SCANNER_TAG}'. Contact admins for more info."
        )


# ---------------------------------------------------------------------------------------
# constants


_IMAGE = "skymap_scanner"
_SKYSCAN_DOCKER_IMAGE_NO_TAG = f"icecube/{_IMAGE}"

SKYSCAN_DOCKERHUB_API_URL = (
    f"https://hub.docker.com/v2/repositories/{_SKYSCAN_DOCKER_IMAGE_NO_TAG}/tags"
)


# ---------------------------------------------------------------------------------------
# getters


def get_skyscan_cvmfs_apptainer_image_path(
    tag: str, check_exists: bool = False
) -> Path:
    """Get the apptainer image path on CVMFS for 'tag' (optionally, check if it exists)."""
    return container_registry_tools.get_cvmfs_image_path(
        ENV.CVMFS_SKYSCAN_SINGULARITY_IMAGES_DIR,
        _IMAGE,
        tag,
        check_exists,
    )


def get_skyscan_docker_image(tag: str) -> str:
    """Get the docker image + tag for 'tag' (assumes it exists)."""
    return f"{_SKYSCAN_DOCKER_IMAGE_NO_TAG}:{tag}"


# ---------------------------------------------------------------------------------------
# utils


def _parse_image_ts(info: dict) -> float:
    """Get the timestamp for when the image was created."""
    try:
        return dateutil_parser.parse(info["last_updated"]).timestamp()
    except Exception as e:
        LOGGER.exception(e)
        raise e


@alru_cache  # cache it forever
async def min_skymap_scanner_tag_ts() -> float:
    """Get the timestamp for when the `MIN_SKYMAP_SCANNER_TAG` image was created."""
    info, _ = await get_info_from_docker_hub(ENV.MIN_SKYMAP_SCANNER_TAG)
    return _parse_image_ts(info)


@aiocache.cached(ttl=ENV.CACHE_DURATION_DOCKER_HUB)  # fyi: tags can be overwritten
async def get_info_from_docker_hub(docker_tag: str) -> tuple[dict, str]:
    """Get the json dict from GET @ Docker Hub, and the non v-prefixed tag (see below).

    Accepts v-prefixed tags, like 'v2.3.4', 'v4', etc. -- and non-v-prefixed tags.
    """
    LOGGER.info(f"retrieving tag info on docker hub: {docker_tag}")

    # prep tag
    try:
        docker_tag = semver_parser_tools.strip_v_prefix(docker_tag)
    except ValueError as e:
        raise container_registry_tools.ImageNotFoundException(docker_tag) from e

    # look for tag on docker hub
    try:
        rc = RestClient(SKYSCAN_DOCKERHUB_API_URL)
        LOGGER.debug(f"looking at {rc.address} for {docker_tag}...")
        resp = await rc.request("GET", docker_tag)
    # -> http issue
    except requests.exceptions.HTTPError as e:
        LOGGER.exception(e)
        raise container_registry_tools.ImageNotFoundException(docker_tag) from e
    # -> tag issue
    except Exception as e:
        LOGGER.exception(e)
        raise container_registry_tools.ImageNotFoundException(
            docker_tag
        ) from ValueError("Image tag verification failed")

    LOGGER.debug(resp)
    return resp, docker_tag


async def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed."""
    LOGGER.info(f"checking docker tag: {docker_tag}")

    # cvmfs is the source of truth
    docker_tag = container_registry_tools.resolve_tag_on_cvmfs(
        ENV.CVMFS_SKYSCAN_SINGULARITY_IMAGES_DIR,
        _IMAGE,
        docker_tag,
    )
    get_skyscan_cvmfs_apptainer_image_path(docker_tag, check_exists=True)  # assurance

    # check that it also exists on docker hub
    dh_info, _ = await get_info_from_docker_hub(docker_tag)

    # check that the image is not too old
    if _parse_image_ts(dh_info) < await min_skymap_scanner_tag_ts():
        raise ImageTooOldException()

    return docker_tag
