"""Utilities for dealing with docker/cvmfs/singularity images."""

import asyncio
import logging
from pathlib import Path

import aiocache  # type: ignore[import-untyped]
from async_lru import alru_cache
from wipac_dev_tools.container_registry_tools import (
    CVMFSRegistryTools,
    DockerHubRegistryTools,
)

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


_IMAGE_NAME = "skymap_scanner"
_SKYSCAN_DOCKER_IMAGE_NAMESPACE = "icecube"


# ---------------------------------------------------------------------------------------
# getters


def get_skyscan_cvmfs_apptainer_image_path(
    tag: str,
    check_exists: bool = False,
) -> Path:
    """Get the apptainer image path on CVMFS for 'tag' (optionally, check if it exists)."""
    cvmfs = CVMFSRegistryTools(ENV.CVMFS_SKYSCAN_SINGULARITY_IMAGES_DIR, _IMAGE_NAME)
    return cvmfs.get_image_path(tag, check_exists)


def get_skyscan_docker_image(tag: str) -> str:
    """Get the docker image + tag for 'tag' (assumes it exists)."""
    return f"{_SKYSCAN_DOCKER_IMAGE_NAMESPACE}/{_IMAGE_NAME}:{tag}"


# ---------------------------------------------------------------------------------------
# utils


@alru_cache  # cache it forever
async def min_skyscan_tag_ts() -> float:
    """Get the timestamp for when the `MIN_SKYMAP_SCANNER_TAG` image was created."""
    info, _ = await get_info_from_docker_hub(ENV.MIN_SKYMAP_SCANNER_TAG)
    return DockerHubRegistryTools.parse_image_ts(info)


@aiocache.cached(ttl=60)  # short ttl to prevent hammering but still up-to-date w/ cvmfs
async def get_info_from_docker_hub(docker_tag: str) -> tuple[dict, str]:
    """Cache docker hub api call."""
    docker_hub = DockerHubRegistryTools(_SKYSCAN_DOCKER_IMAGE_NAMESPACE, _IMAGE_NAME)
    ret = docker_hub.request_info(docker_tag)

    await asyncio.sleep(0)  # let pending async tasks do things after http request
    return ret


@aiocache.cached(ttl=5)  # very short ttl just to stop stampedes
async def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed."""
    LOGGER.info(f"checking docker tag: {docker_tag}")

    # cvmfs is the source of truth
    cvmfs = CVMFSRegistryTools(ENV.CVMFS_SKYSCAN_SINGULARITY_IMAGES_DIR, _IMAGE_NAME)
    docker_tag = cvmfs.resolve_tag(docker_tag)
    get_skyscan_cvmfs_apptainer_image_path(docker_tag, check_exists=True)  # assurance

    # confirm w/ docker hub
    # -> check tag exists
    dh_info, _ = await get_info_from_docker_hub(docker_tag)
    # -> check image is not too old
    if DockerHubRegistryTools.parse_image_ts(dh_info) < await min_skyscan_tag_ts():
        raise ImageTooOldException()

    return docker_tag
