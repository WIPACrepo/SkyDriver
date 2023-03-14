"""Utilities for dealing with docker/cvmfs/singularity images."""

import re
from pathlib import Path
from typing import Iterator

import requests

from .config import LOGGER

# ---------------------------------------------------------------------------------------
# constants


_IMAGE = "skymap_scanner"
_SKYSCAN_DOCKER_IMAGE_NO_TAG = f"icecube/{_IMAGE}"

DOCKERHUB_API_URL = (
    f"https://hub.docker.com/v2/repositories/{_SKYSCAN_DOCKER_IMAGE_NO_TAG}/tags"
)

# cvmfs singularity
_SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH = Path(
    "/cvmfs/icecube.opensciencegrid.org/containers/realtime/"
)
VERSION_REGEX = re.compile(r"\d+\.\d+\.\d+")


# ---------------------------------------------------------------------------------------
# getters


def get_skyscan_cvmfs_singularity_image(tag: str) -> str:
    """Get the singularity image path for 'tag' (assumes it exists)."""
    return f"{_SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH}{_IMAGE}:{tag}"


def get_skyscan_docker_image(tag: str) -> str:
    """Get the docker image + tag for 'tag' (assumes it exists)."""
    return f"{_SKYSCAN_DOCKER_IMAGE_NO_TAG}:{tag}"


# ---------------------------------------------------------------------------------------
# utils
def resolve_latest() -> str:
    """Get the most recent version-tag on Docker Hub.

    This is needed because 'latest' doesn't exist in CVMFS.
    """
    # gives 10 most recent tags by default
    try:
        images = requests.get(DOCKERHUB_API_URL).json()["results"]
    except Exception as e:
        LOGGER.error(e)
        ValueError("Image tag 'latest' failed to resolve to a version")

    def latest_sha() -> str:
        for img in images:
            if img["name"] == "latest":
                return img["digest"]  # type: ignore[no-any-return]
        raise ValueError("Image tag 'latest' not found on Docker Hub")

    def matching_sha(sha: str) -> Iterator[str]:
        for img in images:
            if img["digest"] == sha:
                yield img["name"]

    for tag in matching_sha(latest_sha()):
        if VERSION_REGEX.fullmatch(tag):
            return tag
    raise ValueError("Image tag 'latest' could not resolve to a version")


def get_all_cvmfs_image_tags() -> Iterator[str]:
    """Get all the skymap scanner image tags in CVMFS."""
    for fpath in _SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH.iterdir():
        image, tag = fpath.name.split(":", maxsplit=1)  # ex: skymap_scannner:3.6.9
        if image == _IMAGE:
            yield tag


def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed."""
    if docker_tag == "latest":
        docker_tag = resolve_latest()
    elif docker_tag.startswith("v"):
        # v3.6.9 -> 3.6.9 (if needed)
        if VERSION_REGEX.fullmatch(without_v := docker_tag.lstrip("v")):
            docker_tag = without_v

    # in CVMFS?
    if docker_tag in get_all_cvmfs_image_tags():
        return docker_tag
    raise ValueError("Tag not in CVMFS")
