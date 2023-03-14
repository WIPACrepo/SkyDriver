"""Utilities for dealing with docker/cvmfs/singularity images."""

import re
from pathlib import Path
from typing import Iterator

import requests

_IMAGE = "skymap_scanner"
SKYSCAN_DOCKER_IMAGE_NO_TAG = f"icecube/{_IMAGE}"

DOCKERHUB_API_URL = "https://hub.docker.com/v2/repositories/icecube/skymap_scanner/tags"

# cvmfs singularity
SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH = Path(
    "/cvmfs/icecube.opensciencegrid.org/containers/realtime/"
)
VERSION_REGEX = re.compile(r"\d+\.\d+\.\d+")


def resolve_latest() -> str:
    """Get the most recent version-tag on Docker Hub.

    This is needed because 'latest' doesn't exist in CVMFS.
    """
    # gives 10 most recent tags by default
    images = requests.get(DOCKERHUB_API_URL).json()["results"]

    def latest_sha() -> str:
        for img in images:
            if img["name"] == "latest":
                return img["digest"]  # type: ignore[no-any-return]
        raise RuntimeError("Image tag 'latest' not found on Docker Hub")

    def matching_sha(sha: str) -> Iterator[str]:
        for img in images:
            if img["digest"] == sha:
                yield img["name"]

    for tag in matching_sha(latest_sha()):
        if VERSION_REGEX.fullmatch(tag):
            return tag
    raise RuntimeError("Image tag 'latest' could not resolve to a version")


def get_all_cvmfs_image_tags() -> Iterator[str]:
    """Get all the skymap scanner image tags in CVMFS."""
    for fpath in SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH.iterdir():
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
    raise Exception("Tag not in CVMFS")
