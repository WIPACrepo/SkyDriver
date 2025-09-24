"""Utilities for dealing with docker/cvmfs/singularity images."""

import logging
import re
from pathlib import Path
from typing import Iterable

import aiocache  # type: ignore[import-untyped]
import requests
from async_lru import alru_cache
from dateutil import parser as dateutil_parser
from rest_tools.client import RestClient

from .config import ENV

LOGGER = logging.getLogger(__name__)


class ImageNotFoundException(Exception):
    """Raised when an image (tag) cannot be found."""

    def __init__(self, image: str | Path):
        super().__init__(f"Image '{image}' cannot be found.")


class ImageTooOldException(Exception):
    """Raised when an image (tag) is too old to be used in a scan."""


# ---------------------------------------------------------------------------------------
# constants


_IMAGE = "skymap_scanner"
_SKYSCAN_DOCKER_IMAGE_NO_TAG = f"icecube/{_IMAGE}"

SKYSCAN_DOCKERHUB_API_URL = (
    f"https://hub.docker.com/v2/repositories/{_SKYSCAN_DOCKER_IMAGE_NO_TAG}/tags"
)

# cvmfs singularity
_SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH = Path(
    "/cvmfs/icecube.opensciencegrid.org/containers/realtime/"
)

# NOTE: for security, limit the regex section lengths (with trusted input we'd use + and *)
# https://cwe.mitre.org/data/definitions/1333.html
RE_VERSION_X_Y_Z = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}$")
RE_VERSION_X_Y = re.compile(r"\d{1,3}\.\d{1,3}$")
RE_VERSION_X = re.compile(r"\d{1,3}$")

RE_VERSION_PREFIX_V = re.compile(r"(v|V)\d{1,3}(\.\d{1,3}(\.\d{1,3})?)?$")


# ---------------------------------------------------------------------------------------
# getters


def get_skyscan_cvmfs_singularity_image(tag: str, check_exists: bool = False) -> Path:
    """Get the singularity image path for 'tag' (assumes it exists)."""
    dpath = _SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH / f"{_IMAGE}:{tag}"

    # optional guardrail
    if check_exists and not dpath.exists():
        raise ImageNotFoundException(dpath)

    return dpath


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


def strip_v_prefix(docker_tag: str) -> str:
    """Remove the v-prefix for semver tags."""
    if RE_VERSION_PREFIX_V.fullmatch(docker_tag):
        # v4 -> 4; v5.1 -> 5.1; v3.6.9 -> 3.6.9
        docker_tag = docker_tag.lstrip("v")

    if not docker_tag or not docker_tag.strip():
        raise ImageNotFoundException(docker_tag)

    return docker_tag


@aiocache.cached(ttl=ENV.CACHE_DURATION_DOCKER_HUB)  # fyi: tags can be overwritten
async def get_info_from_docker_hub(docker_tag: str) -> tuple[dict, str]:
    """Get the json dict from GET @ Docker Hub, and the non v-prefixed tag (see below).

    Accepts v-prefixed tags, like 'v2.3.4', 'v4', etc.
    """
    LOGGER.info(f"retrieving tag info on docker hub: {docker_tag}")
    docker_tag = strip_v_prefix(docker_tag)

    try:
        rc = RestClient(SKYSCAN_DOCKERHUB_API_URL)
        LOGGER.debug(f"looking at {rc.address} for {docker_tag}...")
        resp = await rc.request("GET", docker_tag)
    except requests.exceptions.HTTPError as e:
        LOGGER.exception(e)
        raise ImageNotFoundException(docker_tag) from e
    except Exception as e:
        LOGGER.exception(e)
        raise ImageNotFoundException(docker_tag) from ValueError(
            "Image tag verification failed"
        )

    LOGGER.debug(resp)
    return resp, docker_tag


def iter_x_y_z_cvmfs_tags() -> Iterable[str]:
    """Iterate over all 'X.Y.Z' skymap scanner tags on CVMFS, youngest to oldest."""

    cvmfs_tags = sorted(
        _SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH.glob(f"{_IMAGE}:*"),
        key=lambda x: x.stat().st_mtime,  # filesystem modification time
        reverse=True,  # newest -> oldest
    )

    for p in cvmfs_tags:
        try:
            tag = p.name.split(":", maxsplit=1)[1]
        except IndexError:
            continue
        if not RE_VERSION_X_Y_Z.fullmatch(tag):
            continue
        # tag is a full 'X.Y.Z' tag
        yield tag


def resolve_tag_on_cvmfs(docker_tag: str) -> str:
    """Get the 'X.Y.Z' tag on CVMFS corresponding to `docker_tag`.

    Examples:
        3.4.5     ->  3.4.5
        3.1       ->  3.1.5 (forever)
        3         ->  3.3.5 (on 2023/03/08)
        latest    ->  3.4.2 (on 2023/03/15)
        test-foo  ->  test-foo
        typO_tag  ->  `ImageNotFoundException`
    """
    LOGGER.info(f"checking tag exists on cvmfs: {docker_tag}")
    docker_tag = strip_v_prefix(docker_tag)

    # step 1: does the tag simply exist on cvmfs?
    try:
        _path = get_skyscan_cvmfs_singularity_image(docker_tag, check_exists=True)
        LOGGER.debug(f"tag exists on cvmfs: {_path}")
        return docker_tag
    except ImageNotFoundException:
        pass

    # step 2: was the tag a non-specific tag (like 'latest', 'v4.1', 'v4', etc.)
    # -- case 1: user gave 'latest'
    if docker_tag == "latest":
        for tag in iter_x_y_z_cvmfs_tags():
            LOGGER.debug(f"resolved 'latest' to youngest X.Y.Z tag: {tag}")
            return tag
    # -- case 2: user gave an non-specific semver tag (like 'v4.1', 'v4', etc.)
    elif RE_VERSION_X_Y.fullmatch(docker_tag) or RE_VERSION_X.fullmatch(docker_tag):
        for tag in iter_x_y_z_cvmfs_tags():
            if tag.startswith(docker_tag + "."):  # ex: '3.1.4' startswith '3.1.'
                LOGGER.debug(f"resolved '{docker_tag}' to '{tag}'")
                return tag

    # fall-through
    raise ImageNotFoundException(docker_tag)


async def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed."""
    LOGGER.info(f"checking docker tag: {docker_tag}")

    # cvmfs is the source of truth
    docker_tag = resolve_tag_on_cvmfs(docker_tag)
    get_skyscan_cvmfs_singularity_image(docker_tag, check_exists=True)  # just-in-case

    # check that it also exists on docker hub
    dh_info, _ = await get_info_from_docker_hub(docker_tag)

    # check that the image is not too old
    if _parse_image_ts(dh_info) < await min_skymap_scanner_tag_ts():
        raise ImageTooOldException(
            f"Image tag is older than the minimum supported tag "
            f"'{ENV.MIN_SKYMAP_SCANNER_TAG}'. Contact admins for more info."
        )

    return docker_tag
