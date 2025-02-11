"""Utilities for dealing with docker/cvmfs/singularity images."""

import json
import logging
import re
from pathlib import Path

import cachetools.func
import requests
from dateutil import parser as dateutil_parser

from skydriver.config import ENV

LOGGER = logging.getLogger(__name__)


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

# NOTE: for security, limit the regex section lengths (with trusted input we'd use + and *)
# https://cwe.mitre.org/data/definitions/1333.html
VERSION_REGEX_MAJMINPATCH = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}$")
VERSION_REGEX_PREFIX_V = re.compile(r"(v|V)\d{1,3}(\.\d{1,3}(\.\d{1,3})?)?$")


# ---------------------------------------------------------------------------------------
# getters


def get_skyscan_cvmfs_singularity_image(tag: str) -> str:
    """Get the singularity image path for 'tag' (assumes it exists)."""
    return str(_SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH / f"{_IMAGE}:{tag}")


def get_skyscan_docker_image(tag: str) -> str:
    """Get the docker image + tag for 'tag' (assumes it exists)."""
    return f"{_SKYSCAN_DOCKER_IMAGE_NO_TAG}:{tag}"


# ---------------------------------------------------------------------------------------
# utils


def _match_sha_to_majminpatch(sha: str) -> str | None:
    """Finds the image w/ same SHA and has a version tag like '#.#.#'.

    No error handling
    """
    url = DOCKERHUB_API_URL
    while True:
        resp = requests.get(url).json()
        for result in resp["results"]:
            if sha != result.get("digest", result["images"][0]["digest"]):
                # some old ones have their 'digest' in their 'images' list entry
                continue
            if VERSION_REGEX_MAJMINPATCH.fullmatch(result["name"]):
                return result["name"]  # type: ignore[no-any-return]
        if not resp["next"]:
            break
        url = resp["next"]
    return None


def _parse_image_ts(info: dict) -> float:
    """Get the timestamp for when the image was created."""
    try:
        return dateutil_parser.parse(info["last_updated"]).timestamp()
    except Exception as e:
        LOGGER.exception(e)
        raise e


@cachetools.func.lru_cache()  # cache it forever
def min_skymap_scanner_tag_ts() -> float:
    """Get the timestamp for when the `MIN_SKYMAP_SCANNER_TAG` image was created."""
    info, _ = get_info_from_docker_hub(ENV.MIN_SKYMAP_SCANNER_TAG)
    return _parse_image_ts(info)


@cachetools.func.ttl_cache(ttl=5 * 60)  # don't cache too long, tags can be overwritten
def _try_resolve_to_majminpatch_docker_hub(docker_tag: str) -> str:
    """Get the '#.#.#' tag on Docker Hub w/ `docker_tag`'s SHA if possible.

    Return `docker_tag` if already a '#.#.#', or if there's no match.

    Examples:
        3.4.5     ->  3.4.5
        3.1       ->  3.1.5 (forever)
        3         ->  3.3.5 (on 2023/03/08)
        latest    ->  3.4.2 (on 2023/03/15)
        test-foo  ->  test-foo
        typo_tag  ->  `ValueError`

    Raises:
        ValueError -- if `docker_tag` doesn't exist on Docker Hub
        ValueError -- if there's an issue communicating w/ Docker Hub API
    """
    info, docker_tag = get_info_from_docker_hub(docker_tag)
    # check that the image is not too old
    if _parse_image_ts(info) < min_skymap_scanner_tag_ts():
        raise ValueError(
            f"Image tag is older than the minimum supported tag "
            f"'{ENV.MIN_SKYMAP_SCANNER_TAG}'. Contact admins for more info"
        )

    # make sure tag is fully qualified
    if VERSION_REGEX_MAJMINPATCH.fullmatch(docker_tag):
        return docker_tag  # already full version
    # match sha to vX.Y.Z
    try:
        if majminpatch := _match_sha_to_majminpatch(info["digest"]):
            return majminpatch
        else:  # no match
            return docker_tag
    except Exception as e:
        LOGGER.exception(e)
        raise ValueError("Image tag could not resolve to a full version")


def get_info_from_docker_hub(docker_tag: str) -> tuple[dict, str]:
    """Get the json dict from GET @ Docker Hub, and the non v-prefixed tag (see below).

    Accepts v-prefixed tags, like 'v2.3.4', 'v4', etc.
    """
    if VERSION_REGEX_PREFIX_V.fullmatch(docker_tag):
        # v4 -> 4; v5.1 -> 5.1; v3.6.9 -> 3.6.9
        docker_tag = docker_tag.lstrip("v")

    _error = ValueError(f"Image tag not on Docker Hub: {docker_tag}")

    if not docker_tag or not docker_tag.strip():
        raise _error

    try:
        url = f"{DOCKERHUB_API_URL}/{docker_tag}"
        LOGGER.info(f"looking at {url}...")
        resp = requests.get(url, timeout=10)
    except Exception as e:
        LOGGER.exception(e)
        raise ValueError("Image tag verification failed")

    if not resp.ok:
        raise _error

    LOGGER.debug(json.dumps(resp.json(), indent=0))
    return resp.json(), docker_tag


def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed.

    NOTE: Assumes tag exists (or will soon) on CVMFS. Condor will back
          off & retry until the image exists
    """
    LOGGER.info(f"checking docker tag: {docker_tag}")
    try:
        return _try_resolve_to_majminpatch_docker_hub(docker_tag)
    except Exception as e:
        LOGGER.exception(e)
        raise e
