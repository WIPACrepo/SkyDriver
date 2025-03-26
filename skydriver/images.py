"""Utilities for dealing with docker/cvmfs/singularity images."""

import logging
import re
from pathlib import Path

import aiocache  # type: ignore[import-untyped]
import requests
from async_lru import alru_cache
from dateutil import parser as dateutil_parser
from rest_tools.client import RestClient

from skydriver.config import ENV

LOGGER = logging.getLogger(__name__)


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


async def _match_sha_to_majminpatch(target_sha: str) -> str | None:
    """Finds the image w/ same SHA and has a version tag like '#.#.#'.

    No error handling
    """
    LOGGER.debug(
        f"finding an image that has a version tag like '#.#.#' for sha={target_sha}..."
    )

    rc = RestClient(SKYSCAN_DOCKERHUB_API_URL)

    while True:  # loop for pagination
        LOGGER.info(f"looking at {rc.address}...")
        resp = await rc.request("GET", "")

        # look at each result on this page
        for result in resp["results"]:
            result_sha = result.get("digest", result["images"][0]["digest"])
            # ^^^ some old ones have their 'digest' in their 'images' list entry
            LOGGER.debug(f"an api image: sha={result_sha} ({result})")
            if target_sha != result_sha:
                LOGGER.debug("-> no match, looking at next...")
                continue
            elif VERSION_REGEX_MAJMINPATCH.fullmatch(result["name"]):
                LOGGER.debug("-> success! matches AND has a full version tag")
                return result["name"]  # type: ignore[no-any-return]
            else:
                LOGGER.debug("-> matches, but not a full version tag")

        # what now? get url for the next page
        if not resp["next"]:  # no more -> no match!
            LOGGER.debug(
                f"-> could not find a full version tag matching sha={target_sha}"
            )
            return None
        else:
            rc.address = resp["next"]


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


@aiocache.cached(ttl=5 * 60)  # don't cache too long, tags can be overwritten
async def _try_resolve_to_majminpatch_docker_hub(docker_tag: str) -> str:
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
    info, docker_tag = await get_info_from_docker_hub(docker_tag)
    # check that the image is not too old
    if _parse_image_ts(info) < await min_skymap_scanner_tag_ts():
        raise ValueError(
            f"Image tag is older than the minimum supported tag "
            f"'{ENV.MIN_SKYMAP_SCANNER_TAG}'. Contact admins for more info"
        )

    # make sure tag is fully qualified
    if VERSION_REGEX_MAJMINPATCH.fullmatch(docker_tag):
        return docker_tag  # already full version
    # match sha to vX.Y.Z
    try:
        if majminpatch := await _match_sha_to_majminpatch(info["digest"]):
            return majminpatch
        else:  # no match
            return docker_tag
    except Exception as e:
        LOGGER.exception(e)
        raise ValueError("Error validating image on Docker Hub")


async def get_info_from_docker_hub(docker_tag: str) -> tuple[dict, str]:
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
        rc = RestClient(SKYSCAN_DOCKERHUB_API_URL)
        LOGGER.info(f"looking at {rc.address} for {docker_tag}...")
        resp = await rc.request("GET", docker_tag)
    except requests.exceptions.HTTPError:
        raise _error
    except Exception as e:
        LOGGER.exception(e)
        raise ValueError("Image tag verification failed")

    LOGGER.debug(resp)
    return resp, docker_tag


async def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed.

    NOTE: Assumes tag exists (or will soon) on CVMFS. Condor will back
          off & retry until the image exists
    """
    LOGGER.info(f"checking docker tag: {docker_tag}")
    try:
        out_image = await _try_resolve_to_majminpatch_docker_hub(docker_tag)
        LOGGER.info(f"resolved tag: {docker_tag} -> {out_image}")
        return out_image
    except Exception as e:
        LOGGER.exception(e)
        raise e
