"""Utilities for dealing with docker/cvmfs/singularity images."""

import logging
import re
from pathlib import Path

import aiocache  # type: ignore[import-untyped]
import requests
from async_lru import alru_cache
from dateutil import parser as dateutil_parser
from rest_tools.client import RestClient

from .config import ENV

LOGGER = logging.getLogger(__name__)


class ImageNotFound(Exception):
    """Raised when an image (tag) cannot be found."""


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


def get_skyscan_cvmfs_singularity_image(tag: str, check_exists: bool = False) -> Path:
    """Get the singularity image path for 'tag' (assumes it exists)."""
    dpath = _SKYSCAN_CVMFS_SINGULARITY_IMAGES_DPATH / f"{_IMAGE}:{tag}"

    # optional guardrail
    if check_exists and not dpath.exists():
        raise ImageNotFound(dpath)

    return dpath


def get_skyscan_docker_image(tag: str) -> str:
    """Get the docker image + tag for 'tag' (assumes it exists)."""
    return f"{_SKYSCAN_DOCKER_IMAGE_NO_TAG}:{tag}"


# ---------------------------------------------------------------------------------------
# utils


def _extract_digest(result: dict) -> str | None:
    """Return the digest for a Docker Hub tag result.

    Prefers the top-level 'digest'. If missing, falls back to the first image
    digest in 'images' (if any). Returns None if no digest is found.
    """

    # Docker Hub tag results may provide 'digest' at the top level
    # (common in current API responses) or inside the 'images' list
    # (older API responses). Some tags may have an empty 'images' list.
    # This function handles all cases gracefully.

    if digest := result.get("digest"):
        return digest

    if not (imgs := result.get("images")):
        return None

    for img in imgs:
        if d := img.get("digest"):
            return d

    return None


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
        for result in resp.get("results") or []:

            # does the image digest (sha) match?
            result_sha = _extract_digest(result)
            if not result_sha:
                LOGGER.debug(f"-> skipping tag={result.get('name')} (no digest found)")
                continue
            if target_sha != result_sha:
                continue

            # is this a full version ('#.#.#') tag?
            name = result.get("name", "")
            if VERSION_REGEX_MAJMINPATCH.fullmatch(name):
                LOGGER.debug("-> success! matches AND has a full version tag")
                return name  # type: ignore[no-any-return]
            else:
                LOGGER.debug("-> matches, but not a full version tag")

        # looping logic
        if not (next_url := resp.get("next")):  # no more -> no match!
            LOGGER.debug(
                f"-> could not find a full version tag matching sha={target_sha}"
            )
            return None
        else:
            rc.address = next_url


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

    if not docker_tag or not docker_tag.strip():
        raise ImageNotFound(docker_tag)

    try:
        rc = RestClient(SKYSCAN_DOCKERHUB_API_URL)
        LOGGER.info(f"looking at {rc.address} for {docker_tag}...")
        resp = await rc.request("GET", docker_tag)
    except requests.exceptions.HTTPError as e:
        LOGGER.exception(e)
        raise ImageNotFound(docker_tag) from e
    except Exception as e:
        LOGGER.exception(e)
        raise ImageNotFound(docker_tag) from ValueError("Image tag verification failed")

    LOGGER.debug(resp)
    return resp, docker_tag


async def resolve_docker_tag(docker_tag: str) -> str:
    """Check if the docker tag exists, then resolve 'latest' if needed."""
    LOGGER.info(f"checking docker tag: {docker_tag}")

    # resolve tag on docker hub
    try:
        resolved_image = await _try_resolve_to_majminpatch_docker_hub(docker_tag)
        LOGGER.info(f"resolved tag: {docker_tag} -> {resolved_image}")
    except Exception as e:
        LOGGER.exception(e)
        raise ImageNotFound(docker_tag) from e

    # now, check that the image exists on CVMFS
    # -- case 1: user gave a resolved tag (aka a very specific tag)
    if resolved_image == docker_tag:
        get_skyscan_cvmfs_singularity_image(resolved_image, check_exists=True)
    # -- case 2: user gave a non-specific tag (like latest, v4, etc.)
    else:
        try:
            get_skyscan_cvmfs_singularity_image(resolved_image, check_exists=True)
        except ImageNotFound as e:
            LOGGER.warning(f"{repr(e)} -- will now attempt to downgrade tag...")
            pass

    return resolved_image
