"""Util functions wrapping common htcondor actions."""


# pylint:disable=no-member


import os
from pathlib import Path

import htcondor  # type: ignore[import]

from .config import LOGGER


def condor_token_auth(collector: str, schedd: str) -> None:
    """Write condor token file from `CONDOR_TOKEN` (before any condor calls)"""
    # TODO: implement per-collector/schedd tokens
    if token := os.getenv("CONDOR_TOKEN"):
        condor_tokens_dpath = Path("~/.condor/tokens.d/").expanduser()
        condor_tokens_dpath.mkdir(parents=True, exist_ok=True)
        with open(condor_tokens_dpath / "token1", "w") as f:
            f.write(token)


def get_schedd_obj(collector: str, schedd: str) -> htcondor.Schedd:
    """Get object for talking with HTCondor schedd.

    Examples:
        `collector = "foo-bar.icecube.wisc.edu"`
        `schedd = "baz.icecube.wisc.edu"`
    """
    schedd_ad = htcondor.Collector(collector).locate(  # ~> exception
        htcondor.DaemonTypes.Schedd, schedd
    )
    schedd_obj = htcondor.Schedd(schedd_ad)
    LOGGER.info(f"Connected to Schedd {collector=} {schedd=}")
    return schedd_obj


def get_job_classads(
    submit_obj: htcondor.Submit,
    njobs: int,
    clusterid: int,
) -> list:
    """Get list of (simulated) job ClassAds."""
    job_ads = submit_obj.jobs(count=njobs, clusterid=clusterid)
    return list(job_ads)
