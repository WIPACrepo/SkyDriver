"""Util functions wrapping common htcondor actions."""





from typing import Any

import htcondor  # type: ignore[import]

from .config import LOGGER


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
) -> list[Any]:
    """Get list of (simulated) job ClassAds."""
    job_ads = submit_obj.jobs(count=njobs, clusterid=clusterid)
    return list(job_ads)
