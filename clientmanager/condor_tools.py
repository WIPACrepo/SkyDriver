"""Util functions wrapping common htcondor actions."""


# pylint:disable=no-member


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


def get_jobs(njobs: int, clusterid: int) -> list[str]:
    job_ads = job_description.jobs(count=args.jobs, clusterid=submit_result.cluster())
    return list(job_ads)
