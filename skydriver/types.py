"""Miscellaneous types."""

import dataclasses as dc

from typeguard import typechecked


@typechecked
@dc.dataclass
class RequestorInputCluster:
    """A set of details for a user-requested Condor cluster."""

    collector: str
    schedd: str
    n_workers: int  # = 0  # TODO: make optional when using "smart starter"
