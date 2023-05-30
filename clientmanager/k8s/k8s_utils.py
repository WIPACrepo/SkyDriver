"""Util functions wrapping common htcondor actions."""


# pylint:disable=no-member


from typing import Any

import kubernetes  # type: ignore[import]

from .config import LOGGER

def update_skydriver_k8s(
    skydriver_rc: RestClient,

) -> None:
    """Send SkyDriver updates from the `submit_result`."""
    skydriver_rc.request_seq(
        "PATCH",
        f"/scan/{ENV.SKYSCAN_SKYDRIVER_SCAN_ID}/manifest",
        {
            "k8s_cluster": {
                "collector": collector,
                "schedd": schedd,
                "cluster_id": submit_result_obj.cluster(),
                "jobs": submit_result_obj.num_procs(),
            }
        },
    )