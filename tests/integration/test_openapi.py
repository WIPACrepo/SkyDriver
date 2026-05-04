"""Integration tests for the openapi endpoint and spec."""

import copy
import json
from collections.abc import Callable

import pytest
from rest_tools.client import RestClient
from rest_tools.openapi_tools import request_and_validate
from skydriver.config import OPENAPI_DICT, OPENAPI_PATH, OPENAPI_SPEC


@pytest.mark.order("first")  # check any issues with the spec before other route tests
async def test_00(server: Callable[[], RestClient]) -> None:
    """Test backlog job starting."""
    rc = server()

    # get spec from server endpoint
    spec_from_resp = await rc.request("GET", "/openapi.json")
    spec_from_resp.pop("info")  # exists but server adds to it -- remove to compare

    # get spec from disk
    with open(OPENAPI_PATH, "rb") as f:
        spec_on_disk = json.load(f)
        spec_on_disk.pop("info")  # exists but server adds to it -- remove to compare

    # get spec from (server) in-memory
    spec_server_memory = copy.deepcopy(OPENAPI_DICT)
    spec_server_memory.pop("info")  # exists but server adds to it -- remove to compare

    # assert all the specs are the same
    assert spec_on_disk == spec_from_resp  # disk vs response
    assert spec_on_disk == spec_server_memory  # disk vs (server) in-memory

    # now, for fun, let's validate the getter's response
    await request_and_validate(rc, OPENAPI_SPEC, "GET", "/openapi.json")
